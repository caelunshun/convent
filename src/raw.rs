//! Raw implementation of the event buffer. The high-level `Writer`, `Reader`, and `EventBuffer`
//! types are wrappers around these types.
//!
//! TODO: evaluate memory orderings

use crossbeam_utils::Backoff;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::{iter, ptr};

/// As an optimization, we store values as either a single value
/// or a `Vec` of multiple values to allow for efficient bulk writing.
pub enum Value<T> {
    Single(T),
    Multiple(Vec<T>),
}

impl<T> Value<T> {
    pub fn iter<'a>(&'a self) -> ValueIter<'a, T> {
        ValueIter {
            val: self,
            index: 0,
        }
    }

    pub fn single(&self) -> &T {
        match self {
            Value::Single(val) => val,
            Value::Multiple(_) => panic!(),
        }
    }
}

impl<'a, T> IntoIterator for &'a Value<T> {
    type Item = &'a T;
    type IntoIter = ValueIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct ValueIter<'a, T> {
    val: &'a Value<T>,
    index: usize,
}

impl<'a, T> Iterator for ValueIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let res = match self.val {
            Value::Single(val) => {
                if self.index > 0 {
                    None
                } else {
                    Some(val)
                }
            }
            Value::Multiple(vec) => vec.get(self.index),
        };

        self.index += 1;

        res
    }
}

struct Slot<T> {
    occupied: AtomicBool,
    value: MaybeUninit<Value<T>>,
}

/// A segment in the event buffer.
struct Segment<T> {
    /// Number of values which must still be read.
    /// Starts at `num_readers * segment_size`; each
    /// reader decrements the value each time it reads
    /// a value. When this reaches 0, the segment is freed,
    /// since all readers have read all values in this segment.
    reads_remaining: AtomicUsize,
    slots: Vec<Slot<T>>,
    next: AtomicPtr<Segment<T>>,
}

impl<T> Drop for Segment<T> {
    fn drop(&mut self) {
        for slot in self.slots.iter_mut() {
            if slot.occupied.load(Ordering::Acquire) {
                // Drop value
                unsafe {
                    ptr::drop_in_place(slot.value.as_mut_ptr());
                }
            }
        }
    }
}

unsafe impl<T: Send> Send for Segment<T> {}
unsafe impl<T: Sync> Sync for Segment<T> {}

/// An event buffer.
///
/// # Properties
/// * `head` will never be null; there will always be at least one segment
/// in the buffer.
pub struct RawEventBuffer<T> {
    head: AtomicPtr<Segment<T>>,
    tail: AtomicPtr<Segment<T>>,
    num_readers: AtomicUsize,
    segment_size: usize,
    /// The index of the next value to write into `head`.`
    /// If this value is found to equal `segment_size`, then a
    /// writer should push the last value into `head` and then
    /// allocate a new segment. If this value is found
    /// to exceed `segment_size`, then a writer should wait for a new
    /// segment to be allocated. (TODO: make this wait-free)
    write_index: AtomicUsize,
}

impl<T> RawEventBuffer<T> {
    /// Creates a new `RawEventBuffer` with the given segment size.
    pub fn with_segment_size(segment_size: usize) -> Self {
        assert!(segment_size > 1);
        let segment = Box::into_raw(alloc_segment(segment_size, 0));
        Self {
            head: AtomicPtr::new(segment),
            tail: AtomicPtr::new(segment),
            num_readers: AtomicUsize::new(0),
            segment_size,
            write_index: AtomicUsize::new(0),
        }
    }

    /// Atomically writes one or more events into this buffer.
    pub fn write(&self, value: Value<T>) {
        // Find an index into the segment to write to.

        // We ensure that no use after free occurs by not reading from `self.head`
        // unless `write_index` points to a valid index into that segment. Since
        // a segment is not deallocated by readers until all values have been read from
        // it (requiring it to be full), a valid `write_index` ensures that the segment
        // is not full. Thus any reads from it by this function will be valid until
        // we set `occupied` to true for the final slot in the segment, which allows
        // readers to deallocate the segment.

        // If `write_index` is invalid, we will wait for the thread which wrote the last
        // value to the segment to allocate a new one. TODO: make this wait-free
        let backoff = Backoff::new();
        let (index, segment): (usize, *mut Segment<T>) = loop {
            let index = self.write_index.fetch_add(1, Ordering::AcqRel);

            if index < self.segment_size - 1 {
                // Success: a valid index into the segment has been acquired.
                // We can now safely write to `self.head` until we increment its
                // `length` field.
                let segment = self.head.load(Ordering::Acquire);
                break (index, segment);
            } else if index == self.segment_size - 1 {
                // We are writing the last element to the segment, so we need
                // to allocate a new one and update `self.head` accordingly.
                let new_segment = Box::into_raw(alloc_segment(
                    self.segment_size,
                    self.num_readers.load(Ordering::Acquire),
                ));
                let old = self.head.swap(new_segment, Ordering::AcqRel);
                unsafe {
                    (&*old).next.store(new_segment, Ordering::Release);
                }
                self.write_index.store(0, Ordering::Release);
                break (index, old);
            } else {
                // Another thread is currently allocating a new segment. Wait for it to complete.
                // TODO: optimize
                backoff.spin();
            }
        };

        unsafe {
            // Write value to segment.
            let slot = &mut (&mut *segment).slots[index];
            ptr::write(slot.value.as_mut_ptr(), value);

            // Mark the slot as occupied.
            slot.occupied.store(true, Ordering::Release);
        }
    }

    /// Returns an iterator over values read from this event buffer.
    ///
    /// # Safety
    /// * `reader` must have been obtained through a previous call to `new_reader`
    /// _on this buffer_. If this is not the case, behavior is undefined.
    pub unsafe fn read<'a>(&'a self, reader: &'a mut RawReader<T>) -> RawIter<'a, T> {
        RawIter {
            buffer: self,
            reader,
            read: 0,
        }
    }

    /// Drops the tail segment in this buffer, setting the new tail
    /// to the next segment.
    ///
    /// # Safety
    /// * All readers must have completed reading from the segment.
    unsafe fn drop_tail(&self) {
        // Since no values are being read from this segment, and no writes
        // happen to it (since it is full), we can safely assume unique
        // access to it.
        let tail = self.tail.load(Ordering::Acquire);

        debug_assert!(!tail.is_null(), "tail must not be null");
        debug_assert!(
            !(*tail).next.load(Ordering::Acquire).is_null(),
            "tail next ptr must not be null"
        );

        self.tail
            .store((*tail).next.load(Ordering::Acquire), Ordering::Release);

        ptr::drop_in_place(tail);
    }

    /// Creates a `RawReader` for this event buffer. This operation is atomic.
    ///
    /// If events have previously been written into this buffer, it is not
    /// guaranteed that the new reader will not observe these events. However,
    /// any events written to the buffer _after_ the reader is registered will be
    /// observed.
    pub fn new_reader(&self) -> RawReader<T> {
        // Increment `reads_remaining` for every segment to ensure that
        // no use after free occurs.
        // Any new segments allocated will have the correct initial `reads_remaining`
        // value, since we increment `num_readers` before updating any segments,
        self.num_readers.fetch_add(1, Ordering::Release);
        let mut segment = self.tail.load(Ordering::Acquire);
        while !segment.is_null() {
            unsafe {
                (*segment)
                    .reads_remaining
                    .fetch_add(self.segment_size, Ordering::AcqRel);
                segment = (*segment).next.load(Ordering::Acquire);
            }
        }

        RawReader {
            segment: self.tail.load(Ordering::Acquire),
            index: 0,
        }
    }
}

impl<T> Drop for RawEventBuffer<T> {
    fn drop(&mut self) {
        let mut segment = self.tail.load(Ordering::SeqCst);
        while !segment.is_null() {
            let next = unsafe { (&*segment).next.load(Ordering::SeqCst) };
            unsafe {
                // Deallocate segment
                Box::from_raw(segment);
            }
            segment = next;
        }
    }
}

/// Iterator over values read from a `RawEventBuffer`.
///
/// Most of the actual read code is in this construct; `RawEventBuffer::read`
/// only constructs this type.
pub struct RawIter<'a, T> {
    /// Reader handle associated with this iterator.
    reader: &'a mut RawReader<T>,
    /// The event buffer being read from.
    buffer: &'a RawEventBuffer<T>,
    /// Number of values which have been read so far. When reading a segment completes,
    /// `reads_remaining` for that segment should be decremented by this value.
    read: usize,
}

impl<'a, T> RawIter<'a, T> {
    fn finish_segment(&mut self) {
        self.decr_reads_remaining();

        self.reader.index = 0;
        self.read = 0;
    }

    fn decr_reads_remaining(&mut self) {
        let new_count = unsafe {
            (*self.reader.segment)
                .reads_remaining
                .fetch_sub(self.read, Ordering::Release)
        };

        if new_count == 0 {
            // All values have been read from the segment: deallocate it.
            unsafe {
                self.buffer.drop_tail();
            }
        }
    }
}

impl<'a, T> Iterator for RawIter<'a, T> {
    type Item = &'a Value<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.segment.is_null() {
            return None;
        }

        let slot = unsafe { &(*self.reader.segment).slots[self.reader.index] };

        if !slot.occupied.load(Ordering::Relaxed) {
            // Slot not occupied: we have reached the end.
            return None;
        }

        let value = unsafe { slot.value.as_ptr().as_ref().unwrap() };

        self.read += 1;
        self.reader.index += 1;

        if self.reader.index >= self.buffer.segment_size {
            // Next value will be read from the next segment.
            self.finish_segment();
            self.reader.segment = unsafe { (*self.reader.segment).next.load(Ordering::Acquire) };
        }

        Some(value)
    }
}

impl<'a, T> Drop for RawIter<'a, T> {
    fn drop(&mut self) {
        self.decr_reads_remaining();
    }
}

/// A reader handle for `RawEventBuffer`.
pub struct RawReader<T> {
    /// Pointer to the current segment being read from. Since
    /// there are always remaining segments in the buffer (this
    /// is guaranteed by the implementation; see module-level docs),
    /// this value will never be null.
    segment: *mut Segment<T>,
    /// Index of the next value in the segment to read.
    index: usize,
}

unsafe impl<T: Send> Send for RawReader<T> {}
unsafe impl<T: Sync> Sync for RawReader<T> {}

fn alloc_segment<T>(size: usize, num_readers: usize) -> Box<Segment<T>> {
    Box::new(Segment {
        reads_remaining: AtomicUsize::new(size * num_readers),
        slots: iter::repeat_with(|| Slot {
            occupied: AtomicBool::new(false),
            value: MaybeUninit::uninit(),
        })
        .take(size)
        .collect(),
        next: AtomicPtr::new(ptr::null_mut()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    #[test]
    fn basic_write() {
        let buf = RawEventBuffer::with_segment_size(2);

        for _ in 0..100 {
            buf.write(Value::Single(vec![1]));
        }
    }

    #[test]
    fn basic_write_read() {
        let buf = RawEventBuffer::with_segment_size(2);
        let mut reader = buf.new_reader();

        for i in 0..256 {
            buf.write(Value::Single(i));
        }

        let iter = unsafe { buf.read(&mut reader) };

        for (index, val) in iter.enumerate() {
            assert_eq!(*val.single(), index as i32);
        }
    }

    #[test]
    fn concurrent_write_then_concurrent_read() {
        let buf = RawEventBuffer::with_segment_size(2);

        let t = 8;
        let n = 100_000;

        for _ in 0..t {
            crossbeam::scope(|s| {
                s.spawn(|_| {
                    for i in 0i32..n {
                        buf.write(Value::Single(i));
                    }
                });
            })
            .unwrap();
        }

        for _ in 0..t {
            crossbeam::scope(|s| {
                s.spawn(|_| {
                    let mut reader = buf.new_reader();

                    let mut num_read = 0;
                    while num_read < n {
                        let iter = unsafe { buf.read(&mut reader) };

                        for _ in iter {
                            num_read += 1
                        }
                    }

                    // Verify that there are no more values
                    let mut iter = unsafe { buf.read(&mut reader) };
                    assert!(iter.next().is_none());
                });
            })
            .unwrap();
        }
    }

    #[test]
    fn concurrent_read_and_write() {
        let buf = Arc::new(RawEventBuffer::with_segment_size(256));

        let t = 8;
        let n = 100_000;

        let received = Arc::new(Mutex::new(HashSet::new()));
        let sent = Arc::new(Mutex::new(HashSet::new()));

        crossbeam::scope(|s| {
            for thread in 0..t {
                let buf = Arc::clone(&buf);
                let received = Arc::clone(&received);
                let sent = Arc::clone(&sent);
                s.spawn(move |_| {
                    if thread >= 4 {
                        let mut reader = buf.new_reader();

                        let mut num_read = 0;
                        let to_read = n * (t / 2);
                        while num_read < to_read {
                            let iter = unsafe { buf.read(&mut reader) };

                            for val in iter {
                                num_read += 1;
                                received.lock().unwrap().insert(*val.single());
                            }
                        }
                        assert_eq!(num_read, to_read);

                        // Verify that there are no more values
                        let mut iter = unsafe { buf.read(&mut reader) };
                        assert!(iter.next().is_none());
                    } else {
                        for _ in 0..n {
                            let val = rand::random::<i32>();
                            sent.lock().unwrap().insert(val);
                            buf.write(Value::Single(val));
                        }
                    }
                });
            }
        })
        .unwrap();

        let sent = sent.lock().unwrap();
        let received = received.lock().unwrap();
        for sent_value in sent.iter() {
            assert!(received.contains(&sent_value));
        }
    }
}
