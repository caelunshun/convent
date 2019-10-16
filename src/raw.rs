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
pub struct RawEventBuffer<T> {
    head: AtomicPtr<Segment<T>>,
    tail: AtomicPtr<Segment<T>>,
    num_readers: usize,
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
            num_readers: 0,
            segment_size,
            write_index: AtomicUsize::new(0),
        }
    }

    /// Atomically writes one or more events into this buffer.
    pub fn write(&self, value: Value<T>) {
        // Find an index into the segment to write to.

        // We ensure that no use-after-free occurs by not reading from `self.head`
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
                let new_segment = Box::into_raw(alloc_segment(self.segment_size, self.num_readers));
                let old = self.head.swap(new_segment, Ordering::AcqRel);
                unsafe {
                    (&*old).next.store(new_segment, Ordering::Release);
                }
                self.write_index.store(1, Ordering::Release);
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

    #[test]
    fn big_write() {
        let buf = RawEventBuffer::with_segment_size(2);

        for _ in 0..1_000_000 {
            buf.write(Value::Single(vec![1]));
        }
    }
}
