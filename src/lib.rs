use crate::raw::{RawEventBuffer, RawIter, RawReader};
use std::iter::Flatten;
use std::sync::Arc;

mod raw;
pub use raw::{Value, ValueIter};

const DEFAULT_SEGMENT_SIZE: usize = 256;

pub struct EventBuffer<T> {
    raw: Arc<RawEventBuffer<T>>,
}

impl<T> EventBuffer<T> {
    pub fn new() -> Self {
        Self::with_segment_size(DEFAULT_SEGMENT_SIZE)
    }

    pub fn with_segment_size(segment_size: usize) -> Self {
        Self {
            raw: Arc::new(RawEventBuffer::with_segment_size(segment_size)),
        }
    }

    pub fn create_reader(&self) -> EventReader<T> {
        EventReader {
            buffer: Arc::clone(&self.raw),
            raw: self.raw.new_reader(),
        }
    }

    pub fn create_writer(&self) -> EventWriter<T> {
        EventWriter {
            raw: Arc::clone(&self.raw),
        }
    }

    pub fn single_write(&self, value: T) {
        self.raw.write(Value::Single(value));
    }

    pub fn vec_write(&self, vec: Vec<T>) {
        self.raw.write(Value::Multiple(vec));
    }

    pub fn iter_write<I>(&self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.vec_write(iter.into_iter().collect());
    }
}

impl<T> Default for EventBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EventReader<T> {
    buffer: Arc<RawEventBuffer<T>>,
    raw: RawReader<T>,
}

impl<T> EventReader<T> {
    pub fn read<'a>(&'a mut self) -> Flatten<EventIter<'a, T>> {
        let iter = unsafe { self.buffer.read(&mut self.raw) };

        let iter = EventIter { raw: iter };

        // Flatten to convert Value::Multiple to multiple elements
        iter.flatten()
    }
}

impl<T> Clone for EventReader<T> {
    fn clone(&self) -> Self {
        Self {
            buffer: Arc::clone(&self.buffer),
            raw: self.buffer.new_reader(),
        }
    }
}

pub struct EventIter<'a, T> {
    raw: RawIter<'a, T>,
}

impl<'a, T> Iterator for EventIter<'a, T> {
    type Item = &'a Value<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next()
    }
}

#[derive(Clone)]
pub struct EventWriter<T> {
    raw: Arc<RawEventBuffer<T>>,
}

impl<T> EventWriter<T> {
    pub fn single_write(&self, value: T) {
        self.raw.write(Value::Single(value));
    }

    pub fn vec_write(&self, vec: Vec<T>) {
        self.raw.write(Value::Multiple(vec));
    }

    pub fn iter_write<I>(&self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.vec_write(iter.into_iter().collect());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::*;

    #[test]
    fn check_traits() {
        assert_impl_all!(EventBuffer::<i32>: Send, Sync, Default);
        assert_impl_all!(EventReader::<i32>: Send, Sync, Clone);
        assert_impl_all!(EventWriter::<i32>: Send, Sync, Clone);
    }
}
