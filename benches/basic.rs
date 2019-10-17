#[macro_use]
extern crate criterion;

use convent::raw::{RawEventBuffer, Value};
use criterion::{BatchSize, Criterion};
use shrev::EventChannel;

fn convent(c: &mut Criterion) {
    c.bench_function("convent", |b| {
        b.iter_batched_ref(
            || RawEventBuffer::with_segment_size(256),
            |buf| {
                let mut reader = buf.new_reader();
                for _ in 0..512 {
                    buf.write(Value::Single(1i32));
                    let mut iter = unsafe { buf.read(&mut reader) };
                    assert_eq!(iter.next().unwrap().single(), &1);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn shrev(c: &mut Criterion) {
    c.bench_function("shrev", |b| {
        b.iter_batched_ref(
            || EventChannel::with_capacity(256),
            |buf| {
                let mut reader = buf.register_reader();
                for _ in 0..512 {
                    buf.single_write(1i32);
                    let mut iter = buf.read(&mut reader);
                    assert_eq!(iter.next(), Some(&1));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, convent, shrev);
criterion_main!(benches);
