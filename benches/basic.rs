#[macro_use]
extern crate criterion;

use convent::raw::{RawEventBuffer, Value};
use criterion::{BatchSize, Criterion};
use shrev::EventChannel;

fn raw_single_write(c: &mut Criterion) {
    c.bench_function("raw_single_write", |b| {
        b.iter_batched_ref(
            || RawEventBuffer::with_segment_size(256),
            |buf| {
                for _ in 0..512 {
                    buf.write(Value::Single(1i32));
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn shrev_single_write(c: &mut Criterion) {
    c.bench_function("shrev_single_write", |b| {
        b.iter_batched_ref(
            || EventChannel::with_capacity(256),
            |buf| {
                for _ in 0..512 {
                    buf.single_write(1i32);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, raw_single_write, shrev_single_write);
criterion_main!(benches);
