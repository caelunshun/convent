#[macro_use]
extern crate criterion;

use convent::EventBuffer;
use criterion::{BatchSize, Criterion};
use shrev::EventChannel;

fn convent(c: &mut Criterion) {
    c.bench_function("convent", |b| {
        b.iter_batched_ref(
            || EventBuffer::with_segment_size(256),
            |buf| {
                let mut reader = buf.create_reader();
                for x in 0..512i32 {
                    buf.single_write(x);
                    let mut iter = reader.read();
                    assert_eq!(iter.next(), Some(&x));
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
                for x in 0..512i32 {
                    buf.single_write(x);
                    let mut iter = buf.read(&mut reader);
                    assert_eq!(iter.next(), Some(&x));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, convent, shrev);
criterion_main!(benches);
