use convent::EventBuffer;
use crossbeam::scope;
use static_assertions::_core::time::Duration;
use std::iter;

const THREADS: usize = 8;
const COUNT: usize = 25_000;
const TOTAL: usize = THREADS * COUNT;

#[test]
fn vec_write() {
    let buf = EventBuffer::new();

    let mut reader = buf.create_reader();

    let mut vec = vec![0, 1, 3, 4, 5];
    buf.vec_write(vec.clone());
    buf.single_write(1);

    vec.push(1);
    assert_eq!(reader.read().copied().collect::<Vec<_>>(), vec);
}

#[test]
fn mpsc() {
    let buf = EventBuffer::new();

    scope(|s| {
        let mut reader = buf.create_reader();
        for _ in 0..THREADS {
            s.spawn(|_| {
                for i in 0..COUNT {
                    buf.single_write(i);
                }
            });

            dbg!();
        }

        s.spawn(move |_| {
            std::thread::sleep(Duration::from_millis(10));
            let mut num_read = 0;

            while num_read < TOTAL {
                num_read += reader.read().collect::<Vec<_>>().len();
            }

            assert_eq!(num_read, TOTAL);
            assert_eq!(reader.read().next(), None);
        });
    })
    .unwrap();
}

#[test]
fn mpmc() {
    let buf = EventBuffer::new();

    scope(|s| {
        // Pre-create readers so they observe all events
        let mut readers: Vec<_> = iter::repeat_with(|| Some(buf.create_reader()))
            .take(THREADS)
            .collect();
        for thread in 0..THREADS {
            s.spawn(|_| {
                for i in 0..COUNT {
                    buf.single_write(i);
                }
            });

            let mut reader = readers[thread].take().unwrap();

            s.spawn(move |_| {
                let mut num_read = 0;

                while num_read < TOTAL {
                    num_read += reader.read().collect::<Vec<_>>().len();
                }

                assert_eq!(num_read, TOTAL);
                assert_eq!(reader.read().next(), None);
            });
        }
    })
    .unwrap();
}

#[test]
fn spsc() {
    let buf = EventBuffer::new();

    scope(|s| {
        let mut reader = buf.create_reader();
        s.spawn(|_| {
            for i in 0..COUNT {
                buf.single_write(i);
            }
        });

        s.spawn(move |_| {
            let expected_result: Vec<_> = (0..COUNT).collect();

            let mut result = Vec::with_capacity(COUNT);

            while result.len() != expected_result.len() {
                let read = reader.read().collect::<Vec<_>>();
                result.extend(read.into_iter().copied());
            }
        });
    })
    .unwrap();
}

#[test]
fn atomic_reader_creation() {
    let buf = EventBuffer::new();

    scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|_| {
                for i in 0..COUNT {
                    buf.single_write(i);
                }
            });
            s.spawn(|_| {
                std::thread::sleep(Duration::from_millis(10));
                for _ in 0..10 {
                    let mut reader = buf.create_reader();
                    let _ = reader.read().collect::<Vec<_>>();
                }
            });
        }
    })
    .unwrap();
}
