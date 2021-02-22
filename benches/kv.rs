use btreekv::RSDB;
use criterion::{criterion_group, criterion_main, Criterion};
use std::path::Path;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut db = RSDB::new(Path::new("/tmp/rsdb")).unwrap();
    c.bench_function("kv set", |b| b.iter(|| db.set(b"k1", b"v1").unwrap()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
