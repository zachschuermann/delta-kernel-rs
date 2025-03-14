// TODO: rename to arrow_scan_file_visitor_benchmark.rs
// flamegraph with: cargo flamegraph --root --bench visitor_benchmark -- --bench

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU64, Arc};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use object_store::local::LocalFileSystem;
use url::Url;

use delta_kernel::actions::{get_log_schema, ADD_NAME, SIDECAR_NAME};
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::scan;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::{ExpressionRef, FileMeta, ParquetHandler, Table};

use delta_kernel::schema::{DataType, MapType, SchemaRef, StructField, StructType};

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // let path = std::fs::canonicalize(PathBuf::from(
    //     "./tests/data/00000000000000001840.checkpoint.parquet",
    // ))
    // .unwrap();
    let path = std::fs::canonicalize(PathBuf::from(
        "/Users/zach.schuermann/oss/delta-kernel-rs/kernel/tests/data/big_checkpoint",
    ))
    .unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let engine = Arc::new(
        DefaultEngine::try_new(
            &url,
            std::iter::empty::<(&str, &str)>(),
            Arc::new(TokioMultiThreadExecutor::new(rt.handle().clone())),
        )
        .unwrap(),
    );

    let table = Table::new(url.clone());

    c.bench_function(&format!("snapshot creation"), |b| {
        b.iter(|| {
            table.snapshot(engine.as_ref(), None).unwrap();
        })
    });

    let snapshot = table.snapshot(engine.as_ref(), None).unwrap();
    let scan = snapshot.into_scan_builder().build().unwrap();

    c.bench_function(&format!("scan data creation"), |b| {
        b.iter(|| {
            let _ = scan.scan_data(engine.as_ref()).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
