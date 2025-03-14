// flamegraph with: cargo flamegraph --root --bench arrow_scan_file_visitor_bench -- --bench

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

struct ScanFile {
    path: String,
    size: i64,
    transform: Option<ExpressionRef>,
    dv_info: DvInfo,
}

fn callback(
    count: &mut Arc<AtomicU64>,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    _: HashMap<String, String>,
) {
    let _scan_file = ScanFile {
        path: path.to_string(),
        size,
        transform,
        dv_info,
    };
    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

fn criterion_benchmark(c: &mut Criterion) {
    // ~200ms for big checkpoint
    c.bench_function(&format!("visit scan data"), |b| {
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
        let snapshot = table.snapshot(engine.as_ref(), None).unwrap();
        let scan = snapshot.into_scan_builder().build().unwrap();
        let scan_data: Vec<_> = scan.scan_data(engine.as_ref()).unwrap().collect();

        b.iter(|| {
            for res in scan_data.iter() {
                let (data, vector, transforms) = res.as_ref().unwrap();
                let num_rows = data.len();
                let counter = Arc::new(AtomicU64::default());
                scan::state::visit_scan_files(
                    data.as_ref(),
                    &vector,
                    &transforms,
                    counter.clone(),
                    callback,
                )
                .unwrap();
                assert_eq!(
                    counter.load(std::sync::atomic::Ordering::SeqCst),
                    num_rows as u64 - vector.iter().filter(|x| !*x).count() as u64
                );
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
