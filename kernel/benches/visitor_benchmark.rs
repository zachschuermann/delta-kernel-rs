//! TODO: rename to arrow_visitor_benchmark.rs

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use object_store::local::LocalFileSystem;
use url::Url;

use delta_kernel::actions::get_log_schema;
use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::parquet::DefaultParquetHandler;
use delta_kernel::scan;
use delta_kernel::scan::state::{DvInfo, Stats};
use delta_kernel::{ExpressionRef, FileMeta, ParquetHandler};

struct ScanFile {
    path: String,
    size: i64,
    transform: Option<ExpressionRef>,
    dv_info: DvInfo,
}

fn callback(
    _none: &mut (),
    path: &str,
    size: i64,
    _stats: Option<Stats>,
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
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let parquet_handler = DefaultParquetHandler::new(
        Arc::new(LocalFileSystem::new()),
        TokioMultiThreadExecutor::new(rt.handle().clone()).into(),
    );
    let path = std::fs::canonicalize(PathBuf::from(
        "./tests/data/00000000000000001840.checkpoint.parquet",
    ))
    .unwrap();
    let url = Url::from_directory_path(path).unwrap();
    let files = [FileMeta::new(url, 0, 0)];
    let checkpoint_read_schema = get_log_schema().project(&["add"]).unwrap();
    let mut checkpoint_data = parquet_handler
        .read_parquet_files(&files, checkpoint_read_schema, None)
        .unwrap();

    let checkpoint_chunk = checkpoint_data.next().unwrap().unwrap();
    let sv = vec![true; checkpoint_chunk.len()];
    let transforms = vec![None; checkpoint_chunk.len()];
    let num_rows = checkpoint_chunk.len();
    c.bench_function(&format!("visit scan files: {num_rows} rows"), |b| {
        b.iter(|| {
            scan::state::visit_scan_files(checkpoint_chunk.as_ref(), &sv, &transforms, (), callback)
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
