//! TODO: rename to arrow_visitor_benchmark.rs

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU64, Arc};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use object_store::local::LocalFileSystem;
use url::Url;

use delta_kernel::actions::{get_log_schema, ADD_NAME, SIDECAR_NAME};
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
    count: &mut AtomicU64,
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
    println!("stats: {:?}", stats);
    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

fn criterion_benchmark(c: &mut Criterion) {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

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
    let checkpoint_read_schema = get_log_schema().project(&[ADD_NAME, SIDECAR_NAME]).unwrap();
    let mut checkpoint_data = parquet_handler
        .read_parquet_files(&files, checkpoint_read_schema, None)
        .unwrap();

    let checkpoint_chunk = checkpoint_data.next().unwrap().unwrap();

    // use delta_kernel::engine::arrow_data::ArrowEngineData;
    // let chunk: delta_kernel::arrow::record_batch::RecordBatch = checkpoint_chunk
    //     .into_any()
    //     .downcast::<ArrowEngineData>()
    //     .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))
    //     .unwrap()
    //     .into();

    // // print record batch
    // let formatted = delta_kernel::arrow::util::pretty::pretty_format_batches(&[chunk.clone()])
    //     .unwrap()
    //     .to_string();

    // println!("record batch:\n{formatted}");

    let sv = vec![true; checkpoint_chunk.len()];
    let transforms = vec![None; checkpoint_chunk.len()];
    let num_rows = checkpoint_chunk.len();
    c.bench_function(&format!("visit scan files: {num_rows} rows"), |b| {
        b.iter(|| {
            let counter = scan::state::visit_scan_files(
                checkpoint_chunk.as_ref(),
                &sv,
                &transforms,
                AtomicU64::default(),
                callback,
            );
            println!(
                "counter: {}",
                counter.unwrap().load(std::sync::atomic::Ordering::SeqCst)
            );
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
