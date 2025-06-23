//! test UC catalog

use std::sync::Arc;

use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::resolved_table::ResolvedTable;
// use delta_kernel::resolved_table::IntoUnresolvedTable;
use uc_catalog::*;
use uc_client::UCClient;

#[test]
fn dumb_test() -> Result<(), Box<dyn std::error::Error>> {
    // two major pieces we need to work with tables: (1) catalog, (2) engine
    let client = UCClient::default();
    // note with our mock client we do in-mem store
    let object_store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(
        object_store,
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    // now we can go read a table!
    let table = DumbTable::try_new("test_table", &client)?;

    // other API idea: table.at_version(v).resolve(engine)
    // let resolved_table = table.resolve(None, engine.as_ref())?;
    let resolved_table =
        ResolvedTable::try_new(table.table_root().clone(), vec![], engine.as_ref(), None)?;

    // now we have the usual kernel APIs
    let scan = resolved_table.into_scan_builder().build()?;
    let data = scan.execute(engine)?;

    for batch in data {
        let batch = ArrowEngineData::try_from_engine_data(batch?.raw_data?)?;
        println!("RecordBatch: {:?}", batch.record_batch());
    }

    Ok(())
}

#[test]
fn smart_catalog_test() -> Result<(), Box<dyn std::error::Error>> {
    // two major pieces we need to work with tables: (1) catalog, (2) engine
    let client = UCClient::default();
    // note with our mock client we do in-mem store
    let object_store = Arc::new(LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(
        object_store,
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    // now we can go read a table!
    let table = UCTable::try_new("test_table", &client)?;

    // other API idea: trait-based
    // let resolved_table = table.at_version(v).resolve(engine)
    // let resolved_table = table.resolve(engine.as_ref())?; // latest

    // how do we want to handle this bounday? lots of clones are gross
    let resolved_table = ResolvedTable::try_new_from_metadata(
        table.table_root().clone(),
        vec![], // FIXME:
        table.protocol().clone(),
        table.metadata().clone(),
        table.latest_version(),
        engine.as_ref(),
    )?;

    // now we have the usual kernel APIs
    let scan = resolved_table.into_scan_builder().build()?;
    let data = scan.execute(engine)?;

    for batch in data {
        let batch = ArrowEngineData::try_from_engine_data(batch?.raw_data?)?;
        println!("RecordBatch: {:?}", batch.record_batch());
    }

    Ok(())
}
