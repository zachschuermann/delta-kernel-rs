use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::schema::{DataType, SchemaRef, StructField, StructType};
use delta_kernel::{Engine, Table};

// fixme use in macro below
const PROTOCOL_METADATA_TEMPLATE: &'static str = r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
{{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1677811175819}}}}"#;

// setup default engine with in-memory object store.
fn setup(storage_prefix: Path) -> (Arc<dyn ObjectStore>, impl Engine) {
    let storage = Arc::new(InMemory::new());
    (
        storage.clone(),
        DefaultEngine::new(
            storage,
            storage_prefix,
            Arc::new(TokioBackgroundExecutor::new()),
        ),
    )
}

// we provide this table creation function since we only do appends to existing tables for now.
// this will just create an empty table with the given schema. (just protocol + metadata actions)
async fn create_table(
    store: Arc<dyn ObjectStore>,
    table_path: Url,
    // fixme use this schema
    _schema: SchemaRef,
) -> Result<Table, Box<dyn std::error::Error>> {
    // put 0.json with protocol + metadata
    let table_id = "test_id";
    let schema_string = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"number\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}"#;
    let data = format!(
        r#"{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}}}}
{{"metaData":{{"id":"{}","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1677811175819}}}}"#,
        table_id, schema_string
    ).into_bytes();
    store
        .put(
            &Path::from(format!("{table_path}/_delta_log/00000000000000000000.json",)),
            data.into(),
        )
        .await?;
    Ok(Table::new(table_path))
}

// tests
// 1. basic happy path append
// 2. append with schema mismatch
// 3. append with partitioned table

#[tokio::test]
async fn basic_append() -> Result<(), Box<dyn std::error::Error>> {
    // setup in-memory object store and default engine
    let table_location = Url::parse("memory://test_table/").unwrap();
    // prefix is just "/" since we are using in-memory object store - don't really care about
    // collisions TODO check?? lol
    let (store, engine) = setup(Path::from("/"));

    // create a simple table: one int column named 'number'
    let schema = Arc::new(StructType::new(vec![StructField::new(
        "number",
        DataType::INTEGER,
        true,
    )]));
    let table = create_table(store.clone(), table_location, schema.clone()).await?;

    // append an arrow record batch (vec of record batches??)
    let append_data = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
    )?;
    let append_data = Arc::new(ArrowEngineData::new(append_data));

    let append_data2 = RecordBatch::try_new(
        Arc::new(schema.as_ref().try_into()?),
        vec![Arc::new(arrow::array::Int32Array::from(vec![4, 5, 6]))],
    )?;
    let append_data2 = Arc::new(ArrowEngineData::new(append_data2));

    // create a new txn based on current table version
    let txn_builder = table.new_transaction_builder();
    let txn = txn_builder.build(&engine);

    // // create a new async task to do the write (simulate executors)
    // let writer = tokio::task::spawn(async {
    //     // write will transform logical data to physical data and write to object store
    //     txn.write(append_data).expect("write data files");
    // });
    txn.write(&engine, append_data).expect("write append_data");
    txn.write(&engine, append_data2).expect("write append_data2");

    // wait for writes to complete
    // let _ = writer.await?;

    txn.commit()?;

    Ok(())
}