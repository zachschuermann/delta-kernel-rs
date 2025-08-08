//! Snapshot builders docs TODO!

use crate::actions::{Metadata, Protocol};
use crate::log_segment::LogSegment;
use crate::path::ParsedLogPath;
use crate::table_configuration::TableConfiguration;
use crate::{DeltaResult, Engine, Snapshot, Version};
use delta_kernel_derive::internal_api;

use url::Url;

// NOTE: in the future we could (should) modify this to disallow calling APIs like 'latest()' or
// 'at_version()' after unresolved_metadata is provided.
#[internal_api]
pub(crate) struct CatalogSnapshotBuilder {
    table_root: Url,
    log_tail: Vec<ParsedLogPath>,
}

#[internal_api]
pub(crate) struct CatalogSnapshotBuilderWithMetadata {
    table_root: Url,
    log_tail: Vec<ParsedLogPath>,
    protocol: Protocol,
    metadata: Metadata,
    version: Version,
}

impl CatalogSnapshotBuilder {
    // TODO: do we want ParsedLogPath to be our public API? we should consider something
    // separate/smaller? LogData?
    pub(crate) fn new(table_root: Url, log_tail: impl IntoIterator<Item = ParsedLogPath>) -> Self {
        Self {
            table_root,
            log_tail: log_tail.into_iter().collect(),
        }
    }

    pub fn with_metadata(
        self,
        protocol: Protocol,
        metadata: Metadata,
        version: Version,
    ) -> CatalogSnapshotBuilderWithMetadata {
        CatalogSnapshotBuilderWithMetadata::new(self, protocol, metadata, version)
    }

    // two main paths currently, either:
    // 1. we have all the unresolved metadata already: we build TableConfiguration from it and
    // build our own LogSegment then create Snapshot from those pieces
    // 2. we don't have unresolved metadata, so we have to kick off PM replay to get it
    //
    // In either case, the log_tail is just encapsulated in the LogSegment
    //
    // NOTE: we may not want to require Snapshot to always have LogSegment in the future -
    // example of utility for no LogSegment would be if user just wants to write to a table,
    // then they would rather just create a snapshot (with PM from catalog etc - then
    // validate read/write-ability) but then not have to make entire log segment and instead
    // directly just perform a write.
    //
    // FIXME: must check if catalogManaged feature is enabled

    pub fn build_at(self, version: Version, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        self.build(Some(version), engine)
    }

    pub fn build_latest(self, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        self.build(None, engine)
    }

    fn build(self, version: Option<Version>, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        let log_segment = LogSegment::for_snapshot(
            engine.storage_handler().as_ref(),
            self.table_root.join("_delta_log/")?,
            self.log_tail,
            version,
        )?;

        Snapshot::try_new_from_log_segment(self.table_root, log_segment, engine)
    }
}

impl CatalogSnapshotBuilderWithMetadata {
    pub(crate) fn new(
        builder: CatalogSnapshotBuilder,
        protocol: Protocol,
        metadata: Metadata,
        version: Version,
    ) -> Self {
        let CatalogSnapshotBuilder {
            table_root,
            log_tail,
        } = builder;
        Self {
            table_root,
            log_tail,
            protocol,
            metadata,
            version,
        }
    }

    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Snapshot> {
        let log_segment = LogSegment::for_snapshot(
            engine.storage_handler().as_ref(),
            self.table_root.join("_delta_log/")?,
            self.log_tail,
            None,
        )?;

        // we have unresolved metadata, so we can build TableConfiguration
        let table_configuration = TableConfiguration::try_new(
            self.metadata,
            self.protocol,
            self.table_root,
            self.version,
        )?;
        Ok(Snapshot::new(log_segment, table_configuration))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};

    use crate::object_store::memory::InMemory;
    use crate::object_store::ObjectStore;

    use serde_json::json;

    use super::*;

    fn setup_test() -> (
        Arc<DefaultEngine<TokioBackgroundExecutor>>,
        Arc<dyn ObjectStore>,
        Url,
    ) {
        let table_root = Url::parse("memory:///test_table").unwrap();
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngine::new(
            store.clone(),
            Arc::new(TokioBackgroundExecutor::new()),
        ));
        (engine, store, table_root)
    }

    fn create_table(store: &Arc<dyn ObjectStore>, table_root: &Url) -> DeltaResult<()> {
        let protocol = json!({
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["catalogManaged"],
            "writerFeatures": ["catalogManaged"],
        });

        let metadata = json!({
            "id": "test-table-id",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"val\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1587968585495i64
        });

        // Create commit 0 with protocol and metadata
        let commit0 = vec![
            json!({
                "protocol": protocol
            }),
            json!({
                "metaData": metadata
            }),
        ];

        // Write commit 0
        let commit0_data = commit0
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join("\n");

        let path = crate::path::Path::from(format!("_delta_log/{:020}.json", 0).as_str());
        futures::executor::block_on(async { store.put(&path, commit0_data.into()).await })?;

        // Create commit 1 with a single addFile action
        let commit1 = vec![json!({
            "add": {
                "path": "part-00000-test.parquet",
                "partitionValues": {},
                "size": 1024,
                "modificationTime": 1587968586000i64,
                "dataChange": true,
                "stats": null,
                "tags": null
            }
        })];

        // Write commit 1
        let commit1_data = commit1
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join("\n");

        let path = crate::path::Path::from(format!("_delta_log/{:020}.json", 1).as_str());
        futures::executor::block_on(async { store.put(&path, commit1_data.into()).await })?;

        Ok(())
    }

    #[test]
    fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: we should rename existing, and they must check no catalogManaged feature
        // let local_snapshot = Snapshot::new_local_latest(engine)?;
        // let local_snapshot = Snapshot::new_local_at(version, engine)?;

        let (engine, store, table_root) = setup_test();

        create_table(&store, &table_root)?;

        // simple catalog: require catalogManaged feature. but with no log_tail, nor metadata this
        // just degenerates to the same as creating a path-based snapshot.
        let catalog_snapshot_latest =
            Snapshot::build_from_catalog(table_root.clone(), []).build_latest(engine.as_ref())?;
        assert_eq!(catalog_snapshot_latest.version(), 1);

        let catalog_snapshot_time_travel =
            Snapshot::build_from_catalog(table_root, []).build_at(0, engine.as_ref())?;
        assert_eq!(catalog_snapshot_time_travel.version(), 0);

        Ok(())
    }

    #[ignore]
    #[test]
    fn test_snapshot_builder_with_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, store, table_root) = setup_test();
        // catalog that tracks Protocol and Metadata
        // fetch from catalog
        let (protocol, metadata, version) = todo!(); // fetch from catalog
        let catalog_snapshot = Snapshot::build_from_catalog(table_root, [])
            .with_metadata(protocol, metadata, version)
            .build(&engine)?;

        // in any of these you can provide log_tail
        let log_tail: Vec<ParsedLogPath> = todo!(); // fetch log tail from catalog
        let catalog_snapshot =
            Snapshot::build_from_catalog(table_root, log_tail).build_latest(&engine)?;

        Ok(())
    }
}
