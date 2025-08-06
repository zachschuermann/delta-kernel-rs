//! Snapshot builders docs TODO!

use crate::actions::{Metadata, Protocol};
use crate::path::ParsedLogPath;
use crate::{DeltaResult, Engine, Snapshot, Version};
use delta_kernel_derive::internal_api;

use url::Url;

// NOTE: in the future we could (should) modify this to disallow calling APIs like 'latest()' or
// 'at_version()' after unresolved_metadata is provided.
#[internal_api]
pub(crate) struct CatalogSnapshotBuilder {
    table_root: Url,
    log_tail: Vec<ParsedLogPath>,
    target_version: Option<Version>,
    unresolved_metadata: Option<(Protocol, Metadata, Version)>,
}

impl CatalogSnapshotBuilder {
    pub(crate) fn new(table_root: Url, log_tail: impl IntoIterator<Item = ParsedLogPath>) -> Self {
        Self {
            table_root,
            log_tail: log_tail.into_iter().collect(),
            target_version: None,
            unresolved_metadata: None,
        }
    }

    pub fn latest(mut self) -> Self {
        self.target_version = None;
        self
    }

    pub fn at_version(mut self, version: Version) -> Self {
        self.target_version = Some(version);
        self
    }

    pub fn with_metadata(
        mut self,
        protocol: Protocol,
        metadata: Metadata,
        version: Version,
    ) -> Self {
        self.unresolved_metadata = Some((protocol, metadata, version));
        self
    }

    pub fn build(self, engine: &dyn Engine) -> DeltaResult<Snapshot> {
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

        // FIXME: must check if catalogManaged feature is enabled
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};

    use super::*;

    #[test]
    #[ignore]
    #[allow(unreachable_code, unused)]
    fn test_snapshot_builder() -> Result<(), Box<dyn std::error::Error>> {
        let engine: DefaultEngine<TokioBackgroundExecutor> = todo!();

        // TODO: we should rename existing, and they must check no catalogManaged feature
        // let local_snapshot = Snapshot::new_local_latest(engine)?;
        // let local_snapshot = Snapshot::new_local_at(version, engine)?;

        // simple catalog: require catalogManaged feature. but with no log_tail, nor metadata this
        // just degenerates to the same as creating a path-based snapshot.
        let table_root = Url::parse("memory:///test_table").unwrap();
        let catalog_snapshot = Snapshot::build_from_catalog(table_root, [])
            .latest() // or at_version(version)
            .build(&engine)?;

        // catalog that tracks Protocol and Metadata
        // fetch from catalog
        let (protocol, metadata, version) = todo!(); // fetch from catalog
        let catalog_snapshot = Snapshot::build_from_catalog(table_root, [])
            .with_metadata(protocol, metadata, version)
            // TODO: we need to disallow latest() or at_version() now
            .build(&engine)?;

        // in any of these you can provide log_tail
        let log_tail: Vec<ParsedLogPath> = todo!(); // fetch log tail from catalog
        let catalog_snapshot = Snapshot::build_from_catalog(table_root, log_tail)
            .latest() // or at_version(version) or add metadata
            .build(&engine)?;

        Ok(())
    }
}
