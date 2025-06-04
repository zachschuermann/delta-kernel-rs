//! TODO: resolved table mod
use std::sync::Arc;

use crate::actions::{Metadata, Protocol};
use crate::log_segment::LogSegment;
use crate::scan::ScanBuilder;
use crate::schema::SchemaRef;
use crate::table_features::ColumnMappingMode;
use crate::{EngineData, Version};

use url::Url;

pub type CommitResult = Result<Version, CommitError>;
pub enum CommitError {
    /// The commit failed due to a conflict
    Conflict(Version),
    /// The commit failed due to bad
    Bad(String),
}

/// something that has a:
/// - table_root (URL root path)
/// - commit method (how to commit to the table)
pub trait UnresolvedTable {
    /// Returns the URL of the table's storage root path
    fn table_root(&self) -> &Url;

    /// Commit actions to the table
    fn commit(&self, actions: &dyn Iterator<Item = Box<dyn EngineData>>) -> CommitResult;
}

pub trait ProtocolMetadata {
    fn protocol(&self) -> &Protocol;
    fn metadata(&self) -> &Metadata;
    // fn into_parts(self) -> (Protocol, Metadata); // maybe?
}

pub trait Versioned {
    fn version(&self) -> Version;
}

pub trait LogProvider {
    fn log_segment(&self) -> &LogSegment;
}

pub trait ResolvedTable: UnresolvedTable + ProtocolMetadata + Versioned + LogProvider {
    fn schema(&self) -> SchemaRef {
        todo!();
    }

    // all provided methods: basically our existing snapshot code
    fn scan_builder(self: Arc<Self>) -> ScanBuilder {
        todo!()
    }

    fn column_mapping_mode(&self) -> ColumnMappingMode {
        todo!()
    }
}
impl<T: UnresolvedTable + ProtocolMetadata + Versioned + LogProvider> ResolvedTable for T {}
