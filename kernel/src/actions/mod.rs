//! Provides parsing and manipulation of the various actions defined in the [Delta
//! specification](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use self::deletion_vector::DeletionVectorDescriptor;
use crate::expressions::{ArrayData, MapData, Scalar, StructData};
use crate::schema::{
    ArrayType, DataType, MapType, SchemaRef, StructField, StructType, ToSchema as _,
};
use crate::table_features::{
    ReaderFeature, WriterFeature, SUPPORTED_READER_FEATURES, SUPPORTED_WRITER_FEATURES,
};
use crate::table_properties::TableProperties;
use crate::utils::require;
use crate::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandlerExtension as _, FileMeta,
    IntoEngineData, RowVisitor as _,
};

use url::Url;
use visitors::{MetadataVisitor, ProtocolVisitor};

use delta_kernel_derive::{internal_api, IntoEngineData, ToSchema};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

const KERNEL_VERSION: &str = env!("CARGO_PKG_VERSION");
const UNKNOWN_OPERATION: &str = "UNKNOWN";

pub mod deletion_vector;
pub mod set_transaction;

pub(crate) mod crc;
pub(crate) mod domain_metadata;

// see comment in ../lib.rs for the path module for why we include this way
#[cfg(feature = "internal-api")]
pub mod visitors;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod visitors;

#[internal_api]
pub(crate) const ADD_NAME: &str = "add";
#[internal_api]
pub(crate) const REMOVE_NAME: &str = "remove";
#[internal_api]
pub(crate) const METADATA_NAME: &str = "metaData";
#[internal_api]
pub(crate) const PROTOCOL_NAME: &str = "protocol";
#[internal_api]
pub(crate) const SET_TRANSACTION_NAME: &str = "txn";
#[internal_api]
pub(crate) const COMMIT_INFO_NAME: &str = "commitInfo";
#[internal_api]
pub(crate) const CDC_NAME: &str = "cdc";
#[internal_api]
pub(crate) const SIDECAR_NAME: &str = "sidecar";
#[internal_api]
pub(crate) const CHECKPOINT_METADATA_NAME: &str = "checkpointMetadata";
#[internal_api]
pub(crate) const DOMAIN_METADATA_NAME: &str = "domainMetadata";

pub(crate) const INTERNAL_DOMAIN_PREFIX: &str = "delta.";

static LOG_ADD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::nullable(
        ADD_NAME,
        Add::to_schema(),
    )]))
});

static LOG_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([
        StructField::nullable(ADD_NAME, Add::to_schema()),
        StructField::nullable(REMOVE_NAME, Remove::to_schema()),
        StructField::nullable(METADATA_NAME, Metadata::to_schema()),
        StructField::nullable(PROTOCOL_NAME, Protocol::to_schema()),
        StructField::nullable(SET_TRANSACTION_NAME, SetTransaction::to_schema()),
        StructField::nullable(COMMIT_INFO_NAME, CommitInfo::to_schema()),
        StructField::nullable(CDC_NAME, Cdc::to_schema()),
        StructField::nullable(SIDECAR_NAME, Sidecar::to_schema()),
        StructField::nullable(CHECKPOINT_METADATA_NAME, CheckpointMetadata::to_schema()),
        StructField::nullable(DOMAIN_METADATA_NAME, DomainMetadata::to_schema()),
    ]))
});

static LOG_COMMIT_INFO_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::nullable(
        COMMIT_INFO_NAME,
        CommitInfo::to_schema(),
    )]))
});

static LOG_PROTOCOL_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::nullable(
        PROTOCOL_NAME,
        Protocol::to_schema(),
    )]))
});

static LOG_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::nullable(
        METADATA_NAME,
        Metadata::to_schema(),
    )]))
});

static LOG_TXN_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::nullable(
        SET_TRANSACTION_NAME,
        SetTransaction::to_schema(),
    )]))
});

static LOG_DOMAIN_METADATA_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(StructType::new([StructField::nullable(
        DOMAIN_METADATA_NAME,
        DomainMetadata::to_schema(),
    )]))
});

#[internal_api]
pub(crate) fn get_log_schema() -> &'static SchemaRef {
    &LOG_SCHEMA
}

#[internal_api]
pub(crate) fn get_log_add_schema() -> &'static SchemaRef {
    &LOG_ADD_SCHEMA
}

pub(crate) fn get_log_commit_info_schema() -> &'static SchemaRef {
    &LOG_COMMIT_INFO_SCHEMA
}

pub(crate) fn get_log_protocol_schema() -> &'static SchemaRef {
    &LOG_PROTOCOL_SCHEMA
}

pub(crate) fn get_log_metadata_schema() -> &'static SchemaRef {
    &LOG_METADATA_SCHEMA
}

pub(crate) fn get_log_txn_schema() -> &'static SchemaRef {
    &LOG_TXN_SCHEMA
}

pub(crate) fn get_log_domain_metadata_schema() -> &'static SchemaRef {
    &LOG_DOMAIN_METADATA_SCHEMA
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(
    any(test, feature = "internal-api"),
    derive(Serialize, Deserialize),
    serde(rename_all = "camelCase")
)]
#[internal_api]
pub(crate) struct Format {
    /// Name of the encoding for files in this table
    pub(crate) provider: String,
    /// A map containing configuration options for the format
    pub(crate) options: HashMap<String, String>,
}

impl Default for Format {
    fn default() -> Self {
        Self {
            provider: String::from("parquet"),
            options: HashMap::new(),
        }
    }
}

impl TryFrom<Format> for Scalar {
    type Error = Error;

    fn try_from(format: Format) -> DeltaResult<Self> {
        let provider = Scalar::from(format.provider);
        let options = MapData::try_new(
            MapType::new(DataType::STRING, DataType::STRING, false),
            format.options,
        )
        .map(Scalar::Map)?;
        Ok(Scalar::Struct(StructData::try_new(
            Format::to_schema().fields().cloned().collect(),
            vec![provider, options],
        )?))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(
    any(test, feature = "internal-api"),
    derive(Serialize, Deserialize),
    serde(rename_all = "camelCase")
)]
#[internal_api]
pub(crate) struct Metadata {
    /// Unique identifier for this table
    pub(crate) id: String,
    /// User-provided identifier for this table
    pub(crate) name: Option<String>,
    /// User-provided description for this table
    pub(crate) description: Option<String>,
    /// Specification of the encoding for the files stored in the table
    pub(crate) format: Format,
    /// Schema of the table
    pub(crate) schema_string: String,
    /// Column names by which the data should be partitioned
    pub(crate) partition_columns: Vec<String>,
    /// The time when this metadata action is created, in milliseconds since the Unix epoch
    pub(crate) created_time: Option<i64>,
    /// Configuration options for the metadata action. These are parsed into [`TableProperties`].
    pub(crate) configuration: HashMap<String, String>,
}

impl Metadata {
    // TODO: remove allow(dead_code) after we use this API in CREATE TABLE, etc.
    #[allow(dead_code)]
    pub(crate) fn try_new(
        name: Option<String>,
        description: Option<String>,
        schema: StructType,
        partition_columns: Vec<String>,
        created_time: i64,
        configuration: HashMap<String, String>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            description,
            // As of Delta Lake 0.3.0, user-facing APIs only allow the creation of tables where
            // format = 'parquet' and options = {}. Support for reading other formats is present
            // both for legacy reasons and to enable possible support for other formats in the
            // future (See delta-io/delta#87).
            format: Format::default(),
            schema_string: serde_json::to_string(&schema)?,
            partition_columns,
            created_time: Some(created_time),
            configuration,
        })
    }

    // TODO(#1068/1069): make these just pub directly or make better internal_api macro for fields
    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn created_time(&self) -> Option<i64> {
        self.created_time
    }

    pub(crate) fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Metadata>> {
        let mut visitor = MetadataVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.metadata)
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn configuration(&self) -> &HashMap<String, String> {
        &self.configuration
    }

    #[internal_api]
    pub(crate) fn parse_schema(&self) -> DeltaResult<StructType> {
        Ok(serde_json::from_str(&self.schema_string)?)
    }

    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn partition_columns(&self) -> &Vec<String> {
        &self.partition_columns
    }

    /// Parse the metadata configuration HashMap<String, String> into a TableProperties struct.
    /// Note that parsing is infallible -- any items that fail to parse are simply propagated
    /// through to the `TableProperties.unknown_properties` field.
    #[internal_api]
    pub(crate) fn parse_table_properties(&self) -> TableProperties {
        TableProperties::from(self.configuration.iter())
    }
}

// TODO: derive IntoEngineData instead (see issue #1083)
impl IntoEngineData for Metadata {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let id = Scalar::from(self.id);
        let name = Scalar::from(self.name);
        let description = Scalar::from(self.description);
        // For format, we need to provide individual scalars for provider and options
        let format_provider = Scalar::from(self.format.provider);
        let format_options = MapData::try_new(
            MapType::new(DataType::STRING, DataType::STRING, false),
            self.format.options,
        )
        .map(Scalar::Map)?;
        let schema_string = Scalar::from(self.schema_string);
        let partition_columns = Scalar::Array(ArrayData::try_new(
            ArrayType::new(DataType::STRING, false),
            self.partition_columns,
        )?);
        let created_time = Scalar::from(self.created_time);
        let configuration = MapData::try_new(
            MapType::new(DataType::STRING, DataType::STRING, false),
            self.configuration,
        )
        .map(Scalar::Map)?;

        let values = [
            id,
            name,
            description,
            format_provider,
            format_options,
            schema_string,
            partition_columns,
            created_time,
            configuration,
        ];

        let evaluator = engine.evaluation_handler();
        evaluator.create_one(schema, &values)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, ToSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[internal_api]
// TODO move to another module so that we disallow constructing this struct without using the
// try_new function.
pub(crate) struct Protocol {
    /// The minimum version of the Delta read protocol that a client must implement
    /// in order to correctly read this table
    min_reader_version: i32,
    /// The minimum version of the Delta write protocol that a client must implement
    /// in order to correctly write this table
    min_writer_version: i32,
    /// A collection of features that a client must implement in order to correctly
    /// read this table (exist only when minReaderVersion is set to 3)
    #[serde(skip_serializing_if = "Option::is_none")]
    reader_features: Option<Vec<ReaderFeature>>,
    /// A collection of features that a client must implement in order to correctly
    /// write this table (exist only when minWriterVersion is set to 7)
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_features: Option<Vec<WriterFeature>>,
}

fn parse_features<T>(features: Option<impl IntoIterator<Item = impl ToString>>) -> Option<Vec<T>>
where
    T: FromStr,
    T::Err: Debug,
{
    features
        .map(|fs| {
            fs.into_iter()
                .map(|f| T::from_str(&f.to_string()))
                .collect()
        })
        .transpose()
        .ok()?
}

impl Protocol {
    /// Try to create a new Protocol instance from reader/writer versions and table features. This
    /// can fail if the protocol is invalid.
    pub(crate) fn try_new(
        min_reader_version: i32,
        min_writer_version: i32,
        reader_features: Option<impl IntoIterator<Item = impl ToString>>,
        writer_features: Option<impl IntoIterator<Item = impl ToString>>,
    ) -> DeltaResult<Self> {
        if min_reader_version == 3 {
            require!(
                reader_features.is_some(),
                Error::invalid_protocol(
                    "Reader features must be present when minimum reader version = 3"
                )
            );
        }
        if min_writer_version == 7 {
            require!(
                writer_features.is_some(),
                Error::invalid_protocol(
                    "Writer features must be present when minimum writer version = 7"
                )
            );
        }

        let reader_features = parse_features(reader_features);
        let writer_features = parse_features(writer_features);

        Ok(Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        })
    }

    /// Create a new Protocol by visiting the EngineData and extracting the first protocol row into
    /// a Protocol instance. If no protocol row is found, returns Ok(None).
    pub(crate) fn try_new_from_data(data: &dyn EngineData) -> DeltaResult<Option<Protocol>> {
        let mut visitor = ProtocolVisitor::default();
        visitor.visit_rows_of(data)?;
        Ok(visitor.protocol)
    }

    /// This protocol's minimum reader version
    #[internal_api]
    pub(crate) fn min_reader_version(&self) -> i32 {
        self.min_reader_version
    }

    /// This protocol's minimum writer version
    #[internal_api]
    pub(crate) fn min_writer_version(&self) -> i32 {
        self.min_writer_version
    }

    /// Get the reader features for the protocol
    #[internal_api]
    pub(crate) fn reader_features(&self) -> Option<&[ReaderFeature]> {
        self.reader_features.as_deref()
    }

    /// Get the writer features for the protocol
    #[internal_api]
    pub(crate) fn writer_features(&self) -> Option<&[WriterFeature]> {
        self.writer_features.as_deref()
    }

    /// True if this protocol has the requested reader feature
    pub(crate) fn has_reader_feature(&self, feature: &ReaderFeature) -> bool {
        self.reader_features()
            .is_some_and(|features| features.contains(feature))
    }

    /// True if this protocol has the requested writer feature
    pub(crate) fn has_writer_feature(&self, feature: &WriterFeature) -> bool {
        self.writer_features()
            .is_some_and(|features| features.contains(feature))
    }

    /// Check if reading a table with this protocol is supported. That is: does the kernel support
    /// the specified protocol reader version and all enabled reader features? If yes, returns unit
    /// type, otherwise will return an error.
    pub(crate) fn ensure_read_supported(&self) -> DeltaResult<()> {
        match &self.reader_features {
            // if min_reader_version = 3 and all reader features are subset of supported => OK
            Some(reader_features) if self.min_reader_version == 3 => {
                ensure_supported_features(reader_features, &SUPPORTED_READER_FEATURES)
            }
            // if min_reader_version = 3 and no reader features => ERROR
            // NOTE this is caught by the protocol parsing.
            None if self.min_reader_version == 3 => Err(Error::internal_error(
                "Reader features must be present when minimum reader version = 3",
            )),
            // if min_reader_version = 1,2 and there are no reader features => OK
            None if self.min_reader_version == 1 || self.min_reader_version == 2 => Ok(()),
            // if min_reader_version = 1,2 and there are reader features => ERROR
            // NOTE this is caught by the protocol parsing.
            Some(_) if self.min_reader_version == 1 || self.min_reader_version == 2 => {
                Err(Error::internal_error(
                    "Reader features must not be present when minimum reader version = 1 or 2",
                ))
            }
            // any other min_reader_version is not supported
            _ => Err(Error::Unsupported(format!(
                "Unsupported minimum reader version {}",
                self.min_reader_version
            ))),
        }
    }

    /// Check if writing to a table with this protocol is supported. That is: does the kernel
    /// support the specified protocol writer version and all enabled writer features?
    pub(crate) fn ensure_write_supported(&self) -> DeltaResult<()> {
        match &self.writer_features {
            Some(writer_features) if self.min_writer_version == 7 => {
                // if we're on version 7, make sure we support all the specified features
                ensure_supported_features(writer_features, &SUPPORTED_WRITER_FEATURES)
            }
            Some(_) => {
                // there are features, but we're not on 7, so the protocol is actually broken
                Err(Error::unsupported(
                    "Tables with min writer version != 7 should not have table features.",
                ))
            }
            None => {
                // no features, we currently only support version 1 or 2 in this case
                require!(
                    self.min_writer_version == 1 || self.min_writer_version == 2,
                    Error::unsupported(
                        "Currently delta-kernel-rs can only write to tables with protocol.minWriterVersion = 1, 2, or 7"
                    )
                );
                Ok(())
            }
        }
    }
}

// TODO: implement Scalar::From<HashMap<K, V>> so we can derive IntoEngineData using a macro (issue#1083)
impl IntoEngineData for Protocol {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>> {
        fn features_to_scalar<T>(
            features: Option<impl IntoIterator<Item = T>>,
        ) -> DeltaResult<Scalar>
        where
            T: Into<Scalar>,
        {
            match features {
                Some(features) => {
                    let features: Vec<Scalar> = features.into_iter().map(Into::into).collect();
                    Ok(Scalar::Array(ArrayData::try_new(
                        ArrayType::new(DataType::STRING, false),
                        features,
                    )?))
                }
                None => Ok(Scalar::Null(DataType::Array(Box::new(ArrayType::new(
                    DataType::STRING,
                    false,
                ))))),
            }
        }

        let min_reader_version = Scalar::from(self.min_reader_version);
        let min_writer_version = Scalar::from(self.min_writer_version);

        let reader_features = features_to_scalar(self.reader_features)?;
        let writer_features = features_to_scalar(self.writer_features)?;

        let values = [
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        ];

        let evaluator = engine.evaluation_handler();
        evaluator.create_one(schema, &values)
    }
}

// given `table_features`, check if they are subset of `supported_features`
pub(crate) fn ensure_supported_features<T>(
    table_features: &[T],
    supported_features: &[T],
) -> DeltaResult<()>
where
    T: Display + FromStr + Hash + Eq,
    <T as FromStr>::Err: Display,
{
    // first check if all features are supported, else we proceed to craft an error message
    if table_features
        .iter()
        .all(|feature| supported_features.contains(feature))
    {
        return Ok(());
    }

    // we get the type name (ReaderFeature/WriterFeature) for better error messages
    let features_type = std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("table feature");

    // NB: we didn't do this above to avoid allocation in the common case
    let mut unsupported = table_features
        .iter()
        .filter(|feature| !supported_features.contains(*feature));

    Err(Error::Unsupported(format!(
        "Unknown {}s: \"{}\". Supported {}s: \"{}\"",
        features_type,
        unsupported.join("\", \""),
        features_type,
        supported_features.iter().join("\", \""),
    )))
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[internal_api]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub(crate) struct CommitInfo {
    /// The time this logical file was created, as milliseconds since the epoch.
    /// Read: optional, write: required (that is, kernel always writes).
    pub(crate) timestamp: Option<i64>,
    /// The time this logical file was created, as milliseconds since the epoch. Unlike
    /// `timestamp`, this field is guaranteed to be monotonically increase with each commit.
    /// Note: If in-commit timestamps are enabled, both the following must be true:
    /// - The `inCommitTimestamp` field must always be present in CommitInfo.
    /// - The CommitInfo action must always be the first one in a commit.
    pub(crate) in_commit_timestamp: Option<i64>,
    /// An arbitrary string that identifies the operation associated with this commit. This is
    /// specified by the engine. Read: optional, write: required (that is, kernel alwarys writes).
    pub(crate) operation: Option<String>,
    /// Map of arbitrary string key-value pairs that provide additional information about the
    /// operation. This is specified by the engine. For now this is always empty on write.
    pub(crate) operation_parameters: Option<HashMap<String, String>>,
    /// The version of the delta_kernel crate used to write this commit. The kernel will always
    /// write this field, but it is optional since many tables will not have this field (i.e. any
    /// tables not written by kernel).
    pub(crate) kernel_version: Option<String>,
    /// A place for the engine to store additional metadata associated with this commit
    pub(crate) engine_info: Option<String>,
}

impl CommitInfo {
    pub(crate) fn new(
        timestamp: i64,
        operation: Option<String>,
        engine_info: Option<String>,
    ) -> Self {
        Self {
            timestamp: Some(timestamp),
            in_commit_timestamp: None,
            operation: Some(operation.unwrap_or_else(|| UNKNOWN_OPERATION.to_string())),
            operation_parameters: None,
            kernel_version: Some(format!("v{KERNEL_VERSION}")),
            engine_info,
        }
    }
}

// TODO: implement Scalar::From<HashMap<K, V>> so we can derive IntoEngineData using a macro (issue#1083)
impl IntoEngineData for CommitInfo {
    fn into_engine_data(
        self,
        schema: SchemaRef,
        engine: &dyn Engine,
    ) -> DeltaResult<Box<dyn EngineData>> {
        let timestamp = Scalar::from(self.timestamp);
        let in_commit_timestamp = Scalar::from(self.in_commit_timestamp);
        let operation = Scalar::from(self.operation);

        let operation_parameters = MapData::try_new(
            MapType::new(DataType::STRING, DataType::STRING, false),
            self.operation_parameters.unwrap_or_default(),
        )
        .map(Scalar::Map)?;

        let kernel_version = Scalar::from(self.kernel_version);
        let engine_info = Scalar::from(self.engine_info);

        let values = [
            timestamp,
            in_commit_timestamp,
            operation,
            operation_parameters,
            kernel_version,
            engine_info,
        ];

        let evaluator = engine.evaluation_handler();
        evaluator.create_one(schema, &values)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
#[internal_api]
pub(crate) struct Add {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// A map from partition column to value for this logical file. This map can contain null in the
    /// values meaning a partition is null. We drop those values from this map, due to the
    /// `allow_null_container_values` annotation allowing them and because [`materialize`] drops
    /// null values. This means an engine can assume that if a partition is found in
    /// [`Metadata::partition_columns`] but not in this map, its value is null.
    ///
    /// [`materialize`]: crate::engine_data::EngineMap::materialize
    #[allow_null_container_values]
    pub(crate) partition_values: HashMap<String, String>,

    /// The size of this data file in bytes
    pub(crate) size: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub(crate) modification_time: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

    /// Contains [statistics] (e.g., count, min/max values for columns) about the data in this logical file encoded as a JSON string.
    ///
    /// [statistics]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Per-file-Statistics
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub stats: Option<String>,

    /// Map containing metadata about this logical file.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub default_row_commit_version: Option<i64>,

    /// The name of the clustering implementation
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub clustering_provider: Option<String>,
}

impl Add {
    #[internal_api]
    #[allow(dead_code)]
    pub(crate) fn dv_unique_id(&self) -> Option<String> {
        self.deletion_vector.as_ref().map(|dv| dv.unique_id())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[internal_api]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub(crate) struct Remove {
    /// A relative path to a data file from the root of the table or an absolute path to a file
    /// that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the data file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub(crate) path: String,

    /// The time this logical file was created, as milliseconds since the epoch.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) deletion_timestamp: Option<i64>,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    pub(crate) data_change: bool,

    /// When true the fields `partition_values`, `size`, and `tags` are present
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) extended_file_metadata: Option<bool>,

    /// A map from partition column to value for this logical file.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) partition_values: Option<HashMap<String, String>>,

    /// The size of this data file in bytes
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) size: Option<i64>,

    /// Map containing metadata about this logical file.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) tags: Option<HashMap<String, String>>,

    /// Information about deletion vector (DV) associated with this add action
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) deletion_vector: Option<DeletionVectorDescriptor>,

    /// Default generated Row ID of the first row in the file. The default generated Row IDs
    /// of the other rows in the file can be reconstructed by adding the physical index of the
    /// row within the file to the base Row ID
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) base_row_id: Option<i64>,

    /// First commit version in which an add action with the same path was committed to the table.
    #[cfg_attr(test, serde(skip_serializing_if = "Option::is_none"))]
    pub(crate) default_row_commit_version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[internal_api]
#[cfg_attr(test, derive(Serialize, Default), serde(rename_all = "camelCase"))]
pub(crate) struct Cdc {
    /// A relative path to a change data file from the root of the table or an absolute path to a
    /// change data file that should be added to the table. The path is a URI as specified by
    /// [RFC 2396 URI Generic Syntax], which needs to be decoded to get the file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// A map from partition column to value for this logical file. This map can contain null in the
    /// values meaning a partition is null. We drop those values from this map, due to the
    /// `allow_null_container_values` annotation allowing them and because [`materialize`] drops
    /// null values. This means an engine can assume that if a partition is found in
    /// [`Metadata::partition_columns`] but not in this map, its value is null.
    ///
    /// [`materialize`]: crate::engine_data::EngineMap::materialize
    #[allow_null_container_values]
    pub partition_values: HashMap<String, String>,

    /// The size of this cdc file in bytes
    pub size: i64,

    /// When `false` the logical file must already be present in the table or the records
    /// in the added file must be contained in one or more remove actions in the same version.
    ///
    /// Should always be set to false for `cdc` actions because they *do not* change the underlying
    /// data of the table
    pub data_change: bool,

    /// Map containing metadata about this logical file.
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, IntoEngineData)]
#[internal_api]
pub(crate) struct SetTransaction {
    /// A unique identifier for the application performing the transaction.
    pub(crate) app_id: String,

    /// An application-specific numeric identifier for this transaction.
    pub(crate) version: i64,

    /// The time when this transaction action was created in milliseconds since the Unix epoch.
    pub(crate) last_updated: Option<i64>,
}

impl SetTransaction {
    pub(crate) fn new(app_id: String, version: i64, last_updated: Option<i64>) -> Self {
        Self {
            app_id,
            version,
            last_updated,
        }
    }
}

/// The sidecar action references a sidecar file which provides some of the checkpoint's
/// file actions. This action is only allowed in checkpoints following the V2 spec.
///
/// [More info]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#sidecar-file-information
#[derive(ToSchema, Debug, PartialEq)]
#[internal_api]
pub(crate) struct Sidecar {
    /// A path to a sidecar file that can be either:
    /// - A relative path (just the file name) within the `_delta_log/_sidecars` directory.
    /// - An absolute path
    /// The path is a URI as specified by [RFC 2396 URI Generic Syntax], which needs to be decoded
    /// to get the file path.
    ///
    /// [RFC 2396 URI Generic Syntax]: https://www.ietf.org/rfc/rfc2396.txt
    pub path: String,

    /// The size of the sidecar file in bytes.
    pub size_in_bytes: i64,

    /// The time this logical file was created, as milliseconds since the epoch.
    pub modification_time: i64,

    /// A map containing any additional metadata about the logicial file.
    pub tags: Option<HashMap<String, String>>,
}

impl Sidecar {
    /// Convert a Sidecar record to a FileMeta.
    ///
    /// This helper first builds the URL by joining the provided log_root with
    /// the "_sidecars/" folder and the given sidecar path.
    pub(crate) fn to_filemeta(&self, log_root: &Url) -> DeltaResult<FileMeta> {
        Ok(FileMeta {
            location: log_root.join("_sidecars/")?.join(&self.path)?,
            last_modified: self.modification_time,
            size: self.size_in_bytes.try_into().map_err(|_| {
                Error::generic(format!(
                    "Failed to convert sidecar size {} to usize",
                    self.size_in_bytes
                ))
            })?,
        })
    }
}

/// The CheckpointMetadata action describes details about a checkpoint following the V2 specification.
///
/// [More info]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-metadata
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[internal_api]
pub(crate) struct CheckpointMetadata {
    /// The version of the V2 spec checkpoint.
    ///
    /// Currently using `i64` for compatibility with other actions' representations.
    /// Future work will address converting numeric fields to unsigned types (e.g., `u64`) where
    /// semantically appropriate (e.g., for version, size, timestamps, etc.).
    /// See issue #786 for tracking progress.
    pub(crate) version: i64,

    /// Map containing any additional metadata about the V2 spec checkpoint.
    pub(crate) tags: Option<HashMap<String, String>>,
}

/// The [DomainMetadata] action contains a configuration (string) for a named metadata domain. Two
/// overlapping transactions conflict if they both contain a domain metadata action for the same
/// metadata domain.
///
/// Note that the `delta.*` domain is reserved for internal use.
///
/// [DomainMetadata]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
#[derive(Debug, Clone, PartialEq, Eq, ToSchema)]
#[internal_api]
pub(crate) struct DomainMetadata {
    domain: String,
    configuration: String,
    removed: bool,
}

impl DomainMetadata {
    // returns true if the domain metadata is an system-controlled domain (all domains that start
    // with "delta.")
    #[allow(unused)]
    fn is_internal(&self) -> bool {
        self.domain.starts_with(INTERNAL_DOMAIN_PREFIX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array::ListBuilder;
    use crate::arrow::json::ReaderBuilder;
    use crate::{
        arrow::array::{
            Array, Int32Array, Int64Array, MapBuilder, MapFieldNames, StringArray, StringBuilder,
            StructArray,
        },
        arrow::datatypes::{DataType as ArrowDataType, Field, Schema},
        arrow::record_batch::RecordBatch,
        engine::arrow_data::ArrowEngineData,
        engine::arrow_expression::ArrowEvaluationHandler,
        schema::{ArrayType, DataType, MapType, StructField},
        utils::test_utils::assert_result_error_with_message,
        Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
    };
    use serde_json::json;

    // duplicated
    struct ExprEngine(Arc<dyn EvaluationHandler>);

    impl ExprEngine {
        fn new() -> Self {
            ExprEngine(Arc::new(ArrowEvaluationHandler))
        }
    }

    impl Engine for ExprEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.0.clone()
        }

        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            unimplemented!()
        }

        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            unimplemented!()
        }

        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            unimplemented!()
        }
    }

    #[test]
    fn test_metadata_schema() {
        let schema = get_log_schema()
            .project(&[METADATA_NAME])
            .expect("Couldn't get metaData field");

        let expected = Arc::new(StructType::new([StructField::nullable(
            "metaData",
            StructType::new([
                StructField::not_null("id", DataType::STRING),
                StructField::nullable("name", DataType::STRING),
                StructField::nullable("description", DataType::STRING),
                StructField::not_null(
                    "format",
                    StructType::new([
                        StructField::not_null("provider", DataType::STRING),
                        StructField::not_null(
                            "options",
                            MapType::new(DataType::STRING, DataType::STRING, false),
                        ),
                    ]),
                ),
                StructField::not_null("schemaString", DataType::STRING),
                StructField::not_null("partitionColumns", ArrayType::new(DataType::STRING, false)),
                StructField::nullable("createdTime", DataType::LONG),
                StructField::not_null(
                    "configuration",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_add_schema() {
        let schema = get_log_schema()
            .project(&[ADD_NAME])
            .expect("Couldn't get add field");

        let expected = Arc::new(StructType::new([StructField::nullable(
            "add",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::not_null(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                ),
                StructField::not_null("size", DataType::LONG),
                StructField::not_null("modificationTime", DataType::LONG),
                StructField::not_null("dataChange", DataType::BOOLEAN),
                StructField::nullable("stats", DataType::STRING),
                StructField::nullable(
                    "tags",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
                deletion_vector_field(),
                StructField::nullable("baseRowId", DataType::LONG),
                StructField::nullable("defaultRowCommitVersion", DataType::LONG),
                StructField::nullable("clusteringProvider", DataType::STRING),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    fn tags_field() -> StructField {
        StructField::nullable(
            "tags",
            MapType::new(DataType::STRING, DataType::STRING, false),
        )
    }

    fn partition_values_field() -> StructField {
        StructField::nullable(
            "partitionValues",
            MapType::new(DataType::STRING, DataType::STRING, false),
        )
    }

    fn deletion_vector_field() -> StructField {
        StructField::nullable(
            "deletionVector",
            DataType::struct_type([
                StructField::not_null("storageType", DataType::STRING),
                StructField::not_null("pathOrInlineDv", DataType::STRING),
                StructField::nullable("offset", DataType::INTEGER),
                StructField::not_null("sizeInBytes", DataType::INTEGER),
                StructField::not_null("cardinality", DataType::LONG),
            ]),
        )
    }

    #[test]
    fn test_remove_schema() {
        let schema = get_log_schema()
            .project(&[REMOVE_NAME])
            .expect("Couldn't get remove field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "remove",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::nullable("deletionTimestamp", DataType::LONG),
                StructField::not_null("dataChange", DataType::BOOLEAN),
                StructField::nullable("extendedFileMetadata", DataType::BOOLEAN),
                partition_values_field(),
                StructField::nullable("size", DataType::LONG),
                tags_field(),
                deletion_vector_field(),
                StructField::nullable("baseRowId", DataType::LONG),
                StructField::nullable("defaultRowCommitVersion", DataType::LONG),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_cdc_schema() {
        let schema = get_log_schema()
            .project(&[CDC_NAME])
            .expect("Couldn't get cdc field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "cdc",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::not_null(
                    "partitionValues",
                    MapType::new(DataType::STRING, DataType::STRING, true),
                ),
                StructField::not_null("size", DataType::LONG),
                StructField::not_null("dataChange", DataType::BOOLEAN),
                tags_field(),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_sidecar_schema() {
        let schema = get_log_schema()
            .project(&[SIDECAR_NAME])
            .expect("Couldn't get sidecar field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "sidecar",
            StructType::new([
                StructField::not_null("path", DataType::STRING),
                StructField::not_null("sizeInBytes", DataType::LONG),
                StructField::not_null("modificationTime", DataType::LONG),
                tags_field(),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_checkpoint_metadata_schema() {
        let schema = get_log_schema()
            .project(&[CHECKPOINT_METADATA_NAME])
            .expect("Couldn't get checkpointMetadata field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "checkpointMetadata",
            StructType::new([
                StructField::not_null("version", DataType::LONG),
                tags_field(),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_transaction_schema() {
        let schema = get_log_schema()
            .project(&["txn"])
            .expect("Couldn't get transaction field");

        let expected = Arc::new(StructType::new([StructField::nullable(
            "txn",
            StructType::new([
                StructField::not_null("appId", DataType::STRING),
                StructField::not_null("version", DataType::LONG),
                StructField::nullable("lastUpdated", DataType::LONG),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_commit_info_schema() {
        let schema = get_log_schema()
            .project(&["commitInfo"])
            .expect("Couldn't get commitInfo field");

        let expected = Arc::new(StructType::new(vec![StructField::nullable(
            "commitInfo",
            StructType::new(vec![
                StructField::nullable("timestamp", DataType::LONG),
                StructField::nullable("inCommitTimestamp", DataType::LONG),
                StructField::nullable("operation", DataType::STRING),
                StructField::nullable(
                    "operationParameters",
                    MapType::new(DataType::STRING, DataType::STRING, false),
                ),
                StructField::nullable("kernelVersion", DataType::STRING),
                StructField::nullable("engineInfo", DataType::STRING),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_domain_metadata_schema() {
        let schema = get_log_schema()
            .project(&[DOMAIN_METADATA_NAME])
            .expect("Couldn't get domainMetadata field");
        let expected = Arc::new(StructType::new([StructField::nullable(
            "domainMetadata",
            StructType::new([
                StructField::not_null("domain", DataType::STRING),
                StructField::not_null("configuration", DataType::STRING),
                StructField::not_null("removed", DataType::BOOLEAN),
            ]),
        )]));
        assert_eq!(schema, expected);
    }

    #[test]
    fn test_validate_protocol() {
        let invalid_protocols = [
            Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: None,
                writer_features: Some(vec![]),
            },
            Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: Some(vec![]),
                writer_features: None,
            },
            Protocol {
                min_reader_version: 3,
                min_writer_version: 7,
                reader_features: None,
                writer_features: None,
            },
        ];
        for Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        } in invalid_protocols
        {
            assert!(matches!(
                Protocol::try_new(
                    min_reader_version,
                    min_writer_version,
                    reader_features,
                    writer_features
                ),
                Err(Error::InvalidProtocol(_)),
            ));
        }
    }

    #[test]
    fn test_v2_checkpoint_supported() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::V2Checkpoint]),
            Some([ReaderFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());
    }

    #[test]
    fn test_ensure_read_supported() {
        let protocol = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec![]),
            writer_features: Some(vec![]),
        };
        assert!(protocol.ensure_read_supported().is_ok());

        let empty_features: [String; 0] = [];
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::V2Checkpoint]),
            Some(&empty_features),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some(&empty_features),
            Some([WriterFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::V2Checkpoint]),
            Some([WriterFeature::V2Checkpoint]),
        )
        .unwrap();
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol {
            min_reader_version: 1,
            min_writer_version: 7,
            reader_features: None,
            writer_features: None,
        };
        assert!(protocol.ensure_read_supported().is_ok());

        let protocol = Protocol {
            min_reader_version: 2,
            min_writer_version: 7,
            reader_features: None,
            writer_features: None,
        };
        assert!(protocol.ensure_read_supported().is_ok());
    }

    #[test]
    fn test_ensure_write_supported() {
        let protocol = Protocol::try_new(
            3,
            7,
            Some::<Vec<String>>(vec![]),
            Some(vec![
                WriterFeature::AppendOnly,
                WriterFeature::DeletionVectors,
                WriterFeature::Invariants,
            ]),
        )
        .unwrap();
        assert!(protocol.ensure_write_supported().is_ok());

        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::DeletionVectors]),
            Some([WriterFeature::RowTracking]),
        )
        .unwrap();
        assert_result_error_with_message(
            protocol.ensure_write_supported(),
            r#"Unsupported: Unknown WriterFeatures: "rowTracking". Supported WriterFeatures: "appendOnly", "deletionVectors", "invariants", "timestampNtz", "variantType", "variantType-preview", "variantShredding-preview""#,
        );
    }

    #[test]
    fn test_ensure_supported_features() {
        let supported_features = [ReaderFeature::ColumnMapping, ReaderFeature::DeletionVectors];
        let table_features = vec![ReaderFeature::ColumnMapping];
        ensure_supported_features(&table_features, &supported_features).unwrap();

        // test unknown features
        let table_features = vec![ReaderFeature::ColumnMapping, ReaderFeature::unknown("idk")];
        let error = ensure_supported_features(&table_features, &supported_features).unwrap_err();
        match error {
            Error::Unsupported(e) if e ==
                "Unknown ReaderFeatures: \"idk\". Supported ReaderFeatures: \"columnMapping\", \"deletionVectors\""
            => {},
            _ => panic!("Expected unsupported error, got: {error}"),
        }
    }

    #[test]
    fn test_parse_table_feature_never_fails() {
        // parse a non-str
        let features = Some([5]);
        let expected = Some(vec![ReaderFeature::unknown("5")]);
        assert_eq!(parse_features::<ReaderFeature>(features), expected);

        // weird strs
        let features = Some(["", "absurD_)(+13%^⚙️"]);
        let expected = Some(vec![
            ReaderFeature::unknown(""),
            ReaderFeature::unknown("absurD_)(+13%^⚙️"),
        ]);
        assert_eq!(parse_features::<ReaderFeature>(features), expected);
    }

    #[test]
    fn test_into_engine_data() {
        let engine = ExprEngine::new();

        let set_transaction = SetTransaction {
            app_id: "app_id".to_string(),
            version: 0,
            last_updated: None,
        };

        let engine_data =
            set_transaction.into_engine_data(SetTransaction::to_schema().into(), &engine);

        let record_batch: crate::arrow::array::RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("appId", ArrowDataType::Utf8, false),
            Field::new("version", ArrowDataType::Int64, false),
            Field::new("lastUpdated", ArrowDataType::Int64, true),
        ]));

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["app_id"])),
                Arc::new(Int64Array::from(vec![0_i64])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_commit_info_into_engine_data() {
        let engine = ExprEngine::new();

        let commit_info = CommitInfo::new(0, None, None);

        let engine_data = commit_info.into_engine_data(CommitInfo::to_schema().into(), &engine);

        let record_batch: crate::arrow::array::RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let mut map_builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "key_value".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        )
        .with_values_field(Field::new("value".to_string(), ArrowDataType::Utf8, false));
        map_builder.append(true).unwrap();
        let operation_parameters = Arc::new(map_builder.finish());

        let expected = RecordBatch::try_new(
            record_batch.schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(0)])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(StringArray::from(vec![Some("UNKNOWN")])),
                operation_parameters,
                Arc::new(StringArray::from(vec![Some(format!("v{KERNEL_VERSION}"))])),
                Arc::new(StringArray::from(vec![None::<String>])),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_metadata_try_new() {
        let schema = StructType::new([StructField::not_null("id", DataType::INTEGER)]);
        let config = HashMap::from([("key1".to_string(), "value1".to_string())]);

        let metadata = Metadata::try_new(
            Some("test_table".to_string()),
            Some("description".to_string()),
            schema.clone(),
            vec!["year".to_string()],
            1234567890,
            config.clone(),
        )
        .unwrap();

        assert!(!metadata.id.is_empty());
        assert_eq!(metadata.name, Some("test_table".to_string()));
        assert_eq!(
            metadata.schema_string,
            serde_json::to_string(&schema).unwrap()
        );
        assert_eq!(metadata.created_time, Some(1234567890));
        assert_eq!(metadata.configuration, config);
    }

    #[test]
    fn test_metadata_try_new_default() {
        let schema = StructType::new([StructField::not_null("id", DataType::INTEGER)]);
        let metadata = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();

        assert!(!metadata.id.is_empty());
        assert_eq!(metadata.name, None);
        assert_eq!(metadata.description, None);
    }

    #[test]
    fn test_metadata_unique_ids() {
        let schema = StructType::new([StructField::not_null("id", DataType::INTEGER)]);
        let m1 = Metadata::try_new(None, None, schema.clone(), vec![], 0, HashMap::new()).unwrap();
        let m2 = Metadata::try_new(None, None, schema, vec![], 0, HashMap::new()).unwrap();
        assert_ne!(m1.id, m2.id);
    }

    #[test]
    fn test_format_try_from_scalar() {
        let options = HashMap::from([
            ("path".to_string(), "/delta/table".to_string()),
            ("compressionType".to_string(), "snappy".to_string()),
        ]);
        let format = Format {
            provider: "parquet".to_string(),
            options,
        };
        let scalar = Scalar::try_from(format).unwrap();

        let Scalar::Struct(struct_data) = scalar else {
            panic!("Expected struct scalar");
        };
        assert_eq!(struct_data.fields()[0].name(), "provider");
        assert_eq!(struct_data.fields()[1].name(), "options");

        let Scalar::String(provider) = &struct_data.values()[0] else {
            panic!("Expected string provider");
        };
        assert_eq!(provider, "parquet");

        let Scalar::Map(map_data) = &struct_data.values()[1] else {
            panic!("Expected map options");
        };
        assert_eq!(map_data.pairs().len(), 2);
    }

    #[test]
    fn test_format_default() {
        let format = Format::default();
        let expected = Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        };
        assert_eq!(format, expected);
    }

    #[test]
    fn test_format_empty_options() {
        let format = Format {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        };
        let scalar = Scalar::try_from(format).unwrap();

        let Scalar::Struct(struct_data) = scalar else {
            panic!("Expected struct");
        };
        let Scalar::Map(map_data) = &struct_data.values()[1] else {
            panic!("Expected map");
        };
        assert!(map_data.pairs().is_empty());
    }

    #[test]
    fn test_format_special_characters() {
        let options = HashMap::from([
            ("path".to_string(), "/path/with spaces".to_string()),
            ("unicode".to_string(), "测试🎉".to_string()),
            ("empty".to_string(), "".to_string()),
        ]);
        let format = Format {
            provider: "custom".to_string(),
            options,
        };
        let scalar = Scalar::try_from(format).unwrap();

        let Scalar::Struct(struct_data) = scalar else {
            panic!("Expected struct");
        };
        let Scalar::Map(map_data) = &struct_data.values()[1] else {
            panic!("Expected map");
        };
        assert_eq!(map_data.pairs().len(), 3);
    }

    #[test]
    fn test_metadata_into_engine_data() {
        let engine = ExprEngine::new();
        let schema = StructType::new([StructField::not_null("id", DataType::INTEGER)]);

        let test_metadata = Metadata::try_new(
            Some("test".to_string()),
            Some("my table".to_string()),
            schema.clone(),
            vec!["part".to_string()],
            123,
            HashMap::from([("k".to_string(), "v".to_string())]),
        )
        .unwrap();

        // have to get the id since it's random
        let test_id = test_metadata.id.clone();

        let actual: RecordBatch = test_metadata
            .into_engine_data(Metadata::to_schema().into(), &engine)
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let expected_json = json!({
            "id": test_id,
            "name": "test",
            "description": "my table",
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}",
            "partitionColumns": ["part"],
            "createdTime": 123,
            "configuration": {
                "k": "v"
            }
        }).to_string();
        let expected = ReaderBuilder::new(actual.schema())
            .build(expected_json.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_metadata_with_log_schema() {
        let engine = ExprEngine::new();
        let schema = StructType::new([StructField::not_null("id", DataType::INTEGER)]);

        let metadata = Metadata::try_new(
            Some("table".to_string()),
            None, // test that omitting description will omit entire field
            schema,
            vec![],
            456,
            HashMap::new(),
        )
        .unwrap();

        let metadata_id = metadata.id.clone();

        // test with the full log schema that wraps metadata in a "metaData" field
        let log_schema = get_log_schema().project(&[METADATA_NAME]).unwrap();
        let actual: RecordBatch = metadata
            .into_engine_data(log_schema, &engine)
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let expected_json = json!({
            "metaData": {
                "id": metadata_id,
                "name": "table",
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}",
                "partitionColumns": [],
                "createdTime": 456,
                "configuration": {}
            }
        }).to_string();
        let expected = ReaderBuilder::new(actual.schema())
            .build(expected_json.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_protocol_into_engine_data() {
        let engine = ExprEngine::new();
        let protocol = Protocol::try_new(
            3,
            7,
            Some([ReaderFeature::ColumnMapping]),
            Some([WriterFeature::DeletionVectors]),
        )
        .unwrap();

        let engine_data = protocol
            .clone()
            .into_engine_data(Protocol::to_schema().into(), &engine);
        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        let list_field = Arc::new(Field::new("element", ArrowDataType::Utf8, false));
        let protocol_fields = vec![
            Field::new("minReaderVersion", ArrowDataType::Int32, false),
            Field::new("minWriterVersion", ArrowDataType::Int32, false),
            Field::new(
                "readerFeatures",
                ArrowDataType::List(list_field.clone()),
                true, // nullable
            ),
            Field::new(
                "writerFeatures",
                ArrowDataType::List(list_field.clone()),
                true, // nullable
            ),
        ];
        let schema = Arc::new(Schema::new(protocol_fields.clone()));

        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(list_field.clone());
        list_builder.values().append_value("columnMapping");
        list_builder.append(true);
        let reader_features_array = list_builder.finish();

        let string_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(string_builder).with_field(list_field.clone());
        list_builder.values().append_value("deletionVectors");
        list_builder.append(true);
        let writer_features_array = list_builder.finish();

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![7])),
                Arc::new(reader_features_array.clone()),
                Arc::new(writer_features_array.clone()),
            ],
        )
        .unwrap();

        assert_eq!(record_batch, expected);

        // test with the full log schema that wraps protocol in a "protocol" field
        let log_schema = get_log_schema().project(&[PROTOCOL_NAME]).unwrap();
        let engine_data = protocol.into_engine_data(log_schema, &engine);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "protocol",
            ArrowDataType::Struct(protocol_fields.into()),
            true,
        )]));

        let expected = RecordBatch::try_new(
            schema,
            vec![Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("minReaderVersion", ArrowDataType::Int32, false)),
                    Arc::new(Int32Array::from(vec![3])) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new("minWriterVersion", ArrowDataType::Int32, false)),
                    Arc::new(Int32Array::from(vec![7])) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new(
                        "readerFeatures",
                        ArrowDataType::List(list_field.clone()),
                        true,
                    )),
                    Arc::new(reader_features_array) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new(
                        "writerFeatures",
                        ArrowDataType::List(list_field),
                        true,
                    )),
                    Arc::new(writer_features_array) as Arc<dyn Array>,
                ),
            ]))],
        )
        .unwrap();

        let record_batch: RecordBatch = engine_data
            .unwrap()
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        assert_eq!(record_batch, expected);
    }

    #[test]
    fn test_protocol_into_engine_data_empty_features() {
        let engine = ExprEngine::new();
        let empty_features: Vec<String> = vec![];
        let protocol =
            Protocol::try_new(3, 7, Some(empty_features.clone()), Some(empty_features)).unwrap();

        let engine_data = protocol
            .into_engine_data(Protocol::to_schema().into(), &engine)
            .unwrap();
        let record_batch: RecordBatch = engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 4);

        // reader/writer features are Some([]) lists
        let reader_features_col = record_batch
            .column(2)
            .as_any()
            .downcast_ref::<crate::arrow::array::ListArray>()
            .unwrap();
        assert_eq!(reader_features_col.len(), 1);
        assert_eq!(reader_features_col.value(0).len(), 0); // empty list
        let writer_features_col = record_batch
            .column(3)
            .as_any()
            .downcast_ref::<crate::arrow::array::ListArray>()
            .unwrap();
        assert_eq!(writer_features_col.len(), 1);
        assert_eq!(writer_features_col.value(0).len(), 0); // empty list
    }

    #[test]
    fn test_protocol_into_engine_data_no_features() {
        let engine = ExprEngine::new();
        let protocol = Protocol::try_new(1, 2, None::<Vec<String>>, None::<Vec<String>>).unwrap();

        let engine_data = protocol
            .into_engine_data(Protocol::to_schema().into(), &engine)
            .unwrap();
        let record_batch: RecordBatch = engine_data
            .into_any()
            .downcast::<ArrowEngineData>()
            .unwrap()
            .into();

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 4);

        // reader/writer features are null
        assert!(record_batch.column(2).is_null(0));
        assert!(record_batch.column(3).is_null(0));
    }
}
