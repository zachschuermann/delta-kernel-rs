use std::sync::LazyLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumCount, EnumString};

use crate::expressions::Scalar;
use crate::schema::derive_macro_utils::ToDataType;
use crate::schema::DataType;
use delta_kernel_derive::internal_api;

pub(crate) use column_mapping::column_mapping_mode;
pub use column_mapping::{validate_schema_column_mapping, ColumnMappingMode};
pub(crate) use timestamp_ntz::validate_timestamp_ntz_feature_support;
mod column_mapping;
mod timestamp_ntz;

/// Reader features communicate capabilities that must be implemented in order to correctly read a
/// given table. That is, readers must implement and respect all features listed in a table's
/// `ReaderFeatures`. Note that any feature listed as a `ReaderFeature` must also have a
/// corresponding `WriterFeature`.
///
/// The kernel currently supports all reader features except for V2Checkpoints.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    EnumString,
    StrumDisplay,
    AsRefStr,
    EnumCount,
    Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) enum ReaderFeature {
    /// CatalogManaged tables: https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md
    CatalogManaged,
    #[strum(serialize = "catalogOwned-preview")]
    #[serde(rename = "catalogOwned-preview")]
    CatalogOwnedPreview,
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// timestamps without timezone support
    #[strum(serialize = "timestampNtz")]
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // Allow columns to change type
    TypeWidening,
    #[strum(serialize = "typeWidening-preview")]
    #[serde(rename = "typeWidening-preview")]
    TypeWideningPreview,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// vacuumProtocolCheck ReaderWriter feature ensures consistent application of reader and writer
    /// protocol checks during VACUUM operations
    VacuumProtocolCheck,
    /// This feature enables support for the variant data type, which stores semi-structured data.
    VariantType,
    #[strum(serialize = "variantType-preview")]
    #[serde(rename = "variantType-preview")]
    VariantTypePreview,
    #[strum(serialize = "variantShredding-preview")]
    #[serde(rename = "variantShredding-preview")]
    VariantShreddingPreview,
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

/// Similar to reader features, writer features communicate capabilities that must be implemented
/// in order to correctly write to a given table. That is, writers must implement and respect all
/// features listed in a table's `WriterFeatures`.
///
/// Kernel write support is currently in progress and as such these are not supported.
#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    EnumString,
    StrumDisplay,
    AsRefStr,
    EnumCount,
    Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
#[internal_api]
pub(crate) enum WriterFeature {
    /// CatalogManaged tables: https://github.com/delta-io/delta/blob/master/protocol_rfcs/catalog-managed.md
    CatalogManaged,
    #[strum(serialize = "catalogOwned-preview")]
    #[serde(rename = "catalogOwned-preview")]
    CatalogOwnedPreview,
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// Mapping of one column to another
    ColumnMapping,
    /// ID Columns
    IdentityColumns,
    /// Monotonically increasing timestamps in the CommitInfo
    InCommitTimestamp,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    /// Row tracking on tables
    RowTracking,
    /// timestamps without timezone support
    #[strum(serialize = "timestampNtz")]
    #[serde(rename = "timestampNtz")]
    TimestampWithoutTimezone,
    // Allow columns to change type
    TypeWidening,
    #[strum(serialize = "typeWidening-preview")]
    #[serde(rename = "typeWidening-preview")]
    TypeWideningPreview,
    /// domain specific metadata
    DomainMetadata,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// Iceberg compatibility support
    IcebergCompatV2,
    /// vacuumProtocolCheck ReaderWriter feature ensures consistent application of reader and writer
    /// protocol checks during VACUUM operations
    VacuumProtocolCheck,
    /// The Clustered Table feature facilitates the physical clustering of rows
    /// that share similar values on a predefined set of clustering columns.
    #[strum(serialize = "clustering")]
    #[serde(rename = "clustering")]
    ClusteredTable,
    /// This feature enables support for the variant data type, which stores semi-structured data.
    VariantType,
    #[strum(serialize = "variantType-preview")]
    #[serde(rename = "variantType-preview")]
    VariantTypePreview,
    #[strum(serialize = "variantShredding-preview")]
    #[serde(rename = "variantShredding-preview")]
    VariantShreddingPreview,
    #[serde(untagged)]
    #[strum(default)]
    Unknown(String),
}

impl ToDataType for ReaderFeature {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl ToDataType for WriterFeature {
    fn to_data_type() -> DataType {
        DataType::STRING
    }
}

impl From<ReaderFeature> for Scalar {
    fn from(feature: ReaderFeature) -> Self {
        Scalar::String(feature.to_string())
    }
}

impl From<WriterFeature> for Scalar {
    fn from(feature: WriterFeature) -> Self {
        Scalar::String(feature.to_string())
    }
}

#[cfg(test)] // currently only used in tests
impl ReaderFeature {
    pub(crate) fn unknown(s: impl ToString) -> Self {
        ReaderFeature::Unknown(s.to_string())
    }
}

#[cfg(test)] // currently only used in tests
impl WriterFeature {
    pub(crate) fn unknown(s: impl ToString) -> Self {
        WriterFeature::Unknown(s.to_string())
    }
}

pub(crate) static SUPPORTED_READER_FEATURES: LazyLock<Vec<ReaderFeature>> = LazyLock::new(|| {
    vec![
        #[cfg(feature = "internal-api")]
        ReaderFeature::CatalogManaged,
        #[cfg(feature = "internal-api")]
        ReaderFeature::CatalogOwnedPreview,
        ReaderFeature::ColumnMapping,
        ReaderFeature::DeletionVectors,
        ReaderFeature::TimestampWithoutTimezone,
        ReaderFeature::TypeWidening,
        ReaderFeature::TypeWideningPreview,
        ReaderFeature::VacuumProtocolCheck,
        ReaderFeature::V2Checkpoint,
        ReaderFeature::VariantType,
        ReaderFeature::VariantTypePreview,
        // The default engine currently DOES NOT support shredded Variant reads and the parquet
        // reader will reject the read if it sees a shredded schema in the parquet file. That being
        // said, kernel does permit reconstructing shredded variants into the
        // `STRUCT<metadata: BINARY, value: BINARY>` representation if parquet readers of
        // third-party engines support it.
        ReaderFeature::VariantShreddingPreview,
    ]
});

// note: we 'support' Invariants, but only insofar as we check that they are not present.
// we support writing to tables that have Invariants enabled but not used. similarly, we only
// support DeletionVectors in that we never write them (no DML).
pub(crate) static SUPPORTED_WRITER_FEATURES: LazyLock<Vec<WriterFeature>> = LazyLock::new(|| {
    vec![
        WriterFeature::AppendOnly,
        #[cfg(feature = "internal-api")]
        WriterFeature::CatalogManaged,
        #[cfg(feature = "internal-api")]
        WriterFeature::CatalogOwnedPreview,
        WriterFeature::DeletionVectors,
        WriterFeature::Invariants,
        WriterFeature::TimestampWithoutTimezone,
        WriterFeature::VariantType,
        WriterFeature::VariantTypePreview,
        WriterFeature::VariantShreddingPreview,
    ]
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_features() {
        let mixed_reader = &[
            ReaderFeature::DeletionVectors,
            ReaderFeature::unknown("cool_feature"),
            ReaderFeature::ColumnMapping,
        ];
        let mixed_writer = &[
            WriterFeature::DeletionVectors,
            WriterFeature::unknown("cool_feature"),
            WriterFeature::AppendOnly,
        ];

        let reader_string = serde_json::to_string(mixed_reader).unwrap();
        let writer_string = serde_json::to_string(mixed_writer).unwrap();

        assert_eq!(
            &reader_string,
            "[\"deletionVectors\",\"cool_feature\",\"columnMapping\"]"
        );
        assert_eq!(
            &writer_string,
            "[\"deletionVectors\",\"cool_feature\",\"appendOnly\"]"
        );

        let typed_reader: Vec<ReaderFeature> = serde_json::from_str(&reader_string).unwrap();
        let typed_writer: Vec<WriterFeature> = serde_json::from_str(&writer_string).unwrap();

        assert_eq!(typed_reader.len(), 3);
        assert_eq!(&typed_reader, mixed_reader);
        assert_eq!(typed_writer.len(), 3);
        assert_eq!(&typed_writer, mixed_writer);
    }

    #[test]
    fn test_roundtrip_reader_features() {
        let cases = [
            (ReaderFeature::CatalogManaged, "catalogManaged"),
            (ReaderFeature::ColumnMapping, "columnMapping"),
            (ReaderFeature::DeletionVectors, "deletionVectors"),
            (ReaderFeature::TimestampWithoutTimezone, "timestampNtz"),
            (ReaderFeature::TypeWidening, "typeWidening"),
            (ReaderFeature::TypeWideningPreview, "typeWidening-preview"),
            (ReaderFeature::V2Checkpoint, "v2Checkpoint"),
            (ReaderFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (ReaderFeature::VariantType, "variantType"),
            (ReaderFeature::VariantTypePreview, "variantType-preview"),
            (
                ReaderFeature::VariantShreddingPreview,
                "variantShredding-preview",
            ),
            (ReaderFeature::unknown("something"), "something"),
        ];

        assert_eq!(ReaderFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: ReaderFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: ReaderFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }

    #[test]
    fn test_roundtrip_writer_features() {
        let cases = [
            (WriterFeature::AppendOnly, "appendOnly"),
            (WriterFeature::CatalogManaged, "catalogManaged"),
            (WriterFeature::Invariants, "invariants"),
            (WriterFeature::CheckConstraints, "checkConstraints"),
            (WriterFeature::ChangeDataFeed, "changeDataFeed"),
            (WriterFeature::GeneratedColumns, "generatedColumns"),
            (WriterFeature::ColumnMapping, "columnMapping"),
            (WriterFeature::IdentityColumns, "identityColumns"),
            (WriterFeature::InCommitTimestamp, "inCommitTimestamp"),
            (WriterFeature::DeletionVectors, "deletionVectors"),
            (WriterFeature::RowTracking, "rowTracking"),
            (WriterFeature::TimestampWithoutTimezone, "timestampNtz"),
            (WriterFeature::TypeWidening, "typeWidening"),
            (WriterFeature::TypeWideningPreview, "typeWidening-preview"),
            (WriterFeature::DomainMetadata, "domainMetadata"),
            (WriterFeature::V2Checkpoint, "v2Checkpoint"),
            (WriterFeature::IcebergCompatV1, "icebergCompatV1"),
            (WriterFeature::IcebergCompatV2, "icebergCompatV2"),
            (WriterFeature::VacuumProtocolCheck, "vacuumProtocolCheck"),
            (WriterFeature::ClusteredTable, "clustering"),
            (WriterFeature::VariantType, "variantType"),
            (WriterFeature::VariantTypePreview, "variantType-preview"),
            (
                WriterFeature::VariantShreddingPreview,
                "variantShredding-preview",
            ),
            (WriterFeature::unknown("something"), "something"),
        ];

        assert_eq!(WriterFeature::COUNT, cases.len());

        for (feature, expected) in cases {
            assert_eq!(feature.to_string(), expected);
            let serialized = serde_json::to_string(&feature).unwrap();
            assert_eq!(serialized, format!("\"{expected}\""));

            let deserialized: WriterFeature = serde_json::from_str(&serialized).unwrap();
            assert_eq!(deserialized, feature);

            let from_str: WriterFeature = expected.parse().unwrap();
            assert_eq!(from_str, feature);
        }
    }
}
