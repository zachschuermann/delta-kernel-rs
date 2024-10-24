//! This module defines visitors that can be used to extract the various delta actions from
//! [`EngineData`] types.

use std::collections::HashMap;

use crate::{
    engine_data::{GetData, TypedGetData},
    DataVisitor, DeltaResult,
};

use super::{deletion_vector::DeletionVectorDescriptor, Add, Format, Metadata, Protocol, Remove};

#[derive(Default)]
pub(crate) struct MetadataVisitor {
    pub(crate) metadata: Option<Metadata>,
}

impl MetadataVisitor {
    fn visit_metadata<'a>(
        row_index: usize,
        id: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Metadata> {
        let name: Option<String> = getters[1].get_opt(row_index, "metadata.name")?;
        let description: Option<String> = getters[2].get_opt(row_index, "metadata.description")?;
        // get format out of primitives
        let format_provider: String = getters[3].get(row_index, "metadata.format.provider")?;
        // options for format is always empty, so skip getters[4]
        let schema_string: String = getters[5].get(row_index, "metadata.schema_string")?;
        let partition_columns: Vec<_> = getters[6].get(row_index, "metadata.partition_list")?;
        let created_time: Option<i64> = getters[7].get_opt(row_index, "metadata.created_time")?;
        let configuration_map_opt: Option<HashMap<_, _>> =
            getters[8].get_opt(row_index, "metadata.configuration")?;
        let configuration = configuration_map_opt.unwrap_or_else(HashMap::new);

        Ok(Metadata {
            id,
            name,
            description,
            format: Format {
                provider: format_provider,
                options: HashMap::new(),
            },
            schema_string,
            partition_columns,
            created_time,
            configuration,
        })
    }
}

impl DataVisitor for MetadataVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since id column is required, use it to detect presence of a metadata action
            if let Some(id) = getters[0].get_opt(i, "metadata.id")? {
                self.metadata = Some(Self::visit_metadata(i, id, getters)?);
                break;
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct SelectionVectorVisitor {
    pub(crate) selection_vector: Vec<bool>,
}

impl DataVisitor for SelectionVectorVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            self.selection_vector
                .push(getters[0].get(i, "selectionvector.output")?)
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ProtocolVisitor {
    pub(crate) protocol: Option<Protocol>,
}

impl ProtocolVisitor {
    fn visit_protocol<'a>(
        row_index: usize,
        min_reader_version: i32,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Protocol> {
        let min_writer_version: i32 = getters[1].get(row_index, "protocol.min_writer_version")?;
        let reader_features: Option<Vec<_>> =
            getters[2].get_opt(row_index, "protocol.reader_features")?;
        let writer_features: Option<Vec<_>> =
            getters[3].get_opt(row_index, "protocol.writer_features")?;

        Ok(Protocol {
            min_reader_version,
            min_writer_version,
            reader_features,
            writer_features,
        })
    }
}

impl DataVisitor for ProtocolVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since minReaderVersion column is required, use it to detect presence of a Protocol action
            if let Some(mrv) = getters[0].get_opt(i, "protocol.min_reader_version")? {
                self.protocol = Some(Self::visit_protocol(i, mrv, getters)?);
                break;
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct AddVisitor {
    pub(crate) adds: Vec<Add>,
}

impl AddVisitor {
    pub(crate) fn visit_add<'a>(
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Add> {
        let partition_values: HashMap<_, _> = getters[1].get(row_index, "add.partitionValues")?;
        let size: i64 = getters[2].get(row_index, "add.size")?;
        let modification_time: i64 = getters[3].get(row_index, "add.modificationTime")?;
        let data_change: bool = getters[4].get(row_index, "add.dataChange")?;
        let stats: Option<String> = getters[5].get_opt(row_index, "add.stats")?;

        // TODO(nick) extract tags if we ever need them at getters[6]

        let deletion_vector = if let Some(storage_type) =
            getters[7].get_opt(row_index, "add.deletionVector.storageType")?
        {
            // there is a storageType, so the whole DV must be there
            let path_or_inline_dv: String =
                getters[8].get(row_index, "add.deletionVector.pathOrInlineDv")?;
            let offset: Option<i32> = getters[9].get_opt(row_index, "add.deletionVector.offset")?;
            let size_in_bytes: i32 =
                getters[10].get(row_index, "add.deletionVector.sizeInBytes")?;
            let cardinality: i64 = getters[11].get(row_index, "add.deletionVector.cardinality")?;
            Some(DeletionVectorDescriptor {
                storage_type,
                path_or_inline_dv,
                offset,
                size_in_bytes,
                cardinality,
            })
        } else {
            None
        };

        let base_row_id: Option<i64> = getters[12].get_opt(row_index, "add.base_row_id")?;
        let default_row_commit_version: Option<i64> =
            getters[13].get_opt(row_index, "add.default_row_commit")?;
        let clustering_provider: Option<String> =
            getters[14].get_opt(row_index, "add.clustering_provider")?;

        Ok(Add {
            path,
            partition_values,
            size,
            modification_time,
            data_change,
            stats,
            tags: HashMap::new(),
            deletion_vector,
            base_row_id,
            default_row_commit_version,
            clustering_provider,
        })
    }
}

impl DataVisitor for AddVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of an Add action
            if let Some(path) = getters[0].get_opt(i, "add.path")? {
                self.adds.push(Self::visit_add(i, path, getters)?);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct RemoveVisitor {
    pub(crate) removes: Vec<Remove>,
}

impl RemoveVisitor {
    pub(crate) fn visit_remove<'a>(
        row_index: usize,
        path: String,
        getters: &[&'a dyn GetData<'a>],
    ) -> DeltaResult<Remove> {
        let deletion_timestamp: Option<i64> =
            getters[1].get_opt(row_index, "remove.deletionTimestamp")?;
        let data_change: bool = getters[2].get(row_index, "remove.dataChange")?;
        let extended_file_metadata: Option<bool> =
            getters[3].get_opt(row_index, "remove.extendedFileMetadata")?;

        // TODO(nick) handle partition values in getters[4]

        let size: Option<i64> = getters[5].get_opt(row_index, "remove.size")?;

        // TODO(nick) stats are skipped in getters[6] and tags are skipped in getters[7]

        let deletion_vector = if let Some(storage_type) =
            getters[8].get_opt(row_index, "remove.deletionVector.storageType")?
        {
            // there is a storageType, so the whole DV must be there
            let path_or_inline_dv: String =
                getters[9].get(row_index, "remove.deletionVector.pathOrInlineDv")?;
            let offset: Option<i32> =
                getters[10].get_opt(row_index, "remove.deletionVector.offset")?;
            let size_in_bytes: i32 =
                getters[11].get(row_index, "remove.deletionVector.sizeInBytes")?;
            let cardinality: i64 =
                getters[12].get(row_index, "remove.deletionVector.cardinality")?;
            Some(DeletionVectorDescriptor {
                storage_type,
                path_or_inline_dv,
                offset,
                size_in_bytes,
                cardinality,
            })
        } else {
            None
        };

        let base_row_id: Option<i64> = getters[13].get_opt(row_index, "remove.baseRowId")?;
        let default_row_commit_version: Option<i64> =
            getters[14].get_opt(row_index, "remove.defaultRowCommitVersion")?;

        Ok(Remove {
            path,
            data_change,
            deletion_timestamp,
            extended_file_metadata,
            partition_values: None,
            size,
            tags: None,
            deletion_vector,
            base_row_id,
            default_row_commit_version,
        })
    }
}

impl DataVisitor for RemoveVisitor {
    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for i in 0..row_count {
            // Since path column is required, use it to detect presence of an Remove action
            if let Some(path) = getters[0].get_opt(i, "remove.path")? {
                self.removes.push(Self::visit_remove(i, path, getters)?);
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    use super::*;
    use crate::{
        actions::schemas::log_schema,
        client::arrow_data::ArrowEngineData,
        client::sync::{json::SyncJsonHandler, SyncEngineInterface},
        schema::StructType,
        EngineData, EngineInterface, JsonHandler,
    };

    // TODO(nick): Merge all copies of this into one "test utils" thing
    fn string_array_to_engine_data(string_array: StringArray) -> Box<dyn EngineData> {
        let string_field = Arc::new(Field::new("a", DataType::Utf8, true));
        let schema = Arc::new(ArrowSchema::new(vec![string_field]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(string_array)])
            .expect("Can't convert to record batch");
        Box::new(ArrowEngineData::new(batch))
    }

    fn action_batch() -> Box<ArrowEngineData> {
        let handler = SyncJsonHandler {};
        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(log_schema().clone());
        let parsed = handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        ArrowEngineData::try_from_engine_data(parsed).unwrap()
    }

    #[test]
    fn test_parse_protocol() -> DeltaResult<()> {
        let data = action_batch();
        let parsed = Protocol::try_new_from_data(data.as_ref())?.unwrap();
        let expected = Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        };
        assert_eq!(parsed, expected);
        Ok(())
    }

    #[test]
    fn test_parse_metadata() -> DeltaResult<()> {
        let data = action_batch();
        let parsed = Metadata::try_new_from_data(data.as_ref())?.unwrap();

        let configuration = HashMap::from_iter([
            (
                "delta.enableDeletionVectors".to_string(),
                Some("true".to_string()),
            ),
            (
                "delta.columnMapping.mode".to_string(),
                Some("none".to_string()),
            ),
        ]);
        let expected = Metadata {
            id: "testId".into(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".into(),
                options: Default::default(),
            },
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            partition_columns: Vec::new(),
            created_time: Some(1677811175819),
            configuration,
        };
        assert_eq!(parsed, expected);
        Ok(())
    }

    #[test]
    fn test_parse_add_partitioned() {
        let client = SyncEngineInterface::new();
        let json_handler = client.get_json_handler();
        let json_strings: StringArray = vec![
            r#"{"commitInfo":{"timestamp":1670892998177,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[\"c1\",\"c2\"]"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"1356"},"engineInfo":"Apache-Spark/3.3.1 Delta-Lake/2.2.0","txnId":"046a258f-45e3-4657-b0bf-abfb0f76681c"}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet","partitionValues":{"c1":"5","c2":"b"},"size":452,"modificationTime":1670892998136,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998137,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}"}}"#,
        ]
        .into();
        let output_schema = Arc::new(log_schema().clone());
        let batch = json_handler
            .parse_json(string_array_to_engine_data(json_strings), output_schema)
            .unwrap();
        let add_schema = StructType::new(vec![crate::actions::schemas::ADD_FIELD.clone()]);
        let mut add_visitor = AddVisitor::default();
        batch
            .extract(Arc::new(add_schema), &mut add_visitor)
            .unwrap();
        let add1 = Add {
            path: "c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("c1".to_string(), Some("4".to_string())),
                ("c2".to_string(), Some("c".to_string())),
            ]),
            size: 452,
            modification_time: 1670892998135,
            data_change: true,
            stats: Some("{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}".into()),
            tags: HashMap::new(),
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };
        let add2 = Add {
            path: "c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("c1".to_string(), Some("5".to_string())),
                ("c2".to_string(), Some("b".to_string())),
            ]),
            modification_time: 1670892998136,
            stats: Some("{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}".into()),
            ..add1.clone()
        };
        let add3 = Add {
            path: "c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet".into(),
            partition_values: HashMap::from([
                ("c1".to_string(), Some("6".to_string())),
                ("c2".to_string(), Some("a".to_string())),
            ]),
            modification_time: 1670892998137,
            stats: Some("{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}".into()),
            ..add1.clone()
        };
        let expected = vec![add1, add2, add3];
        for (add, expected) in add_visitor.adds.into_iter().zip(expected.into_iter()) {
            assert_eq!(add, expected);
        }
    }
}
