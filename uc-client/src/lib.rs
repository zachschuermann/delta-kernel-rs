//! Stub UC client: dumb rust API for talking to UC

use std::collections::HashMap;
use std::path::PathBuf;

use delta_kernel::actions::{Format, Metadata, Protocol};
use delta_kernel::table_features::{ReaderFeature, WriterFeature};
use delta_kernel::{LogFile, Version};

use url::Url;

#[derive(Debug, Default, Clone)]
pub struct UCClient {
    // tables: Vec<TableMetadata>,
}

pub struct TableMetadata {
    pub name: String,
    pub table_root: Url,
    pub log_tail: Vec<LogFile>,
    pub protocol: Protocol,
    pub metadata: Metadata,
    pub latest_version: Version,
}

impl UCClient {
    pub fn get_table(&self, name: &str) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let path = std::fs::canonicalize(PathBuf::from(
            "../kernel/tests/data/parquet_row_group_skipping",
        ))?;
        let url = Url::from_directory_path(path).unwrap();
        let protocol = Protocol::try_new(
            3,
            7,
            Some(vec![ReaderFeature::TimestampWithoutTimezone]),
            Some(vec![WriterFeature::TimestampWithoutTimezone]),
        )
        .unwrap();
        let metadata = Metadata::new(
        "1234".to_string(),
        None,
        None,
        Format::default(),
        r#"{"type":"struct","fields":[{"name":"bool","type":"boolean","nullable":true,"metadata":{}},{"name":"chrono","type":{"type":"struct","fields":[{"name":"date32","type":"date","nullable":true,"metadata":{}},{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}},{"name":"timestamp_ntz","type":"timestamp_ntz","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"numeric","type":{"type":"struct","fields":[{"name":"decimals","type":{"type":"struct","fields":[{"name":"decimal128","type":"decimal(32,3)","nullable":true,"metadata":{}},{"name":"decimal32","type":"decimal(8,3)","nullable":true,"metadata":{}},{"name":"decimal64","type":"decimal(16,3)","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"floats","type":{"type":"struct","fields":[{"name":"float32","type":"float","nullable":true,"metadata":{}},{"name":"float64","type":"double","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"ints","type":{"type":"struct","fields":[{"name":"int16","type":"short","nullable":true,"metadata":{}},{"name":"int32","type":"integer","nullable":true,"metadata":{}},{"name":"int64","type":"long","nullable":true,"metadata":{}},{"name":"int8","type":"byte","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"varlen","type":{"type":"struct","fields":[{"name":"binary","type":"binary","nullable":true,"metadata":{}},{"name":"utf8","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"missing","type":"long","nullable":true,"metadata":{}}]}"#.to_string(),
        vec![],
        Some(1728065840373),
        HashMap::new());

        Ok(TableMetadata {
            name: name.to_string(),
            table_root: url,
            log_tail: vec![],
            protocol,
            metadata,
            latest_version: 1,
        })
    }

    pub fn commit(&self, actions: Vec<String>) -> Result<(), String> {
        todo!()
    }

    // pub fn create_table(&mut self, name: &str, table_root: Url) -> Result<(), String> {
    //     // if self.tables.iter().any(|t| t.name == name) {
    //     //     return Err(format!("Table {name} already exists"));
    //     // }
    //     let metadata = TableMetadata {
    //         name: name.to_string(),
    //         table_root,
    //         // protocol: Protocol::default(),
    //         // log: Log::default(),
    //     };
    //     // self.tables.push(metadata);
    //     Ok(())
    // }
}
