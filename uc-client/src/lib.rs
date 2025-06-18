//! Stub UC client: dumb rust API for talking to UC

use url::Url;

#[derive(Debug, Default, Clone)]
pub struct UCClient {
    tables: Vec<TableMetadata>,
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub table_root: Url,
    // pub protocol: Protocol,
    // pub log: WhatType?
}

impl UCClient {
    pub fn get_table(&self, name: &str) -> TableMetadata {
        TableMetadata {
            name: name.to_string(),
            table_root: Url::parse("FIXME").unwrap(),
        }
    }

    pub fn commit(&self, actions: Vec<String>) -> Result<(), String> {
        todo!()
    }
}
