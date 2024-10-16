//! A simple, single threaded, EngineInterface that can only read from the local filesystem

use super::arrow_expression::ArrowExpressionHandler;
use crate::{EngineInterface, ExpressionHandler, FileSystemClient, JsonHandler, ParquetHandler};

use std::sync::Arc;

mod fs_client;
mod get_data;
pub(crate) mod json;
mod parquet;

/// This is a simple implemention of [`EngineInterface`]. It only supports reading data from the
/// local filesystem, and internally represents data using `Arrow`.
pub struct SyncEngineInterface {
    fs_client: Arc<fs_client::SyncFilesystemClient>,
    json_handler: Arc<json::SyncJsonHandler>,
    parquet_handler: Arc<parquet::SyncParquetHandler>,
    expression_handler: Arc<ArrowExpressionHandler>,
}

impl SyncEngineInterface {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        SyncEngineInterface {
            fs_client: Arc::new(fs_client::SyncFilesystemClient {}),
            json_handler: Arc::new(json::SyncJsonHandler {}),
            parquet_handler: Arc::new(parquet::SyncParquetHandler {}),
            expression_handler: Arc::new(ArrowExpressionHandler {}),
        }
    }
}

impl EngineInterface for SyncEngineInterface {
    fn get_expression_handler(&self) -> Arc<dyn ExpressionHandler> {
        self.expression_handler.clone()
    }

    fn get_file_system_client(&self) -> Arc<dyn FileSystemClient> {
        self.fs_client.clone()
    }

    /// Get the connector provided [`ParquetHandler`].
    fn get_parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet_handler.clone()
    }

    fn get_json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json_handler.clone()
    }
}
