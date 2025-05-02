use crate::arrow::error::ArrowError;
use crate::object_store;

pub type EngineResult<T, E = EngineError> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    /// Kernel error
    #[error(transparent)]
    KernelError(delta_kernel::Error),

    /// An error performing operations on arrow data
    #[error(transparent)]
    Arrow(ArrowError),

    /// An error enountered while working with parquet data
    #[error("Arrow error: {0}")]
    Parquet(#[from] crate::parquet::errors::ParquetError),

    /// An error interacting with the object_store crate
    // We don't use [#from] object_store::Error here as our From impl transforms
    // object_store::Error::NotFound into Self::FileNotFound
    #[error("Error interacting with object store: {0}")]
    ObjectStore(object_store::Error),

    /// An error working with paths from the object_store crate
    #[error("Object store path error: {0}")]
    ObjectStorePath(#[from] object_store::path::Error),

    #[error("Reqwest Error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("file already exists: {0}")]
    FileAlreadyExists(String),
}
