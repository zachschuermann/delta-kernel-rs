use std::sync::Arc;

use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::filesystem::ObjectStoreStorageHandler;
use crate::engine::default::DefaultEngine;
use crate::log_segment::{LogPathFileType, LogSegment, ParsedLogPath};
use crate::object_store::{memory::InMemory, path::Path, ObjectStore};
use crate::{DeltaResult, FileMeta, Version};
use futures::executor::block_on;
use serde_json::json;
use url::Url;


// Helper to create a new in-memory store
fn new_in_memory_store() -> (Arc<dyn ObjectStore>, Url) {
    let store = Arc::new(InMemory::new());
    let log_root = Url::parse("memory:///_delta_log/").unwrap();
    (store, log_root)
}

// Helper to build a commit log path
fn commit_log_path(log_root: &Url, version: Version, extension: Option<&str>) -> DeltaResult<Url> {
    let filename = match extension {
        Some(ext) => format!("{:020}.{}", version, ext),
        None => format!("{:020}.json", version),
    };
    Ok(log_root.join(&filename)?)
}

// Helper to create a LogSegment with CRC file
fn create_log_segment_with_crc(
    log_root: Url,
    checkpoint_version: Option<Version>,
    crc_version: Version,
    end_version: Version,
    commit_versions: Vec<Version>,
) -> LogSegment {
    let crc_path = ParsedLogPath {
        version: crc_version,
        file_type: LogPathFileType::Crc,
        location: FileMeta::new(
            commit_log_path(&log_root, crc_version, Some("crc")).unwrap(),
            0, // last_modified
            0, // size
        ),
        filename: format!("{:020}.crc", crc_version),
        extension: "crc".to_string(),
    };

    let commit_files: Vec<ParsedLogPath> = commit_versions
        .iter()
        .map(|&v| ParsedLogPath {
            version: v,
            file_type: LogPathFileType::Commit,
            location: FileMeta::new(
                commit_log_path(&log_root, v, Some("json")).unwrap(),
                0, // last_modified
                0, // size
            ),
            filename: format!("{:020}.json", v),
            extension: "json".to_string(),
        })
        .collect();

    LogSegment {
        log_root,
        checkpoint_version,
        checkpoint_parts: vec![],
        latest_crc_file: Some(crc_path),
        ascending_commit_files: commit_files,
        ascending_compaction_files: vec![],
        end_version,
    }
}

#[test]
fn test_protocol_metadata_with_crc_at_target_version() -> DeltaResult<()> {
    // Test case 1: CRC file exists at the exact target version
    // Should return P+M directly from CRC without replay
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create a CRC file with P+M at version 5
    let crc_content = create_crc_with_pm(5);
    let crc_path = commit_log_path(&log_root, 5, Some("crc"))?;
    block_on(async {
        store.put(&Path::from(crc_path.as_str()), crc_content.into()).await
    })?;

    // Create test data: CRC at version 5, reading version 5
    let log_segment =
        create_log_segment_with_crc(log_root.clone(), None, 5, 5, vec![0, 1, 2, 3, 4, 5]);

    // Call protocol_and_metadata - should return P+M directly from CRC
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;
    
    assert!(metadata.is_some());
    assert!(protocol.is_some());
    
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-5");
    
    let protocol = protocol.unwrap();
    assert_eq!(protocol.min_reader_version(), 3);
    assert_eq!(protocol.min_writer_version(), 7);

    Ok(())
}

#[test]
fn test_protocol_metadata_with_crc_newer_than_checkpoint() -> DeltaResult<()> {
    // Test case 2: CRC file is newer than checkpoint
    // Should create pruned log segment starting from CRC version + 1
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC file at version 5
    let crc_content = create_crc_with_pm(5);
    let crc_path = commit_log_path(&log_root, 5, Some("crc"))?;
    block_on(async {
        store.put(&Path::from(crc_path.as_str()), crc_content.into()).await
    })?;
    
    // Create checkpoint at version 3 (older than CRC)
    create_checkpoint(&store, &log_root, 3)?;
    
    // Create commits with P+M at version 7
    create_commit_with_pm(&store, &log_root, 7)?;

    // Create test data: checkpoint at v3, CRC at v5, reading v8
    let log_segment = create_log_segment_with_crc(
        log_root.clone(),
        Some(3),
        5,
        8,
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
    );

    // When we call protocol_and_metadata, it should:
    // 1. Create a pruned segment with only commits 6, 7, 8
    // 2. Clear checkpoint data
    // 3. Find P+M from commit 7
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;
    
    assert!(metadata.is_some());
    assert!(protocol.is_some());
    
    // Should get P+M from commit 7, not from CRC
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-7");

    Ok(())
}

#[test]
fn test_protocol_metadata_with_crc_older_than_checkpoint() -> DeltaResult<()> {
    // Test case 3: CRC file is older than checkpoint
    // Should use normal log segment (full replay from checkpoint)
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC file at version 2 (older than checkpoint)
    let crc_content = create_crc_with_pm(2);
    let crc_path = commit_log_path(&log_root, 2, Some("crc"))?;
    block_on(async {
        store.put(&Path::from(crc_path.as_str()), crc_content.into()).await
    })?;
    
    // Create checkpoint with P+M at version 5
    create_checkpoint_with_pm(&store, &log_root, 5)?;

    // Create test data: CRC at v2, checkpoint at v5, reading v8
    let log_segment = create_log_segment_with_crc(
        log_root.clone(),
        Some(5),
        2,
        8,
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
    );
    
    // When CRC is older than checkpoint, should use normal replay from checkpoint
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;
    
    assert!(metadata.is_some());
    assert!(protocol.is_some());
    
    // Should get P+M from checkpoint, not CRC
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-5");

    Ok(())
}

#[test]
fn test_protocol_metadata_no_crc_file() -> DeltaResult<()> {
    // Test case 4: No CRC file exists
    // Should use normal log segment
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create checkpoint with P+M at version 3
    create_checkpoint_with_pm(&store, &log_root, 3)?;

    let log_segment = LogSegment {
        log_root: log_root.clone(),
        checkpoint_version: Some(3),
        checkpoint_parts: vec![],
        latest_crc_file: None,
        ascending_commit_files: vec![],
        ascending_compaction_files: vec![],
        end_version: 5,
    };
    
    // With no CRC file, should use normal replay
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;
    
    assert!(metadata.is_some());
    assert!(protocol.is_some());
    
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-3");

    Ok(())
}

#[test]
fn test_protocol_metadata_fallback_to_crc_when_no_pm_in_replay() -> DeltaResult<()> {
    // Test case 5: No P+M found in replay, should fallback to CRC
    // This tests step 4 of your implementation
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC with P+M at version 5
    let crc_content = create_crc_with_pm(5);
    let crc_path = commit_log_path(&log_root, 5, Some("crc"))?;
    block_on(async {
        store.put(&Path::from(crc_path.as_str()), crc_content.into()).await
    })?;
    
    // Create commits 6, 7, 8 without P+M actions
    for v in 6..=8 {
        create_commit_without_pm(&store, &log_root, v)?;
    }

    // Create scenario where commits after CRC don't contain P+M
    let log_segment = create_log_segment_with_crc(
        log_root.clone(),
        None,
        5,
        8,
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
    );
    
    // Should fallback to CRC P+M when replay finds nothing
    let (metadata, protocol) = log_segment.protocol_and_metadata(&engine)?;
    
    assert!(metadata.is_some());
    assert!(protocol.is_some());
    
    // Should get P+M from CRC at version 5
    let metadata = metadata.unwrap();
    assert_eq!(metadata.id, "test-table-5");

    Ok(())
}

#[test]
fn test_protocol_metadata_crc_without_protocol_should_error() -> DeltaResult<()> {
    // Test case 6: CRC file missing Protocol should error
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC without protocol field
    let crc_content = create_crc_without_protocol(5);
    let crc_path = commit_log_path(&log_root, 5, Some("crc"))?;
    block_on(async {
        store.put(&Path::from(crc_path.as_str()), crc_content.into()).await
    })?;
    
    let log_segment = create_log_segment_with_crc(log_root.clone(), None, 5, 5, vec![5]);
    
    // Should error when CRC is missing protocol
    let result = log_segment.protocol_and_metadata(&engine);
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(e.to_string().contains("Protocol") || e.to_string().contains("protocol"));
    }

    Ok(())
}

#[test]
fn test_protocol_metadata_crc_without_metadata_should_error() -> DeltaResult<()> {
    // Test case 7: CRC file missing Metadata should error
    let (store, log_root) = new_in_memory_store();
    let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

    // Create CRC without metadata field
    let crc_content = create_crc_without_metadata(5);
    let crc_path = commit_log_path(&log_root, 5, Some("crc"))?;
    block_on(async {
        store.put(&Path::from(crc_path.as_str()), crc_content.into()).await
    })?;
    
    let log_segment = create_log_segment_with_crc(log_root.clone(), None, 5, 5, vec![5]);
    
    // Should error when CRC is missing metadata
    let result = log_segment.protocol_and_metadata(&engine);
    assert!(result.is_err());
    
    if let Err(e) = result {
        assert!(e.to_string().contains("Metadata") || e.to_string().contains("metadata"));
    }

    Ok(())
}

#[test]
fn test_find_commit_cover_with_crc_pruned_segment() -> DeltaResult<()> {
    // Test case 8: Verify find_commit_cover works correctly with pruned segments
    let (_, log_root) = new_in_memory_store();

    // Create a pruned segment (simulating what happens in protocol_and_metadata)
    let mut log_segment = create_log_segment_with_crc(
        log_root.clone(),
        Some(3),
        5,
        8,
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
    );

    // Simulate pruning
    log_segment.checkpoint_version = None;
    log_segment.checkpoint_parts.clear();
    log_segment.ascending_commit_files.retain(|c| c.version > 5);

    // find_commit_cover should only return commits 6, 7, 8
    let covered_files = log_segment.find_commit_cover();
    assert_eq!(covered_files.len(), 3);

    Ok(())
}

// Helper functions for creating test data

fn create_crc_with_pm(version: Version) -> Vec<u8> {
    let crc_json = json!({
        "crc": {
            "tableSizeBytes": 1000,
            "numFiles": 10,
            "numMetadata": 1,
            "numProtocol": 1,
            "metadata": {
                "id": format!("test-table-{}", version),
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1677811175
            },
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": ["columnMapping"]
            }
        }
    });
    crc_json.to_string().into_bytes()
}

fn create_crc_without_protocol(version: Version) -> Vec<u8> {
    let crc_json = json!({
        "crc": {
            "tableSizeBytes": 1000,
            "numFiles": 10,
            "numMetadata": 1,
            "numProtocol": 0,
            "metadata": {
                "id": format!("test-table-{}", version),
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1677811175
            }
        }
    });
    crc_json.to_string().into_bytes()
}

fn create_crc_without_metadata(_version: Version) -> Vec<u8> {
    let crc_json = json!({
        "crc": {
            "tableSizeBytes": 1000,
            "numFiles": 10,
            "numMetadata": 0,
            "numProtocol": 1,
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": ["columnMapping"]
            }
        }
    });
    crc_json.to_string().into_bytes()
}

fn create_commit_with_pm(
    store: &dyn ObjectStore,
    log_root: &Url,
    version: Version,
) -> DeltaResult<()> {
    let commit_json = json!({
        "metadata": {
            "id": format!("test-table-{}", version),
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        }
    }).to_string() + "\n" + &json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    }).to_string();
    
    let commit_path = commit_log_path(log_root, version, Some("json"))?;
    block_on(async {
        store.put(&Path::from(commit_path.as_str()), commit_json.into()).await
    })?;
    Ok(())
}

fn create_commit_without_pm(
    store: &dyn ObjectStore,
    log_root: &Url,
    version: Version,
) -> DeltaResult<()> {
    // Create a commit with just add actions, no P+M
    let commit_json = json!({
        "add": {
            "path": format!("file-{}.parquet", version),
            "size": 1000,
            "modificationTime": 1677811175,
            "dataChange": true
        }
    }).to_string();
    
    let commit_path = commit_log_path(log_root, version, Some("json"))?;
    block_on(async {
        store.put(&Path::from(commit_path.as_str()), commit_json.into()).await
    })?;
    Ok(())
}

fn create_checkpoint(
    store: &dyn ObjectStore,
    log_root: &Url,
    version: Version,
) -> DeltaResult<()> {
    // For now, create an empty checkpoint file
    let checkpoint_path = commit_log_path(log_root, version, Some("checkpoint.parquet"))?;
    block_on(async {
        store.put(&Path::from(checkpoint_path.as_str()), vec![].into()).await
    })?;
    Ok(())
}

fn create_checkpoint_with_pm(
    store: &dyn ObjectStore,
    log_root: &Url,
    version: Version,
) -> DeltaResult<()> {
    // This would need proper parquet writing in real implementation
    // For now, we'll create a JSON checkpoint for simplicity
    let checkpoint_json = json!({
        "metadata": {
            "id": format!("test-table-{}", version),
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1677811175
        }
    }).to_string() + "\n" + &json!({
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["columnMapping"],
            "writerFeatures": ["columnMapping"]
        }
    }).to_string();
    
    let checkpoint_path = commit_log_path(log_root, version, Some("checkpoint.json"))?;
    block_on(async {
        store.put(&Path::from(checkpoint_path.as_str()), checkpoint_json.into()).await
    })?;
    Ok(())
}