use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::object_store;
use delta_kernel::FileMeta;
use delta_kernel::Snapshot;

use uc_client::*;

use url::Url;

pub struct UCCatalog {
    client: UCClient,
}

impl UCCatalog {
    pub fn new(client: UCClient) -> Self {
        UCCatalog { client }
    }

    pub async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<Snapshot, Box<dyn std::error::Error>> {
        let res = self.client.get_table(table_name).await?;
        let table_id = res.table_id;
        let table_uri = res.storage_location;

        let req = CommitsRequest {
            table_id: table_id.clone(),
            table_uri: table_uri.clone(),
            start_version: Some(0),
            end_version: None,
        };
        let commits = self.client.get_commits(req).await?;

        println!("get commits response: {:?}", commits);

        let creds = self
            .client
            .get_credentials(&table_id, "READ")
            .await
            .map_err(|e| format!("Failed to get credentials: {}", e))?;

        let creds = creds
            .aws_temp_credentials
            .ok_or("No AWS temporary credentials found")?;

        let store = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_region("us-west-2")
                .with_bucket_name("us-west-2-extstaging-managed-catalog-test-bucket-1")
                .with_access_key_id(creds.access_key_id)
                .with_secret_access_key(creds.secret_access_key)
                .with_token(creds.session_token)
                .build()
                .map_err(|e| format!("Failed to create object store: {}", e))?,
        );
        let engine = DefaultEngine::new(store.clone(), Arc::new(TokioBackgroundExecutor::new()));

        // use delta_kernel::object_store::path::Path;
        // use delta_kernel::object_store::ObjectStore;
        // use futures::TryStreamExt;
        // let prefix =
        //     Path::from("19a85dee-54bc-43a2-87ab-023d0ec16013/tables/25a60783-603e-469f-8650-36d02bbde50b/_delta_log");
        // let mut stream = store.list(Some(&prefix));
        // while let Some(meta) = stream.try_next().await? {
        //     println!("{}", meta.location);
        // }

        use delta_kernel::path::*;
        let location0 = FileMeta::new(
            Url::parse( "s3://us-west-2-extstaging-managed-catalog-test-bucket-1/19a85dee-54bc-43a2-87ab-023d0ec16013/tables/25a60783-603e-469f-8650-36d02bbde50b/_delta_log/00000000000000000000.json")?,
            1754682294000,
            1363
        );
        let commit0 = ParsedLogPath {
            location: location0,
            filename: "00000000000000000000.json".to_string(),
            extension: "json".to_string(),
            version: 0,
            file_type: LogPathFileType::Commit,
        };
        let location1 = FileMeta::new(
            Url::parse( "s3://us-west-2-extstaging-managed-catalog-test-bucket-1/19a85dee-54bc-43a2-87ab-023d0ec16013/tables/25a60783-603e-469f-8650-36d02bbde50b/_delta_log/_staged_commits/00000000000000000001.34fe3f1d-626b-444c-87bf-c5693841d449.json")?,
            1754682294000,
            1363
        );
        let commit1 = ParsedLogPath {
            location: location1,
            filename: "00000000000000000001.34fe3f1d-626b-444c-87bf-c5693841d449.json".to_string(),
            extension: "json".to_string(),
            version: 1,
            file_type: LogPathFileType::Commit,
        };
        // spoof the commits
        let commits = vec![commit0, commit1];

        Snapshot::build_from_catalog(Url::parse(&(table_uri + "/"))?, commits)
            .build_latest(&engine)
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[tokio::test]
    async fn read_uc_table() -> Result<(), Box<dyn std::error::Error>> {
        let workspace = "https://e2-dogfood.staging.cloud.databricks.com";
        let token = env::var("DBTOKEN").expect("TOKEN environment variable not set");
        let client = UCClient::new(workspace, &token)?;
        let catalog = UCCatalog::new(client);
        let table = "main.zvs.catalog_table";
        let snapshot = catalog.get_table(table).await?;

        println!("snapshot: {:?}", snapshot);

        Ok(())
    }
}
