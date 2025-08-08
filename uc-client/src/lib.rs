use std::collections::HashMap;

use reqwest::header;
use reqwest::Client;
use serde::{Deserialize, Serialize};

pub struct UCClient {
    client: Client,
    url: String, // Url?
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitsRequest {
    // pub table_path: String,
    pub table_id: String,
    pub table_uri: String,
    pub start_version: Option<i64>,
    pub end_version: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommitsResponse {
    pub commits: Option<Vec<Commit>>,
    pub latest_table_version: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Commit {
    pub version: i64,
    pub timestamp: i64,
    pub file_name: String,
    pub file_size: i64,
    pub file_modification_timestamp: i64,
    pub is_disown_commit: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TablesResponse {
    // subset of fields
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: String,
    pub storage_location: String,
    pub owner: String,
    pub properties: HashMap<String, String>,
    pub securable_kind: String,
    pub metastore_id: String,
    pub table_id: String,
    pub schema_id: String,
    pub catalog_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TemporaryTableCredentials {
    pub aws_temp_credentials: Option<AwsTempCredentials>,
    pub expiration_time: i64,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AwsTempCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

impl UCClient {
    pub fn new(workspace: &str, token: &str) -> Result<Self, reqwest::Error> {
        let url = format!("{}/api/2.1/unity-catalog", workspace);

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(), // FIXME
        );
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        let client = Client::builder().default_headers(headers).build()?;

        Ok(Self { client, url })
    }

    pub async fn get_commits(
        &self,
        request: CommitsRequest,
    ) -> Result<CommitsResponse, reqwest::Error> {
        let url = format!("{}/delta/preview/commits", self.url);
        let request = self
            .client
            .request(reqwest::Method::GET, &url)
            .json(&request)
            .build()?;

        let response = self.client.execute(request).await?;

        // println!("commits response: {:?}", response);

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await?;
            eprintln!("Error response ({}): {}", status, text);
            if status == 404 {
                eprintln!("Invalid Target Table");
            }
            todo!("error");
            // return Err(reqwest::Error::builder().status(status).build());
        }

        // println!(
        // "commits response: {}",
        // response.json::<serde_json::Value>().await?
        // );

        response.json::<CommitsResponse>().await
    }

    pub async fn get_table(&self, table_name: &str) -> Result<TablesResponse, reqwest::Error> {
        let url = format!("{}/tables/{table_name}", self.url);
        let request = self.client.get(&url).build()?;

        let response = self.client.execute(request).await?;

        // println!("table response: {:?}", response);

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await?;
            eprintln!("Error response ({}): {}", status, text);
            if status == 404 {
                eprintln!("Invalid Target Table");
            }
            todo!("error");
            // return Err(reqwest::Error::builder().status(status).build());
        }

        response.json::<TablesResponse>().await
    }

    // TODO make enum for operation
    pub async fn get_credentials(
        &self,
        table_id: &str,
        operation: &str,
    ) -> Result<TemporaryTableCredentials, reqwest::Error> {
        let url = format!("{}/temporary-table-credentials", self.url);
        let request = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "operation": operation,
                "table_id": table_id,
            }))
            .build()?;

        let response = self.client.execute(request).await?;

        // println!("credentials response: {:?}", response);

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await?;
            eprintln!("Error response ({}): {}", status, text);
            todo!("error");
            // return Err(reqwest::Error::builder().status(status).build());
        }

        response.json().await
    }
}
