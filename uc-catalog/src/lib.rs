use delta_kernel::actions::{Metadata, Protocol};
// use delta_kernel::LogFile;
use delta_kernel::Version;
use uc_client::UCClient;
use url::Url;

pub struct UCTable<'a> {
    _client: &'a UCClient,
    name: String,
    table_root: Url,
    protocol: Protocol,
    metadata: Metadata,
    latest_version: Version,
    // log_tail: Vec<LogFile>,
}

impl<'a> UCTable<'a> {
    /// calling UCTable::new() will ping the catalog (via UCClient) to fetch the latest table
    /// metadata. since UC is smart, it has cached the latest Protocol/Metadata/version and has a
    /// recent 'tail' of the log in memory to directly hand back to us.
    pub fn try_new(name: &str, client: &'a UCClient) -> Result<Self, Box<dyn std::error::Error>> {
        // look up table
        let table_metadata = client.get_table(name)?;
        Ok(Self {
            _client: client,
            name: name.to_string(),
            table_root: table_metadata.table_root.clone(),
            protocol: table_metadata.protocol,
            metadata: table_metadata.metadata,
            latest_version: table_metadata.latest_version,
            // log_tail: table_metadata.log_tail,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn table_root(&self) -> &Url {
        &self.table_root
    }

    pub fn protocol(&self) -> &Protocol {
        &self.protocol
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn latest_version(&self) -> Version {
        self.latest_version
    }

    // pub fn log_tail(&self) -> &[LogFile] {
    //     &self.log_tail
    // }
}

pub struct DumbTable {
    name: String,
    table_root: Url,
}

impl DumbTable {
    pub fn try_new(name: &str, client: &UCClient) -> Result<Self, Box<dyn std::error::Error>> {
        // look up table
        let table_metadata = client.get_table(name)?;
        Ok(Self {
            name: name.to_string(),
            table_root: table_metadata.table_root.clone(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn table_root(&self) -> &Url {
        &self.table_root
    }
}
