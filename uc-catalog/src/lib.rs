use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::Version;
use uc_client::UCClient;
use url::Url;

pub struct UCTable<'a> {
    client: &'a UCClient,
    name: String,
    table_root: Url,
    // protocol: Protocol,
    // metadata: Metadata,
    latest_version: Version,
    // log_tail: (), // TODO
}

impl<'a> UCTable<'a> {
    /// calling UCTable::new() will ping the catalog (via UCClient) to fetch the latest table
    /// metadata. since UC is smart, it has cached the latest Protocol/Metadata/version and has a
    /// recent 'tail' of the log in memory to directly hand back to us.
    pub fn new(name: String, table_root: Url, client: &'a UCClient) -> Self {
        todo!()
    }
}

pub struct DumbTable<'a> {
    name: String,
    table_root: Url,
    client: &'a UCClient,
}

// // tests
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use uc_client::UCClient;
//
//     #[test]
//     fn test_uc_resolved_table() {
//         let client = UCClient::default();
//         let table = UCTable::new(
//             "test_table".to_string(),
//             Url::parse("BLAH").unwrap(),
//             &client,
//         );
//
//         table.scan().execute();
//     }
// }
