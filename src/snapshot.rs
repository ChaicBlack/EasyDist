use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;

use dashmap::DashMap;

use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json;
use std::io::BufReader;

/// This struct is for node to backup important data and regain it after
/// recovering from failure
#[derive(Debug)]
pub(crate) struct Backup {
    neighbors: DashMap<u64, SocketAddr>,
    log: Vec<String>,
}

impl Backup {
    /// Create a backup.
    ///
    /// The arguments will be transformed to those has implemented Serialize and Deserialize
    /// trait when needed. For example dashmap to hashmap.
    pub(crate) fn new(neighbors: DashMap<u64, SocketAddr>, log: Vec<String>) -> Backup {
        Backup { neighbors, log }
    }

    /// Save the backup to a file named 'backup.json'
    pub(crate) fn save_to_file(&self) -> std::io::Result<()> {
        // serialize the data structure
        let serialized = serde_json::to_string(&self)?;

        // save to the file "backup.json"
        let mut file = File::create("backup.json")?;
        file.write_all(serialized.as_bytes())?;
        Ok(())
    }

    /// Read from file "backup.json" and deserialize it.
    pub(crate) fn load_from_file() -> std::io::Result<Backup> {
        // open the file
        let file = File::open("backup.json")?;
        let reader = BufReader::new(file);

        // deserialize the data structure
        let backup = serde_json::from_reader(reader)?;
        Ok(backup)
    }
}

impl Serialize for Backup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Backup", 2)?;
        let neighbors: HashMap<_, _> = self
            .neighbors
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        state.serialize_field("neighbors", &neighbors)?;
        state.serialize_field("log", &self.log)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Backup {
    fn deserialize<D>(deserializer: D) -> Result<Backup, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct BackupData {
            neighbors: HashMap<u64, SocketAddr>,
            log: Vec<String>,
        }

        let BackupData { neighbors, log } = BackupData::deserialize(deserializer)?;
        let dash_map = DashMap::new();
        for (k, v) in neighbors {
            dash_map.insert(k, v);
        }

        Ok(Backup {
            neighbors: dash_map,
            log,
        })
    }
}
