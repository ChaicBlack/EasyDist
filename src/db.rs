use tokio::sync::RwLock;

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
#[derive(Debug)]
pub struct Db {
    /// State for algorithm except log.
    //state: Arc<State>,

    /// Handle to log.
    ///
    /// The log is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    ///
    /// Needed to updated on stable storage before responding RPCs.
    pub log: RwLock<Vec<String>>,
}

impl Db {
    pub(crate) fn new() -> Db {
        let log = RwLock::new(Vec::new());

        Db { log }
    }

    /// Get the value associated with an index.
    ///
    /// If the index is out of bouds, return an Err. Otherwise,
    /// return an Ok(value).
    pub(crate) async fn get_log(&self, index: usize) -> crate::Result<String> {
        let log = self.log.read().await;

        if index < log.len() {
            Ok(log[index].clone())
        } else {
            Err(format!(
                "Log index is out of bound. Max {}, found {}",
                log.len() - 1,
                index
            )
            .into())
        }
    }

    /// Set a value of a index.
    ///
    /// If the index is out of bouds, return an Err. Otherwise,
    /// return an Ok(())
    pub(crate) async fn set_log(&self, index: usize, value: &str) -> crate::Result<()> {
        let mut log = self.log.write().await;

        if index < log.len() {
            log[index] = value.to_string();
            Ok(())
        } else {
            Err(format!(
                "Log index is out of bound. Max {}, found {}",
                log.len() - 1,
                index
            )
            .into())
        }
    }

    /// Deep copy the inner log and return it.
    pub(crate) async fn get_log_snapshot(&self) -> Vec<String> {
        let log = self.log.read().await;
        log.clone()
    }
}
