use crate::{Connection, Frame};

use tracing::{debug, instrument};

/// Represents an "unknown" message. This is not a real `Redis` message.
#[derive(Debug)]
pub struct Unknown {
    message_name: String,
}

impl Unknown {
    /// Create a new `Unknown` message which responds to unknown messages
    /// issued by clients
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            message_name: key.to_string(),
        }
    }

    /// Returns the message name
    pub(crate) fn get_name(&self) -> &str {
        &self.message_name
    }

    /// Responds to the client, indicating the message is not recognized.
    ///
    /// This usually means the message is not yet implemented by `mini-redis`.
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown message '{}'", self.message_name));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
