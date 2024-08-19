use crate::msg::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
#[derive(Debug)]
pub struct Set {
    key: u64,

    value: String,
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    pub fn new(key: impl Into<u64>, value: impl ToString) -> Set {
        Set {
            key: key.into(),
            value: value.to_string(),
        }
    }

    /// Get the key
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Get the value
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Parse a `Set` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Set` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least 2 entries.
    ///
    /// ```text
    /// SET key value
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        // Read the key to set. This is a required field
        let key = parse.next_int()?;

        // Read the value to set. This is a required field.
        let value = parse.next_string()?;

        Ok(Set { key, value })
    }

    /// Apply the `Set` message to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received message.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Set the value in the shared database state.
        db.set_log(self.key as usize, &self.value).await?;

        // Create a success response and write it to `dst`.
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the message into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Set` message to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_int(self.key);
        frame.push_bulk(Bytes::from(self.value.into_bytes()));
        frame
    }
}
