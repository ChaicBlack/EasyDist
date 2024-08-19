use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a u64, because GET only
/// handles u64 values.
#[derive(Debug)]
pub struct Get {
    /// Name of the key to get.
    key: u64,
}

impl Get {
    /// Create a new 'Get' message which fetches 'key'.
    pub fn new(key: impl Into<u64>) -> Self {
        Get { key: key.into() }
    }

    /// Get the key.
    pub fn key(&self) -> u64 {
        self.key
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a u64 or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_int()?;

        Ok(Get { key })
    }

    /// Apply the `Get` message to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received message.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Get the value from the shared database state
        let response = if let Ok(value) = db.get_log(self.key as usize).await {
            // If a value is present, it is written to the client in "simple"
            // format.
            Frame::Simple(value)
        } else {
            // If there is no value, `Null` is written.
            Frame::Null
        };

        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response);

        Ok(())
    }

    /// Converts the message into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Get` message to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_int(self.key);
        frame
    }
}
