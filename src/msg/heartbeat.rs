use std::sync::Arc;

use crate::{crypto, Frame, Node, Parse};

use async_trait::async_trait;
use bytes::Bytes;

/// Message use for indicating the sender is alive.
///
/// Usually used with timeout event.
#[derive(Debug, Clone)]
pub struct HeartBeat {
    id: u64,
    addr: String,

    sig: Vec<u8>,
}

impl HeartBeat {
    pub(crate) fn new(id: u64, addr: String, sig: &[u8]) -> HeartBeat {
        HeartBeat {
            id,
            addr,
            sig: sig.to_vec(),
        }
    }

    /// Parse a `HeartBeat` instance from a received frame.
    ///
    /// The `HEARTBEAT` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `HeartBeat` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    ///
    /// ```text
    /// HEARTBEAT id addr sig
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HeartBeat> {
        // The `HEARTBEAT` string has already been consumed. The next value is the
        // name of the id to get. If the next value is not a u64 or the
        // input is fully consumed, then an error is returned.
        let id = parse.next_int()?;
        let addr = parse.next_string()?;
        let sig = parse.next_bytes()?;

        Ok(HeartBeat {
            id,
            addr,
            sig: sig.to_vec(),
        })
    }

    /// Converts the message into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `HeartBeat` message to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("heartbeat".as_bytes()));
        frame.push_int(self.id);
        frame.push_string(self.addr);
        frame.push_bulk(self.sig.into());
        frame
    }

    pub(crate) fn get_id(&self) -> u64 {
        self.id
    }

    pub(crate) fn get_addr(&self) -> String {
        self.addr.clone()
    }

    pub(crate) fn get_sig(&self) -> Vec<u8> {
        self.sig.clone()
    }
}

#[async_trait]
impl super::Handler for HeartBeat {
    async fn handle(self, node: Arc<Node>) -> crate::Result<()> {
        let id = self.id;
        let sig = self.sig.clone();

        // Verify if the sender have the same 'SECRET_KEY'.
        if !crypto::verify_signature(&id.to_le_bytes(), &sig) {
            return Err("Wrong signature.".into());
        }

        // Update the sender's activity.
        node.update_activity(id)?;

        Ok(())
    }
}
