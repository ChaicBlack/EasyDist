use std::sync::Arc;

use crate::{crypto, Frame, Node, Parse};

use async_trait::async_trait;
use bytes::Bytes;

/// Message use for recovering nodes to restablish connection with neighbors.
#[derive(Debug, Clone)]
pub struct Restablish {
    id: u64,
    addr: String,

    sig: Vec<u8>,
}

impl Restablish {
    pub(crate) fn new(id: u64, addr: String, sig: &[u8]) -> Restablish {
        Restablish {
            id,
            addr,
            sig: sig.to_vec(),
        }
    }

    /// Parse a `Restablish` instance from a received frame.
    ///
    /// The `RESTABLISH` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Restablish` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// ```text
    /// RESTABLISH id addr sig
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Restablish> {
        // The 'RESTABLISH` string has already been consumed. The next value is the
        // name of the id to get. If the next value is not a u64 or the
        // input is fully consumed, then an error is returned.
        let id = parse.next_int()?;
        let addr = parse.next_string()?;
        let sig = parse.next_bytes()?;

        Ok(Restablish {
            id,
            addr,
            sig: sig.to_vec(),
        })
    }

    /// Converts the message into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Restablish` message to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("restablish".as_bytes()));
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
impl super::Handler for Restablish {
    async fn handle(self, node: Arc<Node>) -> crate::Result<()> {
        // Extract the infomation in the message.
        let id = self.get_id();
        let _addr = self.get_addr();
        let sig = self.get_sig();

        // Verify if the sender have the same 'SECRET_KEY'.
        if !crypto::verify_signature(&id.to_le_bytes(), &sig) {
            return Err("Wrong signature.".into());
        }

        // The sender need to be a neighbor already and its activity is false.
        //
        // Or this will return an Err.
        if node.neighbors.contains_key(&id) {
            if let Some(mut value) = node.neighbors_activity.get_mut(&id) {
                if *value == false {
                    *value = true;
                } else {
                    return Err(
                        format!("The establish message is from an active node {}.", id).into(),
                    );
                }
            };
        } else {
            return Err(format!("There is no node {}.", id).into());
        }

        Ok(())
    }
}
