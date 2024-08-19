use std::sync::Arc;

use crate::{crypto, Frame, Node, Parse};

use async_trait::async_trait;
use bytes::Bytes;

/// Message use for establishing a new connection.
#[derive(Debug, Clone)]
pub struct Establish {
    id: u64,
    addr: String,

    sig: Vec<u8>,
}

impl Establish {
    pub(crate) fn new(id: u64, addr: String, sig: &[u8]) -> Establish {
        Establish {
            id,
            addr,
            sig: sig.to_vec(),
        }
    }

    /// Parse a `Establish` instance from a received frame.
    ///
    /// The `ESTABLISH` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Establish` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// ```text
    /// ESTABLISH id addr sig
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Establish> {
        // The 'ESTABLISH` string has already been consumed. The next value is the
        // name of the id to get. If the next value is not a u64 or the
        // input is fully consumed, then an error is returned.
        let id = parse.next_int()?;
        let addr = parse.next_string()?;
        let sig = parse.next_bytes()?;

        Ok(Establish {
            id,
            addr,
            sig: sig.to_vec(),
        })
    }

    /// Converts the message into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Establish` message to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("establish".as_bytes()));
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
impl super::Handler for Establish {
    async fn handle(self, node: Arc<Node>) -> crate::Result<()> {
        // Extract the infomation in the message.
        let id = self.get_id();
        let addr = self.get_addr();
        let sig = self.get_sig();
        println!("received establish message from node {}.", id);

        // Verify if the sender have the same 'SECRET_KEY'.
        if !crypto::verify_signature(&id.to_le_bytes(), &sig) {
            println!("Wrong signature");
            return Err("Wrong signature.".into());
        }

        // If the sender is not in the neighbor map. Add it in and
        // send a heartbeat message to it. Set the activity to true.
        if !node.neighbors.contains_key(&id) && !node.neighbors_activity.contains_key(&id) {
            // Add in neighbor map
            node.neighbors.insert(id, addr.parse().unwrap());
            // Set activity
            node.neighbors_activity.insert(id, true);

            println!("has established connection with node {}.", id);
        }

        Ok(())
    }
}
