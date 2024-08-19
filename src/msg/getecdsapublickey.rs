use std::sync::Arc;

use bytes::Bytes;
use log::info;
use serde_json;

use crate::{parse::Parse, Connection, Frame, Node};

/// Request to get the Ecdsa public key of this node.
///
/// The `id` field is the id number of a neighbor of current node. Which means that the public key
/// will only be sent to neighbors.
#[derive(Debug)]
pub struct GetEcdsaPublicKey {
    /// Indicates which neighbor the message sent from.
    id: u64,
}

impl GetEcdsaPublicKey {
    /// Create a new `GetEcdsaPublicKey` command which request public key.
    pub fn new(id: u64) -> GetEcdsaPublicKey {
        GetEcdsaPublicKey { id }
    }

    /// Get the id
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Parse a `GetEcdsaPublicKey` instance from the received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<GetEcdsaPublicKey> {
        // Read the id to the struct.
        let id = parse.next_int()?;

        Ok(GetEcdsaPublicKey { id })
    }

    /// Create a frame array which represent the command and will be paesed by the destination
    /// node.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("getecdsapublickey".as_bytes()));
        frame.push_int(self.id);
        frame
    }

    pub(crate) async fn handle(self, node: Arc<Node>, mut conn: Connection) -> crate::Result<()> {
        let id = self.id();

        // Verify if the id is in the neighbor map
        if !node.neighbors.contains_key(&id) {
            let responce = Frame::Error("the id is not in the neighbor map.".into());
            let _ = conn.write_frame(&responce).await?;
            return Ok(());
        }

        // Create the responce.
        let responce = Frame::Simple(serde_json::to_string(&node.ecdsa_public_key).unwrap());

        // Send the frame into the connection.
        conn.write_frame(&responce).await?;

        info!("Sent the ecdsasa public key to node {}.", id);

        Ok(())
    }
}
