use std::sync::Arc;

use bytes::Bytes;
use log::{error, info};
use openssl::{ec::EcKey, pkey::PKey, sign::Verifier};
use serde::{Deserialize, Serialize};

use crate::{msg::UserDefined, parse::Parse, Frame, Node};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendEcdsaEncData {
    sender_id: u64,
    signature: Vec<u8>,
    name: String,
    data: Vec<u8>,
}

impl SendEcdsaEncData {
    pub fn new(sender_id: u64, signature: Vec<u8>, name: &str, enc_data: Vec<u8>) -> Self {
        Self {
            sender_id,
            signature,
            name: name.to_string(),
            data: enc_data,
        }
    }

    /// Get the id
    pub fn sender_id(&self) -> u64 {
        self.sender_id
    }

    /// Get the signature
    pub fn signature(&self) -> Vec<u8> {
        self.signature.clone()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn enc_data(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SendEcdsaEncData> {
        // Read the id to the struct.
        let sender_id = parse.next_int()?;

        // Read the signature to the struct.
        let signature = parse.next_bytes()?;

        let name = parse.next_string()?;

        // Read the enc_data to the struct.
        let enc_data = parse.next_bytes()?;

        Ok(SendEcdsaEncData {
            sender_id,
            signature: signature.to_vec(),
            name,
            data: enc_data.to_vec(),
        })
    }

    /// Create a frame array which represent the command and will be paesed by the destination
    /// node.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("sendecdsaencdata".as_bytes()));
        frame.push_int(self.sender_id);
        frame.push_bulk(self.signature.into());
        frame.push_string(self.name.clone());
        frame.push_bulk(self.data.into());
        frame
    }

    /// Verify the signature, decrypt the data and then handle the message.
    pub(crate) async fn handle(self, node: Arc<Node>) -> crate::Result<()> {
        let id = self.sender_id();
        let signature = self.signature.clone();
        let name = self.name.clone();
        let data = crate::crypto::decrypt_message(&self.data);

        info!("Received ecdsa message from node {}.", id);

        // Verify if the id is in the neighbor map
        if !node.neighbors.contains_key(&id) {
            error!("The node {} is not in the neighbor map.", id);
            return Ok(());
        }

        // Try to get the sender's public key.
        let sender_pub_key = node.clone().request_ecdsa_public_key(id).await?;
        let ec_key = EcKey::public_key_from_pem(&sender_pub_key).unwrap();
        let pub_key = PKey::from_ec_key(ec_key).unwrap();

        // Use the public key to verify the message origin.
        let mut verifier = Verifier::new_without_digest(&pub_key).unwrap();
        verifier.update(&data).unwrap();
        if verifier.verify(&signature).unwrap() {
            println!("Received valid ECDSA signed message from node {}.", node.id);
            info!("Received valid ECDSA signed message from node {}.", node.id);
        } else {
            println!(
                "Received invalid ECDSA signed message from node {}.",
                node.id
            );
            error!(
                "Received invalid ECDSA signed message from node {}.",
                node.id
            );
        }

        // Check if the handler have been registered.
        let handlers = node.user_handlers.read().await;
        if let Some(handler) = handlers.get(&name) {
            println!("received user defined msg {}", name);
            handler
                .handle(UserDefined::new(&name, &data), node.clone())
                .await?;
        } else {
            println!("No handler registered for message type: {}.", name);
            error!("No handler registered for message type: {}", name);
        }

        Ok(())
    }
}
