use std::sync::Arc;

use bytes::Bytes;
use log::error;
use rsa::{Pkcs1v15Encrypt, Pkcs1v15Sign};
use sha2::{Digest, Sha256};

use crate::{msg::UserDefined, parse::Parse, Frame, Node};

/// Used for communication between nodes, encrypted by Rsa Pkcs1v15Encrypt padding. The payload is
/// encrypted by the receiver's public key and can be decrypted by receiver's private key. The
/// signature is decrypted by sender's private key and can be verified by sender's public key.
///
/// This mechanism provide a strong ensurance of protecting the communication between distributed
/// nodes.
#[derive(Debug)]
pub struct SendRsaEncData {
    /// Sender's id.
    id: u64,

    /// A signature to verify the sender generated with sender's private key, receiver can use
    /// this and sender's public key to verify the sender.
    signature: Vec<u8>,

    /// The data's name. Indicate the registered handler for the data.
    name: String,

    /// The payload data encrypted by receiver's public key, this can be decrepted by the
    /// receiver's private key.
    enc_data: Vec<u8>,
}

impl SendRsaEncData {
    /// Create a new `GetRsaPublicKey` command which request public key.
    pub fn new(id: u64, signature: Vec<u8>, name: &str, enc_data: Vec<u8>) -> SendRsaEncData {
        SendRsaEncData {
            id,
            signature,
            name: name.to_string(),
            enc_data,
        }
    }

    /// Get the id
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the signature.
    pub fn signature(&self) -> Vec<u8> {
        self.signature.clone()
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Get the encrypted data.
    pub fn enc_data(&self) -> Vec<u8> {
        self.enc_data.clone()
    }

    /// Parse a `GetRsaPublicKey` instance from the received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SendRsaEncData> {
        // Read the id to the struct.
        let id = parse.next_int()?;

        // Read the signature to the struct.
        let signature = parse.next_bytes()?;

        let name = parse.next_string()?;

        // Read the enc_data to the struct.
        let enc_data = parse.next_bytes()?;

        Ok(SendRsaEncData {
            id,
            signature: signature.to_vec(),
            name,
            enc_data: enc_data.to_vec(),
        })
    }

    /// Create a frame array which represent the command and will be paesed by the destination
    /// node.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("sendrsaencdata".as_bytes()));
        frame.push_int(self.id);
        frame.push_bulk(self.signature.into());
        frame.push_string(self.name.clone());
        frame.push_bulk(self.enc_data.into());
        frame
    }

    /// Verify the signature, decrypt the data and then handle the message.
    ///
    /// NOTE: I create a UserDefined to help handle the logic, so the SendRsaEncData still need a
    /// registered handler for every name.
    pub(crate) async fn handle(self, node: Arc<Node>) -> crate::Result<()> {
        let id = self.id();
        let signature = self.signature.clone();
        let name = self.name.clone();
        let enc_data = self.enc_data.clone();

        // Verify if the id is in the neighbor map
        if !node.neighbors.contains_key(&id) {
            error!("The node {} is not in the neighbor map.", id);
            return Ok(());
        }

        // Try to get the sender's public key.
        let sender_pub_key = node.clone().request_rsa_public_key(id).await?;
        // Use the public key to verify the message origin.
        let mut hasher = Sha256::new();
        hasher.update(id.to_le_bytes());
        let hashed = hasher.finalize();
        if let Err(e) = sender_pub_key.verify(Pkcs1v15Sign::new::<Sha256>(), &hashed, &signature) {
            error!(
                "The message from node {} failed the verificationo: {}.",
                id, e
            );
            return Ok(());
        }

        let dec_data = match node
            .clone()
            .rsa_private_key
            .decrypt(Pkcs1v15Encrypt, &enc_data)
        {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to decrypt the message from node {}: {}.", id, e);
                return Ok(());
            }
        };

        // Check if the handler have been registered.
        let handlers = node.user_handlers.read().await;
        if let Some(handler) = handlers.get(&name) {
            println!("received user defined msg {}", name);
            handler
                .handle(UserDefined::new(&name, &dec_data), node.clone())
                .await?;
        } else {
            println!("No handler registered for message type: {}.", name);
            error!("No handler registered for message type: {}", name);
        }

        Ok(())
    }
}
