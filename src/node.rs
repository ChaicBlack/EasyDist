use std::collections::HashMap;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use crate::msg::{
    Establish, GetEcdsaPublicKey, GetRsaPublicKey, Handler, HeartBeat, SendEcdsaEncData,
    SendRsaEncData, UserDefined,
};
use crate::snapshot::Backup;
use crate::Db;
use crate::{config, Connection, Message};
use crate::{crypto, Frame};

use async_trait::async_trait;
use dashmap::DashMap;

use log::{debug, error, info};
use openssl::pkey::{Private, Public};
use openssl::{
    ec::*,
    nid::Nid,
    pkey::PKey,
    sign::{Signer, Verifier},
};
use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, Pkcs1v15Sign, RsaPrivateKey, RsaPublicKey};
use serde_json;
use sha2::{Digest, Sha256};

use tokio::net::ToSocketAddrs;
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time,
};

#[derive(Clone)]
pub struct Node {
    pub id: u64,
    pub addr: SocketAddr,

    /// Handle the neighboring nodes' address.
    ///
    /// The key is the id of neighbor, the value is a std::net::SocketAddr of neighbor.
    pub neighbors: Arc<DashMap<u64, SocketAddr>>,

    /// This is for check neighbor nodes' activity.
    ///
    /// Need to be set when receiving heartbeat.
    pub neighbors_activity: Arc<DashMap<u64, bool>>,

    /// This a handle of the data stored by the node.
    pub db: Arc<Db>,

    /// Store the handlers for handle user defined messages.
    pub(crate) user_handlers:
        Arc<RwLock<HashMap<String, Arc<dyn UserMessageHandler + Send + Sync>>>>,

    /// The Rsa public key of the node, every connection can get this key.
    pub(crate) rsa_public_key: RsaPublicKey,

    /// The Rsa private key of the node
    pub(crate) rsa_private_key: RsaPrivateKey,

    /// The ECDSA private key of the node.
    pub(crate) ecdsa_public_key: Vec<u8>,

    /// The ECDSA private key of the node.
    pub(crate) ecdsa_private_key: PKey<Private>,
}

impl Default for Node {
    /// Create a new node instance. The id and address is from the config.rs and the
    /// other arguments is empty.
    fn default() -> Node {
        // Create the Rsa public and private key of the node.
        let mut rng = OsRng;
        let bits = 2048;
        let rsa_private_key =
            RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate the private key.");
        let rsa_public_key = RsaPublicKey::from(&rsa_private_key);

        // Create the ECDSA public and private key of the node.
        let group = EcGroup::from_curve_name(Nid::SECP256K1).unwrap();
        let ec_key = EcKey::generate(&group).unwrap();
        let ecdsa_private_key = PKey::from_ec_key(ec_key).unwrap();
        let ecdsa_public_key = ecdsa_private_key.public_key_to_pem().unwrap();

        Node {
            id: config::ID,
            addr: config::ADDR.parse().unwrap(),
            neighbors: Arc::new(DashMap::new()),
            neighbors_activity: Arc::new(DashMap::new()),
            db: Arc::new(Db::new()),
            user_handlers: Arc::new(RwLock::new(HashMap::new())),
            rsa_private_key,
            rsa_public_key,
            ecdsa_public_key,
            ecdsa_private_key,
        }
    }
}

impl Node {
    /// Create a new node instance.
    pub fn new(id: u64, addr: &str) -> Node {
        // Create the Rsa public and private key of the node.
        let mut rng = OsRng;
        let bits = 2048;
        let private_key =
            RsaPrivateKey::new(&mut rng, bits).expect("Failed to generate the private key.");
        let public_key = RsaPublicKey::from(&private_key);

        // Create the ECDSA public and private key of the node.
        let group = EcGroup::from_curve_name(Nid::SECP256K1).unwrap();
        let ec_key = EcKey::generate(&group).unwrap();
        let ecdsa_private_key = PKey::from_ec_key(ec_key).unwrap();
        let ecdsa_public_key = ecdsa_private_key.public_key_to_pem().unwrap();

        Node {
            id,
            addr: addr.parse().unwrap(),
            neighbors: Arc::new(DashMap::new()),
            neighbors_activity: Arc::new(DashMap::new()),
            db: Arc::new(Db::new()),
            user_handlers: Arc::new(RwLock::new(HashMap::new())),
            rsa_private_key: private_key,
            rsa_public_key: public_key,
            ecdsa_public_key,
            ecdsa_private_key,
        }
    }

    /// Establish a connection with another node.
    ///
    /// The first argument is the id of that node, the second is address.
    pub async fn establish_conn(
        self: Arc<Self>,
        remote_id: u64,
        remote_addr: &str,
    ) -> crate::Result<()> {
        // Generate the signature for message creation.
        let sig = crypto::generate_signature(&self.id.to_le_bytes());

        // Create the 'Establish' message.
        let es = Establish::new(self.id, self.addr.clone().to_string(), &sig);

        let remote_addr = match remote_addr.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Invalid address: {}", e);
                return Err(e.into());
            }
        };

        let stream = match TcpStream::connect(remote_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to connect to {}: {}", remote_addr, e);
                return Ok(());
            }
        };

        let mut conn = Connection::new(stream);

        let _ = conn.write_frame(&es.into_frame()).await;

        // Add the remote mode to local neighbors map.
        self.neighbors.insert(remote_id, remote_addr);
        self.neighbors_activity.insert(remote_id, true);

        info!("established connection with node {}", remote_id);
        println!("established connection with node {}", remote_id);

        Ok(())
    }

    /// Package serialized data into a UserDefined msg and send it.
    pub async fn send_userdefinedmsg(
        self: Arc<Self>,
        remote_id: u64,
        name: &str,
        data: &[u8],
    ) -> crate::Result<()> {
        let mut conn = {
            if let Some(node) = self.neighbors.get(&remote_id) {
                Connection::new(TcpStream::connect(node.value()).await?)
            } else {
                println!("There is no neighbor with id {}.", remote_id);
                error!("There is no neighbor with id {}.", remote_id);
                return Ok(());
            }
        };

        let msg = UserDefined::new(name, data);

        println!("Have sent the user defined message {:?}.", msg);
        info!("Have sent the user defined message {:?}.", msg);

        conn.write_frame(&msg.into_frame()).await?;

        Ok(())
    }

    /// For user to register their handlers.
    pub async fn register_user_handler(
        self: Arc<Self>,
        message_type: &str,
        handler: Arc<dyn UserMessageHandler + Send + Sync>,
    ) {
        self.user_handlers
            .write()
            .await
            .insert(message_type.to_string(), handler);
        println!("Handler for '{}' registered.", message_type);
    }

    /// Try to get the rsa public key of another node. The key will be returned as Ok(Some(RsaPublicKey)).
    ///
    /// The anothoer node must be a active neighbor.
    pub async fn request_rsa_public_key(
        self: Arc<Self>,
        remote_id: u64,
    ) -> crate::Result<RsaPublicKey> {
        // Create the command
        let gpk = GetRsaPublicKey::new(self.id);

        // Look up the socket address of the remote node.
        let remote_addr = match self.neighbors.get(&remote_id) {
            Some(neighbor) => {
                match self.neighbors_activity.get(&remote_id) {
                    Some(_) => neighbor.value().clone(),

                    None => {
                        error!("Error when trying to get node {}'s public key: This node isn't active.", remote_id);
                        return Err(format!("Error when trying to get node {}'s public key: This node isn't active.", remote_id).into());
                    }
                }
            }

            None => {
                error!(
                    "Error when trying to get node {}'s public key: This node doesn't exist.",
                    remote_id
                );
                return Err(format!(
                    "Error when trying to get node {}'s public key: This node doesn't exist.",
                    remote_id
                )
                .into());
            }
        };

        // Connect to the remote node.
        let stream = match TcpStream::connect(remote_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to connect to {}: {}", remote_addr, e);
                return Err(format!("Failed to connect to {}: {}", remote_addr, e).into());
            }
        };
        let mut conn = Connection::new(stream);

        // Send the message to the remote node.
        let _ = conn.write_frame(&gpk.into_frame()).await;
        info!(
            "Having sent a `Get Public Key` command to node {}.",
            remote_id
        );

        // Waiting for the responce.
        match conn.read_frame().await {
            Ok(Some(frame)) => match frame {
                Frame::Simple(data) => {
                    println!("Get the public key of node {}.", remote_id);
                    info!("Get the public key of node {}.", remote_id);
                    Ok(serde_json::from_str(&data).unwrap())
                }
                Frame::Error(e) => Err(e.into()),
                _ => Err("Frame responsed by peer not right.".into()),
            },
            _ => Err("Connection got problems.".into()),
        }
    }

    /// Try to get the public key of another node. The key will be returned as Ok(Some(RsaPublicKey)).
    ///
    /// The anothoer node must be a active neighbor.
    pub async fn request_ecdsa_public_key(
        self: Arc<Self>,
        remote_id: u64,
    ) -> crate::Result<Vec<u8>> {
        // Create the command
        let gpk = GetEcdsaPublicKey::new(self.id);

        // Look up the socket address of the remote node.
        let remote_addr = match self.neighbors.get(&remote_id) {
            Some(neighbor) => {
                match self.neighbors_activity.get(&remote_id) {
                    Some(_) => neighbor.value().clone(),

                    None => {
                        error!("Error when trying to get node {}'s public key: This node isn't active.", remote_id);
                        return Err(format!("Error when trying to get node {}'s public key: This node isn't active.", remote_id).into());
                    }
                }
            }

            None => {
                error!(
                    "Error when trying to get node {}'s public key: This node doesn't exist.",
                    remote_id
                );
                return Err(format!(
                    "Error when trying to get node {}'s public key: This node doesn't exist.",
                    remote_id
                )
                .into());
            }
        };

        // Connect to the remote node.
        let stream = match TcpStream::connect(remote_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to connect to {}: {}", remote_addr, e);
                return Err(format!("Failed to connect to {}: {}", remote_addr, e).into());
            }
        };
        let mut conn = Connection::new(stream);

        // Send the message to the remote node.
        let _ = conn.write_frame(&gpk.into_frame()).await;
        info!(
            "Having sent a `Get Ecdsa Public Key` command to node {}.",
            remote_id
        );

        // Waiting for the responce.
        match conn.read_frame().await {
            Ok(Some(frame)) => match frame {
                Frame::Simple(data) => {
                    println!("Get the ecdsa public key of node {}.", remote_id);
                    info!("Get the ecdsa public key of node {}.", remote_id);
                    Ok(serde_json::from_str(&data).unwrap())
                }
                Frame::Error(e) => Err(e.into()),
                _ => Err("Frame responsed by peer not right.".into()),
            },
            _ => Err("Connection got problems.".into()),
        }
    }

    pub async fn send_rsa_encrypted_msg(
        self: Arc<Self>,
        remote_id: u64,
        name: &str,
        data: &[u8],
    ) -> crate::Result<()> {
        // Get the remote node's address in neighbor's map, if can't, return an Err.
        let remote_addr = match self.neighbors.get(&remote_id) {
            Some(entry) => entry.value().clone(),
            None => {
                return Err(
                    format!("Can't find the remote node {} in neighbor map.", remote_id).into(),
                );
            }
        };

        // Establish a Tcp connection.
        let mut conn = Connection::new(TcpStream::connect(remote_addr).await?);

        // First try to get the public key of the remote node.
        let remote_public_key = match self.clone().request_rsa_public_key(remote_id).await {
            Ok(key) => key,
            Err(e) => {
                error!(
                    "Errors when trying to get the remote node {}'s public key: {}.",
                    remote_id, e
                );
                return Err(e);
            }
        };

        // Use remote node's public key to encrypt the data.
        let mut rng = rand::thread_rng();
        let enc_data = remote_public_key
            .encrypt(&mut rng, Pkcs1v15Encrypt, data)
            .expect("Failed to encrypt.");

        // Use private key to generate a signature.
        let mut hasher = Sha256::new();
        hasher.update(self.clone().id.to_le_bytes());
        let hashed = hasher.finalize();
        let signature = self
            .rsa_private_key
            .sign(Pkcs1v15Sign::new::<Sha256>(), &hashed)
            .expect("Failed to sign.");

        // Create the message using former created message.
        let msg = SendRsaEncData::new(self.clone().id, signature, name, enc_data);

        let _ = conn.write_frame(&msg.into_frame()).await?;

        Ok(())
    }

    /// Using ecdsa encryption method to generate a signature which will be examined by the
    /// receiver. The payload data is encrypted by AES encryption.
    pub async fn send_ecdsa_signed_msg(
        self: Arc<Self>,
        remote_id: u64,
        name: &str,
        data: &[u8],
    ) -> crate::Result<()> {
        // Get the remote node's address in neighbor's map, if can't, return an Err.
        let remote_addr = match self.neighbors.get(&remote_id) {
            Some(entry) => entry.value().clone(),
            None => {
                error!("Can't find the remote node {} in neighbor map.", remote_id);
                return Err(
                    format!("Can't find the remote node {} in neighbor map.", remote_id).into(),
                );
            }
        };

        // Establish Tcp connection.
        let mut conn = Connection::new(TcpStream::connect(remote_addr).await?);

        // Use ECDSA private key to generate a signature.
        let mut signer = Signer::new_without_digest(&self.ecdsa_private_key).unwrap();
        signer.update(data).unwrap();
        let signature = signer.sign_to_vec().unwrap();

        // Use AES to encrypt payload data.
        let data = crypto::encrypt_message(&data);

        // Create a message.
        let msg = SendEcdsaEncData::new(self.id, signature, name, data.to_vec());

        // Send out the message.
        let _ = conn.write_frame(&msg.into_frame()).await?;
        info!("Sent out a ecdsa message to node {}.", remote_id);

        Ok(())
    }

    /// Handle message from 'dst'.
    ///
    /// The `UserDefined` type can be used to carry serialized data, which can be later used
    /// as user defined communication.
    pub async fn handle_connection(self: Arc<Self>, mut dst: Connection) -> crate::Result<()> {
        // Here use 'if let' because I don't want to raise an error because of an empty message.
        if let Some(frame) = dst.read_frame().await.unwrap() {
            match Message::from_frame(frame).unwrap() {
                Message::HeartBeat(heart_beat) => heart_beat.handle(self.clone()).await?,

                // This message is for establishing a connection with this node.
                Message::Establish(establish) => establish.handle(self.clone()).await?,

                // This message is for recovering nodes to use.
                Message::Restablish(restablish) => restablish.handle(self.clone()).await?,

                // This message is for requesting another node's Rsa public key.
                Message::GetRsaPublicKey(getrsapublickey) => {
                    getrsapublickey.handle(self.clone(), dst).await?
                }

                // This message is for requesting another node's Rsa public key.
                Message::GetEcdsaPublicKey(getecdsapublickey) => {
                    getecdsapublickey.handle(self.clone(), dst).await?
                }

                Message::SendRsaEncData(sendrsaencdata) => {
                    sendrsaencdata.handle(self.clone()).await?
                }

                Message::SendEcdsaEncData(sendecdsaencdata) => {
                    sendecdsaencdata.handle(self.clone()).await?
                }

                // If this is other type packeged in this message.
                Message::UserDefined(userdefined) => {
                    println!("Handling user defined message: {:?}", userdefined);
                    let handlers = self.user_handlers.read().await;
                    if let Some(handler) = handlers.get(&userdefined.name) {
                        println!("received user defined msg {}", &userdefined.name);
                        handler.handle(userdefined, self.clone()).await?;
                    } else {
                        println!(
                            "No handler registered for message type: {}.",
                            userdefined.name
                        );
                        error!(
                            "No handler registered for message type: {}",
                            userdefined.name
                        );
                    }
                }

                _ => {
                    debug!("Received a message that is not known.");
                    return Err("mismatching message.".into());
                }
            }
        };
        return Ok(());
    }

    /// The boot function of a node.
    pub async fn run(self: Arc<Self>) {
        let node = self.clone();
        tokio::task::spawn(node.start_listening());

        let node = self.clone();
        tokio::task::spawn(node.start_sending_heartbeat());

        let node = self.clone();
        tokio::task::spawn(node.check_neighbors_live());
    }

    /// Get the value associated with a key.
    pub async fn _get_neighbor(&self, key: u64) -> Option<SocketAddr> {
        self.neighbors.get(&key).map(|entry| entry.clone())
    }

    /// Add a new neighbor to the current node.
    ///
    /// If a value already assosiated with the key, it's removed.
    /// The 'start_sending_heartbeat' methods will establish Tcp with the neighbor and add it
    /// to the connection pool the next 100ms.
    pub async fn _set_neighbor(neighbors: Arc<DashMap<u64, SocketAddr>>, id: u64, addr: &str) {
        neighbors.insert(id, addr.parse().unwrap());
    }

    /// Create a 'Backup' and save it to flie "backup.json" for node recovery.
    ///
    /// The backup store a dashmap of neighbor and log.
    /// NOTE: There will be more fileds in the Backup and Node.
    pub(crate) async fn _backup_once(self: Arc<Self>) -> std::io::Result<()> {
        let neighbors = (*self.neighbors).clone();
        let log = self.db.get_log_snapshot().await;
        let backup = Backup::new(neighbors, log);

        debug!("Backup once.");

        backup.save_to_file().into()
    }

    /// Update a node's activity.
    ///
    /// Typically used when receiving a heartbeat message.
    /// TODO: Put the Err message in error log instead of panicing the program.
    pub(crate) fn update_activity(&self, id: u64) -> Result<(), crate::Error> {
        if let Some(mut value) = self.neighbors_activity.get_mut(&id) {
            *value = true;
            Ok(())
        } else {
            error!("Can't find node {}", id);
            Err(format!("Can't find node {}", id).into())
        }
    }

    /// Send a 'HeartBeat' message to dst, the id is sender's id.
    async fn send_heartbeat<T: ToSocketAddrs>(
        id: u64,
        addr: String,
        remote_addr: T,
    ) -> io::Result<()> {
        // Generate a signature using the id and secret key in config.rs
        let sig = crypto::generate_signature(&id.to_le_bytes());

        let hb = HeartBeat::new(id, addr, &sig);

        // Initiate a new Tcp connection and send the message.
        let stream = TcpStream::connect(remote_addr).await?;
        let mut dst = Connection::new(stream);

        dst.write_frame(&hb.into_frame()).await?;

        Ok(())
    }

    /// For every 1s, check all neighbor nodes' activity, and set them to false for next check.
    ///
    /// If any node is not active, cuts off the TCP connection and returns an error.
    async fn check_neighbors_live(self: Arc<Self>) -> Result<(), crate::Error> {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            for mut entry in self.neighbors_activity.iter_mut() {
                if *entry.value() == false {
                    // If neighbor's activity is false then drop the connection.
                    error!("Node {} is dnown", &entry.key());
                    println!("Node {} is dnown", &entry.key());
                } else {
                    *entry.value_mut() = false;
                }
            }
        }
    }

    /// Start listening to 'addr' as node 'id'.
    async fn start_listening(self: Arc<Self>) -> crate::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Node {} running on {}", self.id, self.addr);
        println!("Node {} running on {}", self.id, self.addr);

        // Keep recieving connections and dispatching it to other working tasks.
        loop {
            let (socket, _) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    continue;
                }
            };
            socket.set_linger(Some(Duration::from_secs(60)))?;

            let connection = Connection::new(socket);
            let node = self.clone();

            tokio::task::spawn(node.handle_connection(connection));
        }
    }

    /// For every 100ms, send a 'HeartBeat' message to every neighbor in the connection pool.
    /// TODO: Trying to establish connection to a dead node is resourse consuming, because this
    /// operation will repeat every 100ms. need a new 'Restablish' message to do this.
    async fn start_sending_heartbeat(self: Arc<Self>) -> crate::Result<()> {
        let mut interval = time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;

            // Collect inactive nodes in a Vec.
            let neighbors_to_send: Vec<(u64, String)> = self
                .neighbors
                .iter()
                .filter_map(|entry| {
                    // Check the if node is alive.
                    if *self.neighbors_activity.get(entry.key()).unwrap().value() {
                        Some((*entry.key(), entry.value().to_string()))
                    } else {
                        None
                    }
                })
                .collect();

            // Starting sending heartbeat.
            for (id, addr) in neighbors_to_send {
                // Only send heartbeat to neighbors that are alive.
                if let Err(e) =
                    Self::send_heartbeat(self.id, self.addr.to_string(), addr.clone()).await
                {
                    println!("Failed to send heartbeat to {}: {}", id, e);
                    error!("Failed to send heartbeat to {}: {}", id, e)
                }
            }
        }
    }
}

#[async_trait]
pub trait UserMessageHandler {
    async fn handle(&self, message: UserDefined, connection: Arc<Node>)
        -> Result<(), crate::Error>;
}
