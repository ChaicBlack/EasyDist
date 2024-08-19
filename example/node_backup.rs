use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use crate::crypto;
use crate::msg::{Establish, HeartBeat};
use crate::{config, Connection, Message};

use dashmap::DashMap;
use env_logger;
use log::error;
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
};

pub struct Node {
    id: u64,
    addr: SocketAddr,

    /// Handle the neighboring nodes' address.
    ///
    /// The key is the id of neighbor, the value is a std::net::SocketAddr of neighbor.
    neighbors: Arc<DashMap<u64, SocketAddr>>,
}

impl Node {
    pub fn new() -> Node {
        env_logger::init();
        Node {
            id: config::ID,
            addr: config::ADDR.parse().unwrap(),
            neighbors: Arc::new(DashMap::new()),
        }
    }

    pub fn new_clone() -> Node {
        env_logger::init();
        Node {
            id: config::ID,
            addr: config::ADDR2.parse().unwrap(),
            neighbors: Arc::new(DashMap::new()),
        }
    }

    /// Send a 'HeartBeat' message to dst, the id is sender's id.
    async fn send_heartbeat(id: u64, addr: String, dst: &mut Connection) -> io::Result<()> {
        let sig = crypto::generate_signature(&id.to_le_bytes());

        let hb = HeartBeat::new(id, addr, &sig);

        dst.write_frame(&hb.into_frame()).await?;

        Ok(())
    }

    /// Get the value associated with a key.
    pub async fn _get_neighbor(&self, key: u64) -> Option<SocketAddr> {
        self.neighbors.get(&key).map(|entry| entry.clone())
    }

    /// Establish a connection with a remote node. Then add the remote node to local neighbor map,
    /// connection pool and turn the activity into true (after adding to neighbor map, local node
    /// will send heartbeat since next 100ms).
    ///
    /// NOTE: User need to know the id and addr of remote node. Usually mannually written,
    /// TODO: There is no denial option for reciever. Later I shall consider. A lot of work.
    pub async fn establish_conn(&self, remote_id: u64, remote_addr: &str) -> crate::Result<()> {
        // Generate the signature for message creation.
        let sig = crypto::generate_signature(&self.id.to_le_bytes());

        // Create the 'Establish' message.
        let es = Establish::new(self.id, self.addr.clone().to_string(), &sig);

        // Connect to remote node and send message.
        let stream =
            TcpStream::connect::<SocketAddr>(remote_addr.to_string().parse().unwrap()).await?;
        let mut conn = Connection::new(stream);
        conn.write_frame(&es.into_frame()).await?;

        tokio::spawn(Node::handle_connection(conn, self.neighbors.clone()));

        // Add the remote mode to local neighbors map.
        self.neighbors
            .clone()
            .insert(remote_id, remote_addr.to_string().parse().unwrap());

        Ok(())
    }

    /// Handle message from 'dst'.
    ///
    /// Verify every incomming message.
    ///
    /// Update neighbors' acitivity after receiving a 'HeartBeat'.
    async fn handle_connection(
        mut dst: Connection,
        neighbors: Arc<DashMap<u64, SocketAddr>>,
    ) -> Result<(), crate::Error> {
        // This mpsc channel is for resetting timer.
        let (reset_tx, mut reset_rx) = mpsc::channel::<()>(1);

        // This mpsc channel is for notifying time expiration.
        let (timeout_tx, mut timeout_rx) = mpsc::channel::<()>(1);

        // Spawn a task to create the timer.
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = time::sleep(Duration::from_secs(1)) => {
                        // If 1s duration was passed, notify timeout.
                        let _ = timeout_tx.send(()).await;
                        break;
                    }
                    _ = reset_rx.recv() => {
                        // Upon receiving reset notification, enter the next loop.
                    }
                }
            }
        });

        loop {
            tokio::select! {
                frame_result = dst.read_frame() => {
                    match frame_result.unwrap(){
                        Some(frame) => {
                            match Message::from_frame(frame)? {
                                Message::HeartBeat(heart_beat) => {
                                    let id = heart_beat.clone().get_id();
                                    println!("Received hb from Node {}", id);

                                    // Verify if the sender have the same 'SECRET_KEY'.
                                    /*if crypto::verify_signature(&id.to_le_bytes(), &heart_beat.clone().get_sig()) {
                                        return Err("Wrong signature.".into());
                                    }*/

                                    let _ = reset_tx.send(()).await;

                                    return Ok(());
                                }

                                Message::Establish(establish) => {
                                    let id = establish.clone().get_id();
                                    println!("Received establish from Node {}", id);

                                    // Verify if the sender have the same 'SECRET_KEY'.
                                    /*if crypto::verify_signature(&id.to_le_bytes(), &establish.clone().get_sig()) {
                                        let err_msg = "Wrong signature.";
                                        error!("{}", err_msg);
                                        return Err(err_msg.into());
                                    }*/

                                    neighbors.insert(id, establish.clone().get_addr().parse()?);

                                    let _ = Node::send_heartbeat(id, establish.clone().get_addr(), &mut dst).await;

                                    let _ = reset_tx.send(()).await;
                                }

                                _ => {
                                    let err_msg = "mismatching message.";
                                    error!("{}", err_msg);
                                    return Err(err_msg.into());
                                }
                            };
                        },
                        None => {
                            continue;
                        },
                    };

                }
                _ = timeout_rx.recv() => {
                        return Ok(());
                }
            }
        }
    }

    /// Start listening to 'addr' as node 'id'.
    async fn start_listening(
        id: u64,
        addr: SocketAddr,
        neighbors: Arc<DashMap<u64, SocketAddr>>,
    ) -> crate::Result<()> {
        let listener = TcpListener::bind(addr).await?;

        println!("Node {} running on {}", id, addr);

        // Keep recieving connections and dispatching it to other working tasks.
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let neighbors = neighbors.clone();

                    tokio::task::spawn(async move {
                        let con = Connection::new(socket);

                        let _ = Node::handle_connection(con, neighbors).await;
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {:?}", e);
                }
            }
        }
    }

    /// The boot function of a node.
    pub async fn run(&self) {
        let neighbors = self.neighbors.clone();

        tokio::task::spawn(Node::start_listening(self.id, self.addr, neighbors));
    }

    pub async fn run_clone(&self) {
        let neighbors = self.neighbors.clone();

        tokio::task::spawn(Node::start_listening(self.id, self.addr, neighbors));

        let _ = Self::establish_conn(&self, 0, "127.0.0.1:8080").await;
    }
}
