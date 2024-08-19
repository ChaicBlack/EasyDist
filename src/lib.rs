//! A non-blocking IO and easy-to-use Rust crate for constructing simple distributed system preject. The crate contains several building blocks that can be use to construct a distributed system program.
//! The establish methods in the `node` module can establish connection between two nodes. Once established the connection, the nodes add each other into neighbors list and continuesly send `heartbeat` message to each other to check for activity.
//! You could defined the logic on their your based on the `senduserdefinedmsg` methods easily. You just need to register a handler methods with a particular name and you can define specific logic of handling logic with this specific name. Of course you can choose `sendecdsaencmsg` or `sendrsaencmsg` to achive the same function with a safer message communication.
//!
//! # Example
//! This is an example code for initiating the node instance. This example takes 2 arguments, the
//! first is the id of node you choose, the second is the local network port you choose. For
//! example, 0 127.0.0.1:8080 means you choose to the id as 0 for the node and bind the node to
//! port 8080 on this machine. This is also the default input(which means you don't input any
//! argument) of this code.
//! ````
//! #[tokio::main()]
//! async fn main() -> std::io::Result<()> {
//!     // The first argument is the id of node.
//!     // The second is the addr of node.
//!     let args: Vec<String> = env::args().collect();
//!
//!     // Initiate the env_logger.
//!    env_logger::init();
//!
//!    // If there is no argument, use the configuration in config.rs
//!    let node = if args.len() > 2 {
//!        Arc::new(Node::new(
//!            args[1]
//!                .parse::<u64>()
//!                .expect("The first argument must can be converted to u64."),
//!            &args[2],
//!        ))
//!    } else {
//!        Arc::new(Node::default())
//!    };
//!
//!    // Run the node.
//!    node.run().await;
//!
//!    tokio::signal::ctrl_c()
//!        .await
//!        .expect("Failed to listen for ctrl_c");
//!
//!    Ok(())
//! }
//! ````
//! This is an example using handler.
//! ````
//! #[derive(Serialize, Deserialize, Debug)]
//! struct Point {
//!     id: u64,
//!     x: i32,
//!     y: i32,
//! }
//!
//! pub struct CustomHandler;
//!
//! #[async_trait]
//! impl UserMessageHandler for CustomHandler {
//!     async fn handle(&self, message: UserDefined, connection: Arc<Node>) -> EasyDist::Result<()> {
//!         let point: Point = serde_json::from_slice(&message.get_data()).unwrap();
//!         println!("ping received: {:?}.", point);
//!         let data = vec![];
//!         connection
//!             .send_ecdsa_signed_msg(point.id, "OK", &data)
//!             .await?;
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main()]
//! async fn main() -> std::io::Result<()> {
//!     // The first argument is the id of node.
//!     // The second is the addr of node.
//!     let args: Vec<String> = env::args().collect();
//!
//!     // Initiate the env_logger.
//!     env_logger::init();
//!
//!     // If there is no argument, use the configuration in config.rs
//!     let node = if args.len() > 2 {
//!         Arc::new(Node::new(
//!             args[1]
//!                 .parse::<u64>()
//!                 .expect("The first argument must can be converted to u64."),
//!             &args[2],
//!         ))
//!     } else {
//!         Arc::new(Node::default())
//!     };
//!
//!     let handle = Arc::new(CustomHandler {});
//!     let _ = node.clone().register_user_handler("ping", handle).await;
//!
//!     // Run the node.
//!     node.run().await;
//!
//!     tokio::signal::ctrl_c()
//!         .await
//!         .expect("Failed to listen for ctrl_c");
//!     println!("Shutting down...");
//!
//!     Ok(())
//! }
//! ````

mod config;

mod conn;
pub use conn::Connection;

pub mod msg;
pub use msg::Message;

pub mod frame;
pub use frame::Frame;

mod parse;
use parse::{Parse, ParseError};

pub mod db;
use db::Db;

pub mod asyncEventQueue;

pub mod crypto;

mod snapshot;

pub mod wal;

mod state;
pub use state::State;

mod server;
pub use server::server;
mod client;
pub use client::client;
pub mod node;
pub use node::Node;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
