use async_trait::async_trait;
use EasyDist::{msg::UserDefined, node::UserMessageHandler, Node};

use std::env;
use std::sync::Arc;

use env_logger;
use serde::{Deserialize, Serialize};
use tokio;

#[derive(Serialize, Deserialize, Debug)]
struct Point {
    id: u64,
    x: i32,
    y: i32,
}

pub struct CustomHandler;

#[async_trait]
impl UserMessageHandler for CustomHandler {
    async fn handle(&self, message: UserDefined, connection: Arc<Node>) -> EasyDist::Result<()> {
        println!("pong received.");
        Ok(())
    }
}

#[tokio::main()]
async fn main() -> std::io::Result<()> {
    // The first argument is the id of node.
    // The second is the addr of node.
    let args: Vec<String> = env::args().collect();

    // Initiate the env_logger.
    env_logger::init();

    // If there is no argument, use the configuration in config.rs
    let node = if args.len() > 1 {
        Arc::new(Node::new(
            args[1]
                .parse::<u64>()
                .expect("The first argument must can be converted to u64."),
            &args[2],
        ))
    } else {
        Arc::new(Node::default())
    };

    let handle = Arc::new(CustomHandler {});
    let _ = node.clone().register_user_handler("OK", handle).await;

    // Run the node.
    tokio::task::spawn(node.clone().run());

    let _ = node
        .clone()
        .establish_conn(0, "127.0.0.1:8080")
        .await
        .unwrap();

    let point = Point {
        id: node.id,
        x: -1,
        y: -1,
    };
    let data = serde_json::to_vec(&point).unwrap();
    let _ = node.clone().send_rsa_encrypted_msg(0, "ping", &data).await;
    println!("msg has been sent.");

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c");
    println!("Shutting down...");

    Ok(())
}
