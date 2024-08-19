use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use EasyDist::msg::UserDefined;
use EasyDist::node::UserMessageHandler;
use EasyDist::Node;

use std::env;
use std::sync::Arc;

use env_logger;
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
        let point: Point = serde_json::from_slice(&message.get_data()).unwrap();
        println!("ping received: {:?}.", point);
        let data = vec![];
        connection
            .send_ecdsa_signed_msg(point.id, "OK", &data)
            .await?;
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
    let node = if args.len() > 2 {
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
    let _ = node.clone().register_user_handler("ping", handle).await;

    // Run the node.
    node.run().await;

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c");
    println!("Shutting down...");

    Ok(())
}
