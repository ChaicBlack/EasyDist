use EasyDist::Node;

use std::env;
use std::sync::Arc;

use env_logger;
use tokio;

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

    // Run the node.
    node.run().await;

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c");
    println!("Shutting down...");

    Ok(())
}
