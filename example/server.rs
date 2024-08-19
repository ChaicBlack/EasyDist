use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::error::Error;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 绑定到本地端口
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server running on 127.0.0.1:8080");

    loop {
        // 接受一个新的客户端连接
        let (stream, addr) = listener.accept().await?;
        println!("New client: {:?}", addr);

        // 处理客户端连接
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream).await {
                println!("Error handling client: {:?}", e);
            }
        });
    }
}

async fn handle_client(stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut framed = Framed::new(stream, LinesCodec::new());

    while let Some(Ok(line)) = framed.next().await {
        println!("Received: {}", line);

        // 回显收到的消息
        framed.send(format!("Echo: {}", line)).await?;
    }

    Ok(())
}
