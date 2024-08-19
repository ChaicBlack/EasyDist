use futures::sink::SinkExt;
use futures::stream::StreamExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 连接到服务器
    let stream = TcpStream::connect("127.0.0.1:8080").await?;

    // 将TCP流包装为Framed流，使用LinesCodec
    let mut framed = Framed::new(stream, LinesCodec::new());

    // 发送一行数据
    framed.send("Hello, world!").await?;

    // 接收服务器的响应
    if let Some(Ok(line)) = framed.next().await {
        println!("Received: {}", line);
    }

    Ok(())
}
