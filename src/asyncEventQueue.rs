use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task;

/// Used for user defined task and structure.
#[derive(Clone)]
pub struct AsyncEventQueue {
    sender: mpsc::Sender<Box<dyn Any + Send>>,
    handlers: Arc<
        Mutex<
            HashMap<TypeId, Arc<dyn Fn(Box<dyn Any + Send>) -> task::JoinHandle<()> + Send + Sync>>,
        >,
    >,
}

impl AsyncEventQueue {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let queue = AsyncEventQueue {
            sender,
            handlers: Arc::new(Mutex::new(HashMap::new())),
        };

        queue.start(receiver);
        queue
    }

    pub fn reg_handler<F, T, Fut>(&mut self, handler: F)
    where
        F: Fn(T) -> Fut + Send + Sync + 'static,
        T: Any + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let type_id = TypeId::of::<T>();
        let handler = Arc::new(move |event: Box<dyn Any + Send>| {
            if let Ok(event) = event.downcast::<T>() {
                let fut = handler(*event);
                tokio::spawn(fut)
            } else {
                panic!("Type mismatch in handler");
            }
        });

        self.handlers.lock().unwrap().insert(type_id, handler);
    }

    pub async fn send<T: Any + Send + 'static>(&self, event: T) {
        self.sender.send(Box::new(event)).await.unwrap();
    }

    fn start(&self, mut receiver: mpsc::Receiver<Box<dyn Any + Send>>) {
        let handlers = Arc::clone(&self.handlers);

        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                let type_id = (&*event).type_id();
                if let Some(handler) = handlers.lock().unwrap().get(&type_id) {
                    let handler = handler.clone();

                    tokio::task::spawn(async move {
                        handler(event);
                    });
                } else {
                    eprintln!("No handler found for type {:?}", type_id);
                }
            }
        });
    }
}

/*

handle string
#[tokio::main]
async fn main() {
    let mut event_queue = AsyncEventQueue::new();

    event_queue.reg_handler(|event: String| async move {
        println!("String Handler received: {}", event);
    });

    event_queue.send("Event 1".to_string()).await;
    event_queue.send("Event 2".to_string()).await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}

handle integer
#[tokio::main]
async fn main() {
    let mut event_queue = AsyncEventQueue::new();

    event_queue.reg_handler(|event: i32| async move {
        println!("Integer Handler received: {}", event);
    });

    event_queue.send(1).await;
    event_queue.send(2).await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}

handle user defined structure
#[derive(Debug)]
struct MyEvent {
    id: u32,
    description: String,
}

#[tokio::main]
async fn main() {
    let mut event_queue = AsyncEventQueue::new();

    event_queue.reg_handler(|event: MyEvent| async move {
        println!("MyEvent Handler received: {:?}", event);
    });

    event_queue.send(MyEvent { id: 1, description: "Event 1".to_string() }).await;
    event_queue.send(MyEvent { id: 2, description: "Event 2".to_string() }).await;

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}
*/
