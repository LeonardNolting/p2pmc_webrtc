use std::collections::HashMap;
use tokio::sync::{oneshot, Mutex};

// ResponseManager handles matching responses to their requests
#[derive(Debug)]
pub(crate) struct ResponseManager<I, T> {
    pending_requests: Mutex<HashMap<I, oneshot::Sender<T>>>
}

impl<I, T> ResponseManager<I, T> where I: Eq + std::hash::Hash {
    pub(crate) fn new() -> Self {
        Self {
            pending_requests: Mutex::new(HashMap::new())
        }
    }

    // Register a new request and get a receiver to await the response
    pub(crate) async fn wait_for_response(&self, id: I) -> oneshot::Receiver<T> {
        let (sender, receiver) = oneshot::channel();
        self.pending_requests.lock().await.insert(id, sender);
        receiver
    }

    // Called from the callback to deliver the response
    pub(crate) async fn handle_response(&self, id: I, response: T) {
        if let Some(sender) = self.pending_requests.lock().await.remove(&id) {
            let _ = sender.send(response);
        }
    }
}