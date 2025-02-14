use std::collections::HashMap;
use tokio::sync::{oneshot, Mutex};
use std::sync::Arc;

// ResponseManager handles matching responses to their requests
pub(crate) struct ResponseManager<T> {
    pending_requests: Mutex<HashMap<u32, oneshot::Sender<T>>>
}

impl<T> ResponseManager<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            pending_requests: Mutex::new(HashMap::new())
        })
    }

    // Register a new request and get a receiver to await the response
    pub(crate) async fn wait_for_response(&self, request_id: u32) -> oneshot::Receiver<T> {
        let (sender, receiver) = oneshot::channel();
        self.pending_requests.lock().await.insert(request_id, sender);
        receiver
    }

    // Called from the callback to deliver the response
    pub(crate) async fn handle_response(&self, request_id: u32, response: T) {
        if let Some(sender) = self.pending_requests.lock().await.remove(&request_id) {
            let _ = sender.send(response);
        }
    }
}