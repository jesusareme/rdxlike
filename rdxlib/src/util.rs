use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Weak};
use crate::Client;
use crate::error::InitError;
use crate::messages::Message;

pub trait MessageSend: Clone + Send + 'static {
    type Message: Send + 'static;

    /// # Errors
    /// Will return `Err` if the message destinatary is no longer available.
    fn send_message(
        &self,
        message: impl Into<Self::Message>,
    ) -> Result<(), SendError<Self::Message>>;
}

// TODO! docs
pub struct CancellationHandle<C: Client> {
    retained_runtime_sender: Option<Arc<Sender<Message<C::Action, C::RuntimeAction>>>>,
}

impl<C: Client> CancellationHandle<C>
{
    pub fn cancel(&mut self) -> Result<(), InitError> {
        self.retained_runtime_sender.take().map_or_else(|| Err(InitError::NoLongerRunning), |_| Ok(()))
    }

    #[must_use]
    pub(crate) fn new(tx: Arc<Sender<Message<C::Action, C::RuntimeAction>>>) -> Self {
        CancellationHandle { retained_runtime_sender: Some(tx) }
    }
}

pub struct MessageSender<M> {
    tx: Weak<Sender<M>>,
}

impl<M> MessageSender<M> {
    pub(crate) fn new(sender: &Arc<Sender<M>>) -> Self {
        MessageSender {
            tx: Arc::downgrade(sender),
        }
    }
}

impl<M> Clone for MessageSender<M> {
    fn clone(&self) -> Self {
        MessageSender {
            tx: self.tx.clone(),
        }
    }
}

impl<M> MessageSend for MessageSender<M>
where
    M: Send + 'static,
{
    type Message = M;

    fn send_message(&self, message: impl Into<M>) -> Result<(), SendError<M>> {
        match self.tx.upgrade() {
            None => Err(SendError(message.into())),
            Some(s) => {
                s.send(message.into())?;
                Ok(())
            }
        }
    }
}