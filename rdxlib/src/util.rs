use std::sync::mpsc::Sender;
use crate::Client;
use crate::error::InitError;
use crate::messages::{Message, Operation};
use crate::messages::Operation::{Run, Stop};

pub trait MessageSend: Clone + Send + 'static {
    type Message: Send + 'static;

    /// # Errors
    /// Will return `Err` if the message destinatary is no longer available.
    fn send_message(
        &self,
        message: impl Into<Self::Message>,
    ) -> Result<(), InitError>;
}

// TODO! docs
pub struct RuntimeHandle<C: Client> {
    sender: Sender<Operation<Message<C::Action, C::RuntimeAction>>>,
}

impl<C: Client> RuntimeHandle<C>
{
    pub fn create_sender(&self) -> MessageSender<Message<C::Action, C::RuntimeAction>> {
        MessageSender {
            tx: self.sender.clone(),
        }
    }
    
    pub fn cancel(&self) -> Result<(), InitError> {
        self.sender.send(Stop).map_err(|_| InitError::NoLongerRunning)
    }

    pub(super) fn from_sender(sender: Sender<Operation<Message<C::Action, C::RuntimeAction>>>) -> Self {
        RuntimeHandle {
            sender,
        }
    }
}

impl<C: Client> Drop for RuntimeHandle<C> {
    fn drop(&mut self) {
        _ = self.sender.send(Stop);
    }
}

pub struct MessageSender<M> {
    tx: Sender<Operation<M>>,
}

impl<M> MessageSender<M> {
    pub(super) fn from_sender(sender: Sender<Operation<M>>) -> Self {
        MessageSender {
            tx: sender,
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

    fn send_message(&self, message: impl Into<M>) -> Result<(), InitError> {
        self.tx.send(Run(message.into())).map_err(|_| InitError::NoLongerRunning)?;
        Ok(())
    }
}