//! Small helpers shared across the crate.

use crate::error::RuntimeError;
use crate::messages::Operation::Run;
use crate::messages::Operation;
use std::sync::mpsc::Sender;
use std::sync::{Arc, PoisonError, RwLock};
use uuid::Uuid;

/// The ability to put a message on the runtime queue.
pub trait MessageSend: Clone + Send + 'static {
    type Message: Send + 'static;

    /// # Errors
    /// Will return `RuntimeError` if the message destinatary is no longer available, which means
    /// `Runtime` is no longer accepting messages, it's no longer running
    fn send_message(
        &self,
        message: impl Into<Self::Message>,
    ) -> Result<(), RuntimeError>;
}

/// [`MessageSend`] implementation.
pub struct MessageSender<M> {
    tx: Sender<Operation<M>>,
}

impl<M> MessageSender<M> {
    pub(super) fn new(sender: Sender<Operation<M>>) -> Self {
        MessageSender {
            tx: sender
        }
    }
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

    fn send_message(&self, message: impl Into<M>) -> Result<(), RuntimeError> {
        self.tx.send(Run(message.into())).map_err(|_| RuntimeError::NoLongerRunning)?;
        Ok(())
    }
}

/// Small utility to handle cooperative cancellation of asynchronous tasks.
///
/// This unique handle can notice several instance of the derived construct [`CancellationCheck`]
/// whether cancelling their current task is required, by calling [`Self::cancel`] or
/// just dropping the instance.
#[allow(unused)]
#[derive(Debug)]
pub struct DropCancellation(Arc<RwLock<bool>>, Uuid);

impl DropCancellation {
    pub fn new() -> Self {
        DropCancellation(Arc::new(RwLock::new(false)), Uuid::new_v4())
    }

    /// Creates a [`CancellationCheck`]. They can also be created by cloning any other existent
    /// instance.
    pub fn cancellation_check(&self) -> CancellationCheck {
        CancellationCheck(self.0.clone(), self.1)
    }

    /// Notify [`CancellationCheck`]s the need to cancel the process they are embedded in.
    pub fn cancel(self) { }
}

impl Drop for DropCancellation {
    fn drop(&mut self) {
        let mut guard = self.0.write().unwrap_or_else(PoisonError::into_inner);
        *guard = true;
    }
}

/// Utility to check the need to stop working on the current job after a cancellation
/// order by the parent [`DropCancellation`].
#[allow(unused)]
#[derive(Debug)]
pub struct CancellationCheck(Arc<RwLock<bool>>, Uuid);
impl CancellationCheck {

    /// Returns the cancellation status. Current task is expected to end as soon as possible after
    /// this method returns `true`.
    pub fn is_cancelled(&self) -> bool {
        *self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Utility method to easily check for cancellation by using `?` operator in [`Option`] returning
    /// functions.
    pub fn still_working(&self) -> Option<()> {
        (!self.is_cancelled()).then_some(())
    }
}


impl Clone for CancellationCheck {
    fn clone(&self) -> Self {
        CancellationCheck(self.0.clone(), self.1)
    }
}

impl PartialEq for CancellationCheck {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}

impl PartialEq for DropCancellation {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}
