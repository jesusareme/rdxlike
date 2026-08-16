use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;

pub trait MessageSend: Clone + Send + 'static {
	type Message: Send + 'static;

	/// # Errors
	/// Will return `Err` if the message destinatary is no longer available.
	fn send_message(&self, message: impl Into<Self::Message>) -> Result<(), SendError<Self::Message>>;
}

pub struct MessageSender<M> {
	tx: Sender<M>,
}

impl<M> MessageSender<M> {
	#[must_use]
	pub fn new(tx: Sender<M>) -> Self {
		MessageSender { tx }
	}
}

impl<M> MessageSend for MessageSender<M>
where
	M: Send + 'static,
{
	type Message = M;

	fn send_message(&self, message: impl Into<M>) -> Result<(), SendError<M>> {
		self.tx.send(message.into())
	}
}

impl<M> Clone for MessageSender<M> {
	fn clone(&self) -> Self {
		MessageSender { tx: self.tx.clone() }
	}
}