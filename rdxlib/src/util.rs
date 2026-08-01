use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use crate::messages::Message;

pub struct MessageSender<Action, RuntimeAction> {
	tx: Sender<Message<Action, RuntimeAction>>,
}

impl<Action, RuntimeAction> MessageSender<Action, RuntimeAction> {
	pub fn new(tx: Sender<Message<Action, RuntimeAction>>) -> Self {
		MessageSender { tx }
	}

	pub fn send_message(&self, message: impl Into<Message<Action, RuntimeAction>>) -> Result<(), SendError<Message<Action, RuntimeAction>>> {
		self.tx.send(message.into())
	}
}

impl<Action, RuntimeAction> Clone for MessageSender<Action, RuntimeAction> {
	fn clone(&self) -> Self {
		MessageSender { tx: self.tx.clone() }
	}
}