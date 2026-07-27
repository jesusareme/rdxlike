use std::fmt::{Display, Formatter};
use enumset::EnumSet;

#[derive(Debug)]
pub enum SubscriberError {
	MissingState,
	UnableToNotifySubscriber,
}
impl Display for SubscriberError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			SubscriberError::MissingState => write!(f, "Missing required state."),
			SubscriberError::UnableToNotifySubscriber => {
				write!(f, "Unable to notify subscriber.")
			}
		}
	}
}

pub trait Subscriber {
	type State;
	type Flag: enumset::EnumSetType;
	
	fn notify(&mut self, new_state: &Self::State) -> Result<(), SubscriberError>;
	fn is_active(&self) -> bool;
	fn interested_in(&self, offered: &EnumSet<Self::Flag>) -> bool;
}