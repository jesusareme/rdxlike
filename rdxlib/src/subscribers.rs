use crate::Client;
use crate::error::InitError;
use crate::subscribers::ComparableResult::Comparable;
use enumset::EnumSet;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::mpsc::Sender;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;
use tracing::debug;
use uuid::Uuid;

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
impl Error for SubscriberError {}

pub trait Subscriber {
	type State;
	type Flag: enumset::EnumSetType;
	
	fn notify(&mut self, new_state: &Self::State) -> Result<(), SubscriberError>;
	fn is_active(&self) -> bool;
	fn interested_in(&self, offered: &EnumSet<Self::Flag>) -> bool;
}


#[derive(Debug, PartialEq)]
pub enum ComparableResult<T> {
	/// No comparison is possible to any previous state because current state is undefined or
	/// incompatible with the intended subscription
	NothingToCompare,
	/// Comparison is possible to a previous state because we have a well-defined current value
	Comparable(T),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ViewId { id: Uuid }
impl From<Uuid> for ViewId {
	fn from(value: Uuid) -> Self {
		ViewId { id: value}
	}
}
impl Display for ViewId {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.id)
	}
}

pub trait ViewTransformer<C: Client>: Send + 'static {
	type ComparableValue: PartialEq;
	type Slice: Send + 'static;
	type Product: Send + 'static;

	/// Indicates caller whether this subscriber will potentially be notifiable based on rough
	/// estimation expressed by `Dirty` flags.
	fn interested_in(offered: &EnumSet<C::Flag>) -> bool;

	/// Returns a `ComparableResult::Comparable` value used to identify relevant changes in the
	/// model. A new derived state to feed a view will be generated if the comparison is
	/// favorable (previous value returned from this function is different from the one before).
	/// First execution of a `ViewTransformer` will always generate a new state derivation as
	/// long as this functions doesn't return a `ComparableResult::NothingToCompare` result.
	fn comparable(state: &C::State, token: ViewId) -> ComparableResult<Self::ComparableValue>;

	/// Extracts the minimum thread-safe slice from the original state needed to calculate the
	/// final information the view needs. It returns `SubscriberError` if was unable to create the
	/// state slice.
	fn slice(state: &C::State, token: ViewId) -> Result<Self::Slice, SubscriberError>;

	/// Derives the final data needed by the view. This method is executed on its own
	/// thread.
	fn derive(&mut self, slice: Self::Slice) -> Option<Self::Product>;
}

pub trait ViewOutput<V: Send + 'static>: Clone + Send + 'static {
	fn new(capacity: usize) -> Self;
	fn send(&self, v: V) -> bool;
	fn is_active(&self) -> bool;
}

#[allow(unused)]
pub struct OutputSubscriber<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> {
	id: ViewId,
	last: Option<VT::ComparableValue>,
	sender: Sender<Option<VT::Slice>>,
	output: VO,
	thread_handle: JoinHandle<()>,
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> OutputSubscriber<C, VT, VO> {
	pub fn new(id: ViewId, mut transformer: VT, output: VO) -> Result<Self, InitError> {
		let (sender, receiver) = mpsc::channel::<Option<VT::Slice>>();
		let builder = thread::Builder::new().name(id.to_string());
		let output_clone = output.clone();
		let thread_handle = builder.spawn(move || {
			while let Some(slice) = receiver.recv().unwrap() {
				if output_clone.is_active() {
					if let Some(product) = transformer.derive(slice) {
						output_clone.send(product);
					}
				} else {
					break;
				}
			}
			debug!("dropping thread for output subscriber: {}", id)
		})?;

		Ok(OutputSubscriber {
			id,
			last: None,
			sender,
			output,
			thread_handle,
		})
	}

	pub fn is_finished(&self) -> bool {
		self.thread_handle.is_finished()
	}
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> Subscriber for OutputSubscriber<C, VT, VO> {
	type State = C::State;
	type Flag = C::Flag;

	fn notify(&mut self, new_state: &C::State) -> Result<(), SubscriberError> {
		let Comparable(current_comparable) = VT::comparable(new_state, self.id) else {
			return Ok(());
		};

		let current_comparable = Some(current_comparable);

		if self.last != current_comparable {
			self.last = current_comparable;
			let slice = VT::slice(new_state, self.id)?;
			self.sender
				.send(Some(slice))
				.map_err(|_| SubscriberError::UnableToNotifySubscriber)?;
		}
		Ok(())
	}

	fn is_active(&self) -> bool {
		self.output.is_active()
	}

	fn interested_in(&self, offered: &EnumSet<C::Flag>) -> bool {
		VT::interested_in(offered)
	}
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> Drop for OutputSubscriber<C, VT, VO> {
	fn drop(&mut self) {
		_ = self.sender.send(None)
	}
}
