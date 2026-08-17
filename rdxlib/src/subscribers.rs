//! How views observe the model without touching it.
//!
//! A subscriber is asked, in order, whether it is still alive, whether it cares about what
//! changed, and only then is it handed the new state. [`OutputSubscriber`] is the ready-made
//! implementation: it compares, slices state on the runtime thread and derives the view's
//! data on a thread of its own.
//!
//! TODO: a diagram or short walkthrough of one notification, end to end.

use crate::Client;
use crate::error::InitError;
pub use crate::error::SubscriberError;
use crate::primitives::{OneSlotSender, one_slot_channel};
use crate::subscribers::ComparableResult::Comparable;
use enumset::EnumSet;
use std::fmt::{Debug, Display, Formatter};
use std::thread;
use std::thread::JoinHandle;
use tracing::debug;
use uuid::Uuid;

/// Something the runtime notifies when the model changes.
///
/// Every method runs on the runtime thread, in between messages, so none of them should
/// block.
///
/// TODO: point implementors at [`OutputSubscriber`] as the normal choice.
pub trait Subscriber {
    /// The client state this subscriber reads from.
    type State;

    /// The client's dirty flags.
    type Flag: enumset::EnumSetType;

    /// Offers the new state, already filtered by [`Subscriber::interested_in`].
    ///
    /// Deciding the change is irrelevant after a closer look is normal, and returning
    /// `Ok(())` without doing anything is the way to say so.
    ///
    /// # Errors
    /// Will return `Err` if Subscriber could not be notified of a relevant change.
    fn notify(&mut self, new_state: &Self::State) -> Result<(), SubscriberError>;

    /// Whether this subscriber is still worth keeping.
    ///
    /// Answering `false` gets it dropped from the runtime before any other check.
    fn is_active(&self) -> bool;

    /// Whether the flags dirtied by the last cascade could matter to this subscriber.
    ///
    /// A cheap filter: answering `true` only earns a call to [`Subscriber::notify`].
    fn interested_in(&self, offered: &EnumSet<Self::Flag>) -> bool;
}

/// The answer to "can this state be compared against the previous one?".
#[derive(Debug, PartialEq)]
pub enum ComparableResult<T> {
    /// No comparison is possible to any previous state because current state is undefined or
    /// incompatible with the intended subscription
    NothingToCompare,
    /// Comparison is possible to a previous state because we have a well-defined current value
    Comparable(T),
}

/// Identity of a single view subscription.
///
/// Passed to the transformer so one implementation can serve several views, each looking
/// at its own slice of the model.
///
/// TODO: say who mints these and how a client ties one to its view.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ViewId {
    id: Uuid,
}
impl From<Uuid> for ViewId {
    fn from(value: Uuid) -> Self {
        ViewId { id: value }
    }
}
impl Display for ViewId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// The client's recipe for turning model state into what one view displays.
///
/// The work is split in three so that as little as possible happens on the runtime thread:
/// [`ViewTransformer::interested_in`] and [`ViewTransformer::comparable`] are cheap checks,
/// [`ViewTransformer::slice`] copies out the minimum the view needs, and
/// [`ViewTransformer::derive`] does the real work elsewhere.
///
/// TODO: worked example of a transformer for a list view.
pub trait ViewTransformer<C: Client>: Send + 'static {
    /// Cheap stand-in for "the view's input changed", compared between notifications.
    type ComparableValue: PartialEq;

    /// The self-contained piece of state handed to the worker thread.
    ///
    /// TODO: note why this must not borrow from the model.
    type Slice: Send + 'static;

    /// The finished data the view consumes.
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
    /// # Errors
    /// Will return `Err` if it could not get a meaningful slice of state from which derive a view product.
    fn slice(state: &C::State, token: ViewId) -> Result<Self::Slice, SubscriberError>;

    /// Derives the final data needed by the view. This method is executed on its own
    /// thread.
    fn derive(&mut self, slice: Self::Slice) -> Option<Self::Product>;
}

/// The channel a derived product travels through to reach the actual view.
///
/// Implemented on the platform side (UI framework, FFI bridge, test double).
///
/// TODO: describe the expected buffering and back-pressure behaviour.
pub trait ViewOutput<V: Send + 'static>: Clone + Send + 'static {
    /// Creates the output with room for `capacity` pending products.
    ///
    /// TODO: say what should happen when that capacity is exceeded.
    fn new(capacity: usize) -> Self;

    /// Delivers a product, answering whether it was accepted.
    fn send(&self, v: V) -> bool;

    /// Whether the view on the other end is still listening.
    ///
    /// Once this turns `false` the subscriber is dropped by the runtime.
    fn is_active(&self) -> bool;
}

/// The general-purpose [`Subscriber`]: compares on the runtime thread, derives on its own.
///
/// Each instance owns a worker thread that lives until the output goes inactive or the
/// slice channel closes. Slices are sent through a one-slot channel, so a worker that falls
/// behind picks up the latest state and skips whatever it missed.
///
/// TODO: note the cost of one thread per view, if that matters at scale.
#[allow(unused)]
pub struct OutputSubscriber<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> {
    id: ViewId,
    last: Option<VT::ComparableValue>,
    sender: OneSlotSender<VT::Slice>,
    output: VO,
    thread_handle: JoinHandle<()>,
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> OutputSubscriber<C, VT, VO> {
    /// Spawns the worker thread for this view and returns the subscriber that feeds it.
    ///
    /// The thread is named after `id`, which makes it easy to spot while debugging.
    ///
    /// # Errors
    /// Will return `Err` if an OS level error was produced while spawning a worker thread.
    pub fn new(id: ViewId, mut transformer: VT, output: VO) -> Result<Self, InitError> {
        let (sender, receiver) = one_slot_channel::<VT::Slice>();
        let builder = thread::Builder::new().name(id.to_string());
        let output_clone = output.clone();
        let thread_handle = builder.spawn(move || {
            for slice in receiver {
                if output_clone.is_active() {
                    if let Some(product) = transformer.derive(slice) {
                        output_clone.send(product);
                    }
                } else {
                    break;
                }
            }
            debug!("dropping thread for output subscriber: {}", id);
        })?;

        Ok(OutputSubscriber {
            id,
            last: None,
            sender,
            output,
            thread_handle,
        })
    }

    /// Whether the worker thread has ended.
    ///
    /// TODO: clarify how this relates to [`Subscriber::is_active`] and when a caller
    /// should prefer one over the other.
    pub fn is_finished(&self) -> bool {
        self.thread_handle.is_finished()
    }
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> Subscriber
    for OutputSubscriber<C, VT, VO>
{
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
                .send(slice)
                .map_err(|source| SubscriberError::UnableToNotifySubscriber(Box::from(source)))?;
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
