//! Subscribers observe the model without touching it and report back to Client when needed.
//!
//! A subscriber is asked, in order, whether it is still alive (Client could have disconnected a
//! view associated with the Subscriber), whether it cares about what changed, and only then it is
//! handed the new state.
//!
//! [`OutputSubscriber`] is a ready-made, view-oriented implementation: it compares, slices
//! minimum requited state on the runtime thread and derives the view's data on a thread of its own.

use crate::Client;
use crate::error::RuntimeError;
pub use crate::error::SubscriberError;
use crate::primitives::{OneSlotSender, one_slot_channel};
use crate::subscribers::ComparableResult::Comparable;
use enumset::EnumSet;
use std::fmt::{Debug, Display, Formatter};
use std::thread;
use tracing::debug;
use uuid::Uuid;

/// Defines what makes a Subscriber.
///
/// Every method runs on the runtime thread, in between messages, so none of them should
/// block.
pub trait Subscriber {
    /// The client this subscriber belongs to.
    type Client: Client;

    /// Whether this subscriber is still worth keeping.
    ///
    /// Answering `false` gets it dropped from the runtime before any other check.
    ///
    /// This is useful for auto-disabling Subscribers, that is, Subscribers able to
    /// detect when Client no longer needs them. At that point they should return `false` to
    /// this call for the Runtime to prune them from memory before calling [`Self::interested_in`].
    fn is_active(&self) -> bool;

    /// Whether the flags dirty during last events loop could matter to this subscriber.
    ///
    /// A cheap filter: answering `true` only earns a call to [`Subscriber::notify`].
    fn interested_in(&self, offered: &EnumSet<<Self::Client as Client>::Flag>) -> bool;

    /// Offers the new state, already filtered by [`Subscriber::interested_in`].
    ///
    /// Deciding the change is irrelevant after a closer look is not considered error and should
    /// return `Ok(())`.
    ///
    /// # Errors
    /// Will return [`SubscriberError`] if Subscriber could not get to an expected state, or if
    /// an internal error has prevented the Subscriber from doing its job. Returning an error will
    /// neither break the run-loop nor exclude this Subscriber from updates later.
    fn notify(
        &mut self,
        new_state: &<Self::Client as Client>::State,
    ) -> Result<(), SubscriberError>;
}

/// Defines the answer to the question "can this state be compared against the previous one?".
#[derive(Debug, PartialEq)]
pub enum ComparableResult<T> {
    /// No comparison is possible to any previous state because current state is undefined or
    /// incompatible with the intended subscription
    NothingToCompare,
    /// Comparison is possible to a previous state because we have a well-defined current value
    /// that we pass as argument for this type.
    Comparable(T),
}

/// The client's recipe for turning model state into what one view displays, to be used
/// with the concrete type [`OutputSubscriber`]
///
/// The work is split in three so that as little as possible happens on the runtime thread:
/// [`ViewTransformer::interested_in`] and [`ViewTransformer::comparable`] are cheap checks,
/// [`ViewTransformer::slice`] copies out the minimum the view needs, and
/// [`ViewTransformer::derive`] does the real work off the Runtime thread.
pub trait ViewTransformer<C: Client>: Send + 'static {
    /// As-cheap-as-possible internal state to be compared between notifications to identify relevant
    /// state changes. [`OutputSubscriber`] stores and compares the value on out behalf.
    ///
    /// Example: To identify if we have a new email pending to be drawn, we may use as `ComparableValue`
    /// the type used as email id (we would store the latest received email id there).
    type ComparableValue: PartialEq;

    /// The self-contained piece of state handed to the worker thread.
    ///
    /// Example: to show a list of emails, Slice could be an array of not-yet-displayed emails models.
    type Slice: Send + 'static;

    /// The finished data the view consumes.
    ///
    /// Example: to show a list of emails, the Product could be the list of titles and first lines
    /// from emails received since last update.
    type Product: Send + 'static;

    /// Indicates caller whether this subscriber will potentially be notifiable based on rough
    /// estimation expressed by `Dirty` flags.
    fn interested_in(offered: &EnumSet<C::Flag>) -> bool;

    /// Returns a `ComparableResult::Comparable` value used to identify relevant changes in the
    /// model. A new derived state will be generated if the comparison is favorable
    /// (previous value returned from this function is different from the one before).
    /// First execution of a `ViewTransformer` will always generate a new state derivation as
    /// long as this functions doesn't return a `ComparableResult::NothingToCompare` result.
    fn comparable(state: &C::State, token: ViewId) -> ComparableResult<Self::ComparableValue>;

    /// Extracts the minimum thread-safe slice from the original state needed to calculate the
    /// final information the view needs. It returns `SubscriberError` if was unable to create the
    /// state slice.
    /// # Errors
    /// Will return [`SubscriberError`] if it could not get a meaningful slice of state
    /// from which derive a view product.
    fn slice(state: &C::State, token: ViewId) -> Result<Self::Slice, SubscriberError>;

    /// Derives the final data needed by the view. This method is executed on its own
    /// thread.
    fn derive(&mut self, slice: Self::Slice) -> Option<Self::Product>;
}

/// A view/oriented general-purpose [`Subscriber`]: compares on the runtime thread, derives on
/// its own.
///
/// Each instance owns a worker thread that lives until the output goes inactive or the
/// slice channel closes. Slices are sent through a one-slot channel, so a worker that falls
/// behind picks up the latest state and skips whatever it missed.
#[allow(unused)]
pub struct OutputSubscriber<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> {
    id: ViewId,
    last: Option<VT::ComparableValue>,
    sender: OneSlotSender<VT::Slice>,
    output: VO,
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> OutputSubscriber<C, VT, VO> {
    /// Creates an instance of [`OutputSubscriber`] by spawning a new thread ready
    /// to participate as a Subscriber.
    ///
    /// # Errors
    /// Will return `Err` if an OS level error was produced while spawning a worker thread.
    pub fn new(id: ViewId, mut transformer: VT, output: VO) -> Result<Self, RuntimeError> {
        let (sender, receiver) = one_slot_channel::<VT::Slice>();
        let builder = thread::Builder::new().name(id.to_string());
        let output_clone = output.clone();
         builder.spawn(move || {
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
        })
    }
}

/// The channel a derived product travels through to reach the actual view.
///
/// Implemented on the platform side (UI framework, FFI bridge, test double).
///
/// This is currently designed as a thin wrapper around BoltFFI´s `EventSubscription`.
#[allow(unused)]
pub trait ViewOutput<V: Send + 'static>: Clone + Send + 'static {
    /// Creates the output.
    fn new() -> Self;

    /// Delivers a product, answering whether it was accepted.
    fn send(&self, v: V) -> bool;

    /// Whether the view on the other end is still listening.
    ///
    /// Once this turns `false` the subscriber is dropped by the runtime.
    fn is_active(&self) -> bool;
}

impl<C: Client, VT: ViewTransformer<C>, VO: ViewOutput<VT::Product>> Subscriber
    for OutputSubscriber<C, VT, VO>
{
    type Client = C;

    fn is_active(&self) -> bool {
        self.output.is_active()
    }

    fn interested_in(&self, offered: &EnumSet<C::Flag>) -> bool {
        VT::interested_in(offered)
    }

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
}

/// Identity for a single view subscription.
///
/// Created by Client as a token to be attached to Actions and to identify Subscription results to
/// track specific view related behavior and derived data. [`ViewId`] can be safely shared and
/// copied across threads and used to store and identify view specific state in [`Client::State`].
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