//! Commands models side effects a reducer can ask the runtime to perform on its behalf. They
//!
//! Key idea is reducers stay pure: instead of doing work themselves they return [`Cmd`] values, and
//! the runtime decides where and when each one runs. Once executed, they communicate back with
//! the Runtime by generating new Actions that, again,are processes exclusively by the Reducer.

use crate::util::MessageSender;
use crate::{Client, ClientMessage};
use std::fmt::{Debug, Formatter};
use std::panic::UnwindSafe;

/// A side effect returned by a reducer for the runtime to carry out.
///
/// This crate provides different *modes* of execution.
///
/// In its most basic form, a Command, seen as a side effect, can be as simple a new [`Client::Action`]
/// we want to execute immediately in this same run loop iteration. [`Cmd::Direct`] solves this situation.
///
/// Similarly, we may want to execute an [`Client::Action`] as side effect, but we don't need to accumulate it
/// during the current run loop iteration and can (or want to) wait until the next one to be executed.
/// [`Cmd::Queue`] sends back the passed [`Client::Action`] to be processed on next runtime loop.
///
/// For costly operations on Runtime internal state that would block the Runtime's main thread, we
/// want to be able to offload the cost into a different thread and just get the response back when
/// finished. For this case we provide [`Client::Async`]. We also provide [`crate::util::DropCancellation`]
/// and [`crate::util::CancellationCheck`] for easy cooperative cancellation of this type of
/// asynchronous task.
///
/// Finally, the most common case for Commands is relying on some external data provider
/// outside of Runtime boundary, by calling into those services provided to Runtime as part of the
/// [`Client::Environment`] set. [`Cmd::Env`] wraps a Client provided [`EnvironmentCommand`] trait
/// implementer able to dispatch Commands to its Environment.
pub enum Cmd<C: Client> {
    /// Handle these actions immediately, before subscribers are notified.
    ///
    /// Notice a closed graph of dependencies between the Actions returned here could block the Runtime.
    Direct(Vec<C::Action>),

    /// Send these actions to the back of the message queue.
    ///
    /// Notice a closed graph of dependencies between the Actions returned here could block the Runtime.
    Queue(Vec<C::Action>),

    /// Run work on the jobs dispatcher and queue the action it returns.
    Async(AsyncTask<C::Action>),

    /// Hand a command to the client's environment.
    Env(C::ServiceCommand),
}

impl<C: Client> PartialEq for Cmd<C>
where
    C::Action: PartialEq,
    C::ServiceCommand: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cmd::Direct(a), Cmd::Direct(b)) | (Cmd::Queue(a), Cmd::Queue(b)) => a == b,
            (Cmd::Async(a), Cmd::Async(b)) => a == b,
            (Cmd::Env(a), Cmd::Env(b)) => a == b,
            _ => false,
        }
    }
}

impl<C: Client> Debug for Cmd<C>
where
    C::Action: Debug,
    C::ServiceCommand: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Cmd::Direct(actions) => f.debug_tuple("Direct").field(actions).finish(),
            Cmd::Queue(actions) => f.debug_tuple("Queue").field(actions).finish(),
            Cmd::Async(task) => f.debug_tuple("Async").field(task).finish(),
            Cmd::Env(cmd) => f.debug_tuple("Env").field(cmd).finish(),
        }
    }
}

/// A command needs, by definition, the client's environment to do its work.Think services that own
/// long-lived resources: timers, storage, network clients, etc. and in general any kind of resource
/// present outside the boundaries of a reducer (which essentially can only deal with received actions and
/// its inner state)
pub trait EnvironmentCommand {
    /// The client commands belongs to, which fixes both the environment it runs
    /// on and the messages it can produce as a product.
    type Client: Client;

    /// Runs the command against the environment. This is the boundary method between Client's services
    /// and the Runtime.
    ///
    /// Executes on the Runtime thread, so it should not block: long work belongs in a
    /// thread the environment owns, reporting back through `messages_tx`. This also means
    /// this method must not panic,or it would bring the entire Runtime down.
    ///
    /// Returned messages are handled inside the current loop iteration, before subscribers are
    /// notified, which allows the Client to prepare changes in state, via actions, related to a
    /// new Subscriber being present (i.e. prepare a view-related state)
    ///
    /// On the other side, actions sent through `messages_tx` are delivered on the next iteration
    /// loop at the earliest.
    fn process(
        self,
        env: &mut <Self::Client as Client>::Environment,
        messages_tx: &MessageSender<ClientMessage<Self::Client>>,
    ) -> Vec<ClientMessage<Self::Client>>;
}

/// A one-shot piece of work to run off the runtime thread, producing an action.
///
/// The unit of work is submitted to the [`crate::primitives::JobsDispatcher`] implementation passed
/// to Runtime in [`crate::RuntimeConfig`] and panic during its execution is controlled via [`UnwindSafe`]
pub struct AsyncTask<A> {
    /// Human-readable label, used for tracing and for equality between tasks.
    pub id: String,

    /// The work itself; its returned action is sent back to the runtime queue.
    pub job: Box<dyn FnOnce() -> A + UnwindSafe + Send + 'static>,
}

impl<A> Debug for AsyncTask<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncTask")
            .field("name", &self.id)
            .finish_non_exhaustive()
    }
}

impl<A> PartialEq for AsyncTask<A> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
