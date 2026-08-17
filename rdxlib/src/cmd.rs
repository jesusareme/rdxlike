//! Side effects a reducer can ask the runtime to perform.
//!
//! Reducers stay pure: instead of doing work themselves they return [`Cmd`] values, and
//! the runtime decides where and when each one runs.

use crate::{Client, ClientMessage};
use crate::util::MessageSender;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::panic::UnwindSafe;

/// A command that needs the client's environment to do its work.
///
/// This is the escape hatch for effects that own long-lived resources - timers, storage,
/// network clients - which cannot be expressed as a one-shot [`AsyncTask`].
///
/// TODO: guidance on what belongs in the environment and what does not.
pub trait EnvironmentCommand {
    /// The client this command belongs to, which fixes both the environment it runs
    /// against and the messages it can produce.
    type Client: Client;

    /// Runs the command against the environment.
    ///
    /// Executes on the runtime thread, so it should not block: long work belongs in a
    /// thread the environment owns, reporting back through `messages_tx`.
    ///
    /// Actions pushed onto `pending` are handled inside the current cascade, before
    /// subscribers are notified; actions sent through `messages_tx` are handled later, as
    /// separate messages.
    ///
    /// TODO: state whether a panic here is expected to be fatal for the runtime.
    fn process(
        self,
        env: &mut <Self::Client as Client>::Environment,
        pending: &mut VecDeque<ClientMessage<Self::Client>>,
        messages_tx: &MessageSender<ClientMessage<Self::Client>>,
    );
}

/// A one-shot piece of work to run off the runtime thread, producing an action.
///
/// TODO: mention the unwind-safety requirement and what happens if the job panics.
pub struct AsyncTask<A> {
    /// Human-readable label, used for tracing and for equality between tasks.
    pub name: String,

    /// The work itself; its returned action is sent back to the runtime queue.
    pub job: Box<dyn FnOnce() -> A + UnwindSafe + Send + 'static>,
}

impl<A> Debug for AsyncTask<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncTask")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl<A> PartialEq for AsyncTask<A> {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

/// A side effect returned by a reducer for the runtime to carry out.
///
/// The variant chosen decides *when* the resulting actions are seen: `Direct` keeps them
/// inside the current cascade, everything else defers them to a later message.
///
/// TODO: a short "which one do I pick?" note for client authors.
pub enum Cmd<C: Client> {
    /// Handle these actions immediately, before subscribers are notified.
    ///
    /// TODO: warn about the loop risk of an action that keeps producing itself.
    Direct(Vec<C::Action>),

    /// Send these actions to the back of the message queue.
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
