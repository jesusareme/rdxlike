//! The envelopes every unit of work travels in on its way to the runtime.

use crate::error::RuntimeError;

/// A unit of work queued for the runtime.
///
/// Implementing [`From<T>`] for each Message subtype is ergonomically recommended practice, and,
/// in fact, both Client implementations [`crate::Client::Action`] and [`crate::Client:RuntimeAction`] need to implement
/// `Into<ClientMessage<_>>`.
#[derive(Debug, PartialEq)]
pub enum Message<A, R> {
    /// Model related actions.
    Action(A),

    /// Runtime specific actions.
    Runtime(R),
}

#[derive(Debug)]
pub(super) enum Operation<M> {
    Run(M),
    Stop(Option<RuntimeError>),
}
