//! Errors surfaced by the crate.
//!
//! All of them expose non-recoverable errors that would prevent the Runtime from executing normally.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

/// Something went wrong while setting up a component that owns threads.
#[derive(Debug)]
pub enum RuntimeError {
    /// The OS refused to spawn a thread.
    ThreadSpawn(io::Error),

    /// A capacity of zero was requested, which would leave nothing to do the work.
    InvalidCapacity,

    /// Client requested an operation but Runtime is no longer running.
    NoLongerRunning,
}

#[derive(Debug)]
pub struct RuntimeFatalError(pub(super) RuntimeError);

impl Display for RuntimeFatalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RuntimeFatalError").field(&self.0).finish()
    }
}

impl Error for RuntimeFatalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::ThreadSpawn(source) => write!(f, "Unable to spawn thread: {source}"),
            RuntimeError::InvalidCapacity => write!(f, "Capacity needs to be greater than zero"),
            RuntimeError::NoLongerRunning => write!(f, "Runtime already cancelled"),
        }
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RuntimeError::ThreadSpawn(source) => Some(source),
            RuntimeError::InvalidCapacity | RuntimeError::NoLongerRunning => None,
        }
    }
}

impl From<io::Error> for RuntimeError {
    fn from(value: io::Error) -> Self {
        RuntimeError::ThreadSpawn(value)
    }
}

/// Subscribers specific errors, rising up programming errors or receiving unexpected state.
#[derive(Debug)]
pub enum SubscriberError {
    /// The state a subscriber needs is not there, so no slice could be built.
    MissingState,

    /// The slice could not be handed over, i.e. the worker thread is gone.
    UnableToNotifySubscriber(Box<dyn Error + 'static>),
}

impl Display for SubscriberError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriberError::MissingState => write!(f, "Missing required state."),
            SubscriberError::UnableToNotifySubscriber(_) => {
                write!(f, "Unable to notify subscriber.")
            }
        }
    }
}
impl Error for SubscriberError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SubscriberError::MissingState => None,
            SubscriberError::UnableToNotifySubscriber(source) => Some(source.as_ref()),
        }
    }
}
