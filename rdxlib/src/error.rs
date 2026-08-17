//! Errors surfaced by the crate.
//!
//! TODO: note the intended handling for each one - which are fatal at startup and which a
//! client can recover from.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

/// Something went wrong while setting up a component that owns threads.
#[derive(Debug)]
pub enum InitError {
    /// The OS refused to spawn a thread.
    ThreadSpawn(io::Error),

    /// A capacity of zero was requested, which would leave nothing to do the work.
    InvalidCapacity,
    NoLongerRunning,
}

impl Display for InitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InitError::ThreadSpawn(source) => write!(f, "Unable to spawn thread: {source}"),
            InitError::InvalidCapacity => write!(f, "Capacity needs to be greater than zero"),
            InitError::NoLongerRunning => write!(f, "Runtime already cancelled"),
        }
    }
}

impl Error for InitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InitError::ThreadSpawn(source) => Some(source),
            InitError::InvalidCapacity | InitError::NoLongerRunning  => None,
        }
    }
}

impl From<io::Error> for InitError {
    fn from(value: io::Error) -> Self {
        InitError::ThreadSpawn(value)
    }
}

/// A subscriber could not be told about a change it was interested in.
///
/// TODO: confirm the runtime's policy for these (currently logged, subscriber kept).
#[derive(Debug)]
pub enum SubscriberError {
    /// The state a subscriber needs is not there, so no slice could be built.
    MissingState,

    /// The slice could not be handed over, e.g. the worker thread is gone.
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
