use crate::MoniValidationError;
use boltffi::{data, error};
use rdxlib::error::InitError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::sync::mpsc::SendError;
use uuid::Uuid;

#[error]
#[derive(Debug, Clone)]
pub struct MoniError {
    pub id: Uuid,
    pub error_type: MoniErrorType,
}

#[data]
#[derive(Debug, Clone)]
pub enum MoniErrorType {
    Domain(MoniDomainError),
    Lib(LibErrorCause),
}

#[data]
#[derive(Debug, Clone)]
pub enum MoniDomainError {
    Validation(MoniValidationError),
    ExpenseNotFound(Uuid),
}

impl MoniError {
    pub fn new(error_type: MoniErrorType) -> Self {
        MoniError {
            id: Uuid::new_v4(),
            error_type,
        }
    }
}

impl From<MoniValidationError> for MoniError {
    fn from(value: MoniValidationError) -> Self {
        MoniError::new(MoniErrorType::Domain(MoniDomainError::Validation(value)))
    }
}

impl Display for MoniErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MoniErrorType::Domain(e) => e.fmt(f),
            MoniErrorType::Lib(LibErrorCause::Sender) => {
                write!(f, "Lib fatal error, unable to connect.")
            }
            MoniErrorType::Lib(LibErrorCause::Path) => {
                write!(f, "Lib fatal error, path is not available")
            }
            MoniErrorType::Lib(LibErrorCause::Threading) => {
                write!(f, "Lib fatal error, unable to create lib thread")
            }
            MoniErrorType::Lib(LibErrorCause::StateLoad(cause)) => {
                write!(f, "Lib fatal error, unable to load stored state: {cause}")
            }
        }
    }
}

impl Display for MoniError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.error_type.fmt(f)
    }
}

impl From<MoniErrorType> for MoniError {
    fn from(error_type: MoniErrorType) -> Self {
        MoniError::new(error_type)
    }
}

impl From<MoniDomainError> for MoniError {
    fn from(error: MoniDomainError) -> Self {
        MoniError::new(MoniErrorType::Domain(error))
    }
}

impl From<LibErrorCause> for MoniError {
    fn from(cause: LibErrorCause) -> Self {
        MoniError::new(MoniErrorType::Lib(cause))
    }
}

impl Display for MoniDomainError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MoniDomainError::Validation(e) => write!(f, "Validation error {e}"),
            MoniDomainError::ExpenseNotFound(id) => write!(f, "ExpenseNotFound: {id}"),
        }
    }
}

impl Error for MoniError {}

#[data]
#[derive(Debug, Clone)]
pub enum LibErrorCause {
    Sender,
    Path,
    Threading,
    StateLoad(String),
}

impl From<InitError> for MoniError {
    fn from(error: InitError) -> Self {
        tracing::error!("Lib component could not be initialized: {error}");
        MoniError::from(LibErrorCause::Threading)
    }
}

impl From<io::Error> for MoniError {
    fn from(error: io::Error) -> Self {
        MoniError::from(InitError::ThreadSpawn(error))
    }
}

impl<M> From<SendError<M>> for MoniError {
    fn from(_: SendError<M>) -> Self {
        MoniError::from(LibErrorCause::Sender)
    }
}
