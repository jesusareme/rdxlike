use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;

#[derive(Debug)]
pub enum InitError {
    ThreadSpawn(io::Error),
    InvalidCapacity,
}

impl Display for InitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InitError::ThreadSpawn(source) => write!(f, "Unable to spawn thread: {source}"),
            InitError::InvalidCapacity => write!(f, "Capacity needs to be greater than zero"),
        }
    }
}

impl Error for InitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            InitError::ThreadSpawn(source) => Some(source),
            InitError::InvalidCapacity => None,
        }
    }
}

impl From<io::Error> for InitError {
    fn from(value: io::Error) -> Self {
        InitError::ThreadSpawn(value)
    }
}