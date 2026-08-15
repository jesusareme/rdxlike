use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::panic::UnwindSafe;
use crate::Client;

pub trait EnvironmentCommand {
    type Environment;

    fn process(self, env: &mut Self::Environment);
}

pub struct AsyncTask<A> {
    pub name: String,
    pub job: Box<dyn FnOnce() -> A + UnwindSafe + Send + 'static>,
}

impl<A> Debug for AsyncTask<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncTask")
            .field("name", &self.name)
            .finish()
    }
}

impl<A> PartialEq for AsyncTask<A> {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

pub enum Cmd<C: Client>
{
    Direct(Vec<C::Action>),
    Queue(Vec<C::Action>),
    Async(AsyncTask<C::Action>),
    Env(C::ServiceCommand),
}

impl<C: Client> PartialEq for Cmd<C>
where
    C::Action: PartialEq,
    C::ServiceCommand: PartialEq {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cmd::Direct(a), Cmd::Direct(b)) => a == b,
            (Cmd::Queue(a), Cmd::Queue(b)) => a == b,
            (Cmd::Async(a), Cmd::Async(b)) => a == b,
            (Cmd::Env(a), Cmd::Env(b)) => a == b,
            _ => false,
        }
    }
}

impl<C: Client> Debug for Cmd<C>
where
    C::Action: Debug,
    C::ServiceCommand: Debug {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Cmd::Direct(actions) => f.debug_tuple("Direct").field(actions).finish(),
            Cmd::Queue(actions) => f.debug_tuple("Queue").field(actions).finish(),
            Cmd::Async(task) => f.debug_tuple("Async").field(task).finish(),
            Cmd::Env(cmd) => f.debug_tuple("Env").field(cmd).finish(),
        }
    }
}