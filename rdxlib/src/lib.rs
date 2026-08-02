pub mod cmd;
pub mod messages;
pub mod middleware;
pub mod subscribers;
pub mod threadpool;
pub mod util;

use crate::cmd::{Cmd, SCmd};
use crate::messages::Message;
use crate::middleware::{ChainableMiddleware, MiddlewareStore};
use crate::threadpool::{JobsDispatcher, ThreadPool};
use enumset::EnumSet;
use std::collections::VecDeque;
use std::ops::{Add, AddAssign};
use std::sync::mpsc::Receiver;
use subscribers::Subscriber;
use tracing::{error, info};
use util::MessageSender;

pub struct RuntimeProducts<C: Client> {
    pub subscriber: Option<Box<dyn Subscriber<Flag = C::Flag, State = C::State>>>,
    pub actions: Vec<C::Action>,
}

pub struct ActionProducts<C: Client> {
    pub cmds: Vec<Cmd<C::Action, C::ServiceCommand>>,
    pub dirty: EnumSet<C::Flag>,
}

impl<C: Client> ActionProducts<C> {
    pub fn none() -> Self {
        ActionProducts {
            cmds: vec![],
            dirty: EnumSet::empty(),
        }
    }

    pub fn cmd(cmd: impl Into<Cmd<C::Action, C::ServiceCommand>>) -> Self {
        ActionProducts {
            cmds: vec![cmd.into()],
            dirty: EnumSet::empty(),
        }
    }

    pub fn cmds(cmds: Vec<Cmd<C::Action, C::ServiceCommand>>) -> Self {
        ActionProducts {
            cmds,
            dirty: EnumSet::empty(),
        }
    }

    pub fn with_cmd(mut self, cmd: impl Into<Cmd<C::Action, C::ServiceCommand>>) -> Self {
        self.cmds.push(cmd.into());
        self
    }

    pub fn with_dirty(mut self, flags: impl Into<EnumSet<C::Flag>>) -> Self {
        self.dirty |= flags.into();
        self
    }
}

impl<C: Client> Default for ActionProducts<C> {
    fn default() -> Self {
        ActionProducts::none()
    }
}

impl<C: Client> Add<ActionProducts<C>> for ActionProducts<C> {
    type Output = ActionProducts<C>;

    fn add(mut self, rhs: ActionProducts<C>) -> Self::Output {
        self += rhs;
        self
    }
}

impl<C: Client> AddAssign<ActionProducts<C>> for ActionProducts<C> {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn add_assign(&mut self, rhs: ActionProducts<C>) {
        self.cmds.extend(rhs.cmds);
        self.dirty |= rhs.dirty;
    }
}

pub trait Client {
    type State;
    type Action: Send + 'static + Into<Message<Self::Action, Self::RuntimeAction>>;
    type RuntimeAction: Send + 'static + Into<Message<Self::Action, Self::RuntimeAction>>;
    type Flag: enumset::EnumSetType;
    type ServiceCommand: SCmd;
    type Environment;
}

pub type Reducer<C> = fn(&mut <C as Client>::State, <C as Client>::Action) -> ActionProducts<C>;

pub type RuntimeReducer<C> = fn(<C as Client>::RuntimeAction) -> RuntimeProducts<C>;

pub struct RuntimeConfig<C: Client, JD: JobsDispatcher = ThreadPool> {
    pub services: <C::ServiceCommand as SCmd>::Environment,
    pub state: C::State,
    pub middlewares: Vec<Box<dyn ChainableMiddleware<C>>>,
    pub reducer: Reducer<C>,
    pub runtime_reducer: RuntimeReducer<C>,
    pub jobs_dispatcher: JD,
    pub messages_rx: Receiver<Message<C::Action, C::RuntimeAction>>,
    pub messages_tx: MessageSender<C::Action, C::RuntimeAction>,
}

pub struct Runtime<C: Client, JD: JobsDispatcher = ThreadPool> {
    services: <C::ServiceCommand as SCmd>::Environment,
    state: C::State,
    middlewares: MiddlewareStore<C>,
    subscribers: Vec<Box<dyn Subscriber<Flag = C::Flag, State = C::State>>>,
    messages_rx: Receiver<Message<C::Action, C::RuntimeAction>>,
    messages_tx: MessageSender<C::Action, C::RuntimeAction>,
    runtime_reducer: RuntimeReducer<C>,
    jobs_dispatcher: JD,
}

impl<C: Client, JD: JobsDispatcher> Runtime<C, JD> {
    pub fn new(config: RuntimeConfig<C, JD>) -> Self {
        Runtime {
            services: config.services,
            state: config.state,
            middlewares: MiddlewareStore::new(config.middlewares, config.reducer),
            subscribers: vec![],
            messages_rx: config.messages_rx,
            messages_tx: config.messages_tx,
            runtime_reducer: config.runtime_reducer,
            jobs_dispatcher: config.jobs_dispatcher,
        }
    }

    pub fn run(mut self) {
        info!("Started run loop for RdxLib...");

        while let Ok(message) = self.messages_rx.recv() {
            self.process_message(message);
        }

        info!("Finished run loop for RdxLib...");
    }

    fn process_message(&mut self, message: Message<C::Action, C::RuntimeAction>) {
        let mut pending: VecDeque<Message<C::Action, C::RuntimeAction>> = VecDeque::new();
        pending.push_back(message);
        let mut dirty = EnumSet::empty();

        while let Some(message) = pending.pop_front() {
            match message {
                Message::Runtime(runtime_action) => {
                    let products = (self.runtime_reducer)(runtime_action);
                    if let Some(subscriber) = products.subscriber {
                        self.subscribers.push(subscriber);
                    }
                    pending.extend(products.actions.into_iter().map(Into::into));
                }

                Message::Action(action) => {
                    let effects = self.middlewares.run(&mut self.state, action);
                    dirty |= effects.dirty;

                    for cmd in effects.cmds {
                        Self::process_command(
                            cmd,
                            &mut self.services,
                            &self.jobs_dispatcher,
                            &self.messages_tx,
                            &mut pending,
                        );
                    }
                }
            }
        }

        Self::notify_subscribers(&self.state, &mut self.subscribers, dirty);
    }

    fn notify_subscribers(
        state: &C::State,
        subscribers: &mut Vec<Box<dyn Subscriber<Flag = C::Flag, State = C::State>>>,
        dirty: EnumSet<C::Flag>,
    ) {
        subscribers.retain(|s| s.is_active());
        subscribers
            .iter_mut()
            .filter(|s| s.interested_in(&dirty))
            .filter_map(|s| s.notify(state).err())
            .for_each(|e| error!("Subscriber error: {e}"));
    }

    fn process_command(
        cmd: Cmd<C::Action, C::ServiceCommand>,
        services: &mut <C::ServiceCommand as SCmd>::Environment,
        jobs_dispatcher: &JD,
        messages_tx: &MessageSender<C::Action, C::RuntimeAction>,
        pending: &mut VecDeque<Message<C::Action, C::RuntimeAction>>,
    ) {
        use Cmd::*;
        match cmd {
            Direct(new_work_actions) => {
                pending.extend(new_work_actions.into_iter().map(Into::into));
            }

            Queue(new_work_actions) => {
                new_work_actions.into_iter().for_each(|a| {
                    _ = messages_tx.send_message(a).inspect_err(|e| {
                        error!("Error while sending new actions from Queue command: {e:?}");
                    });
                });
            }

            Async(job) => {
                let messages_tx = messages_tx.clone();
                jobs_dispatcher.work_on(Box::new(move || {
                    let action = job();
                    messages_tx.send_message(action).unwrap(); //todo! control errors
                }));
            }

            Env(job) => {
                job.process(services);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn received_msg_reducer_middlewares_called() {}
}
