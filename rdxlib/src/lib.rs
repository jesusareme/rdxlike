pub mod cmd;
pub mod messages;
pub mod middleware;
pub mod subscribers;
pub mod threadpool;
pub mod util;
pub mod products;

use crate::cmd::{Cmd, SCmd};
use crate::messages::Message;
use crate::middleware::{ChainableMiddleware, MiddlewareStore};
use crate::products::{ActionProducts, RuntimeProducts};
use crate::threadpool::{JobsDispatcher, ThreadPool};
use enumset::EnumSet;
use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use subscribers::Subscriber;
use tracing::{error, info};
use util::MessageSender;

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
