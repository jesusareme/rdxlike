mod cmd;
mod messages;
pub mod middleware;
mod subscribers;
mod threadpool;
mod util;

use crate::cmd::{Cmd, SCmd};
use crate::messages::Message;
use crate::middleware::{ChainableMiddleware, MiddlewareStore};
use crate::threadpool::{JobsDispatcher, ThreadPool};
use enumset::EnumSet;
use std::collections::VecDeque;
use std::ops::{Add, AddAssign};
use std::sync::mpsc::{Receiver, channel};
use subscribers::Subscriber;
use tracing::{error, info};
use util::MessageSender;

pub struct RuntimeProducts<State, Flag> {
    pub subscriber: Option<Box<dyn Subscriber<Flag = Flag, State = State>>>,
}

pub struct ActionProducts<CM>
where
    CM: ChainableMiddleware,
{
    pub cmds: Vec<Cmd<CM::Action, CM::ServiceCmd>>,
    pub dirty: EnumSet<CM::Flag>,
}

impl<CM: ChainableMiddleware> ActionProducts<CM> {
    pub fn none() -> Self {
        ActionProducts {
            cmds: vec![],
            dirty: EnumSet::empty(),
        }
    }

    pub fn cmd(cmd: impl Into<Cmd<CM::Action, CM::ServiceCmd>>) -> Self {
        ActionProducts {
            cmds: vec![cmd.into()],
            dirty: EnumSet::empty(),
        }
    }

    pub fn cmds(cmds: Vec<Cmd<CM::Action, CM::ServiceCmd>>) -> Self {
        ActionProducts {
            cmds,
            dirty: EnumSet::empty(),
        }
    }

    pub fn with_cmd(mut self, cmd: impl Into<Cmd<CM::Action, CM::ServiceCmd>>) -> Self {
        self.cmds.push(cmd.into());
        self
    }

    pub fn with_dirty(mut self, flags: impl Into<EnumSet<CM::Flag>>) -> Self {
        self.dirty |= flags.into();
        self
    }
}

impl<CM: ChainableMiddleware> Default for ActionProducts<CM> {
    fn default() -> Self {
        ActionProducts::none()
    }
}

impl<CM: ChainableMiddleware> Add<ActionProducts<CM>> for ActionProducts<CM> {
    type Output = ActionProducts<CM>;

    fn add(mut self, rhs: ActionProducts<CM>) -> Self::Output {
        self += rhs;
        self
    }
}

impl<CM: ChainableMiddleware> AddAssign<ActionProducts<CM>> for ActionProducts<CM> {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn add_assign(&mut self, rhs: ActionProducts<CM>) {
        self.cmds.extend(rhs.cmds);
        self.dirty |= rhs.dirty;
    }
}

type Reducer<CM> = fn(
    &mut <CM as ChainableMiddleware>::State,
    <CM as ChainableMiddleware>::Action,
) -> ActionProducts<CM>;
type RuntimeReducer<Action, State, Flag> = fn(Action) -> RuntimeProducts<State, Flag>;

pub struct Runtime<RuntimeAction, CM: ChainableMiddleware, JD: JobsDispatcher = ThreadPool>
{
    services: <CM::ServiceCmd as SCmd>::Environment,
    state: CM::State,
    middlewares: MiddlewareStore<CM>,
    subscribers: Vec<Box<dyn Subscriber<Flag = CM::Flag, State = CM::State>>>,
    messages_rx: Receiver<Message<CM::Action, RuntimeAction>>,
    messages_tx: MessageSender<CM::Action, RuntimeAction>,
    runtime_reducer: RuntimeReducer<RuntimeAction, CM::State, CM::Flag>,
    jobs_dispatcher: JD,
}

impl<RuntimeAction, CM, JD> Runtime<RuntimeAction, CM, JD>
where
    RuntimeAction: Send + 'static,
    CM: ChainableMiddleware,
    JD: JobsDispatcher,
    Message<CM::Action, RuntimeAction>: From<CM::Action>,
{
    pub fn new(
        services: <<CM as ChainableMiddleware>::ServiceCmd as SCmd>::Environment,
        state: CM::State,
        middlewares_chain: Vec<CM>,
        reducer: Reducer<CM>,
        runtime_reducer: RuntimeReducer<RuntimeAction, CM::State, CM::Flag>,
        jobs_dispatcher: JD,
    ) -> Self {
        let (tx, messages_rx) = channel();

        Runtime {
            services,
            state,
            middlewares: MiddlewareStore::new(middlewares_chain, reducer),
            subscribers: vec![],
            messages_rx,
            messages_tx: MessageSender::new(tx),
            runtime_reducer,
            jobs_dispatcher,
        }
    }

    pub fn sender(&self) -> MessageSender<CM::Action, RuntimeAction> {
        self.messages_tx.clone()
    }

    pub fn run(mut self) {
        info!("Started run loop for RdxLib...");

        while let Ok(message) = self.messages_rx.recv() {
            self.process_message(message);
        }

        info!("Finished run loop for RdxLib...");
    }

    fn process_message(&mut self, message: Message<CM::Action, RuntimeAction>) {
        let mut pending: VecDeque<Message<CM::Action, RuntimeAction>> = VecDeque::new();
        pending.push_back(message);
        let mut dirty = EnumSet::empty();

        while let Some(message) = pending.pop_front() {
            match message {
                Message::Runtime(runtime_action) => {
                    let products = (self.runtime_reducer)(runtime_action);
                    if let Some(subscriber) = products.subscriber {
                        self.subscribers.push(subscriber);
                    }
                }

                Message::Action(action) => {
                    let effects = self.middlewares.run(&mut self.state, action);
                    dirty |= effects.dirty;

                    for cmd in effects.cmds {
                        self.process_command(cmd, &mut pending);
                    }
                }
            }
        }

        self.notify_subscribers(dirty);
    }

    fn notify_subscribers(&mut self, dirty: EnumSet<CM::Flag>) {
        let state = &self.state;

        self.subscribers.retain(|s| s.is_active());
        self.subscribers
            .iter_mut()
            .filter(|s| s.interested_in(&dirty))
            .filter_map(|s| s.notify(state).err())
            .for_each(|e| error!("Subscriber error: {e}"));
    }

    fn process_command(
        &mut self,
        cmd: Cmd<CM::Action, CM::ServiceCmd>,
        pending: &mut VecDeque<Message<CM::Action, RuntimeAction>>,
    ) {
        use Cmd::*;
        match cmd {
            Direct(new_work_actions) => {
                pending
                    .extend(new_work_actions
                    .into_iter()
                    .map(Into::into));
            }

            Queue(new_work_actions) => {
                new_work_actions.into_iter().for_each(|a| {
                    _ = self.messages_tx.send_message(a).inspect_err(|e| {
                        error!("Error while sending new actions from Queue command: {e:?}");
                    });
                });
            }

            Async(job) => {
                let messages_tx = self.messages_tx.clone();
                self.jobs_dispatcher.work_on(Box::new(move || {
                    let action = job();
                    messages_tx.send_message(action).unwrap(); //todo! control errors
                }));
            }

            Env(job) => {
                job.process(&mut self.services);
            }
        }
    }
}