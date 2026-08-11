pub mod cmd;
pub mod error;
pub mod messages;
pub mod middleware;
pub mod primitives;
pub mod products;
pub mod subscribers;
pub mod util;

use crate::cmd::{Cmd, EnvironmentCommand};
use crate::messages::Message;
use crate::middleware::{ChainableMiddleware, MiddlewareStore};
use crate::primitives::{JobsDispatcher, ThreadPool};
use crate::products::{ActionProducts, RuntimeProducts};
use crate::util::MessageSend;
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
    type ServiceCommand: EnvironmentCommand;
}

pub type Reducer<C> = fn(&mut <C as Client>::State, <C as Client>::Action) -> ActionProducts<C>;

pub type RuntimeReducer<C> = fn(<C as Client>::RuntimeAction) -> RuntimeProducts<C>;

pub struct RuntimeConfig<C: Client, JD: JobsDispatcher = ThreadPool> {
    pub services: <C::ServiceCommand as EnvironmentCommand>::Environment,
    pub state: C::State,
    pub middlewares: Vec<Box<dyn ChainableMiddleware<C>>>,
    pub reducer: Reducer<C>,
    pub runtime_reducer: RuntimeReducer<C>,
    pub jobs_dispatcher: JD,
    pub messages_rx: Receiver<Message<C::Action, C::RuntimeAction>>,
    pub messages_tx: MessageSender<Message<C::Action, C::RuntimeAction>>,
}

pub struct Runtime<C: Client, JD: JobsDispatcher = ThreadPool> {
    services: <C::ServiceCommand as EnvironmentCommand>::Environment,
    state: C::State,
    middlewares: MiddlewareStore<C>,
    subscribers: Vec<Box<dyn Subscriber<Flag = C::Flag, State = C::State>>>,
    messages_rx: Receiver<Message<C::Action, C::RuntimeAction>>,
    messages_tx: MessageSender<Message<C::Action, C::RuntimeAction>>,
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
                    dirty |= effects.flags;

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
        cmd: Cmd<C>,
        services: &mut <C::ServiceCommand as EnvironmentCommand>::Environment,
        jobs_dispatcher: &JD,
        messages_tx: &MessageSender<Message<C::Action, C::RuntimeAction>>,
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

            Async(task) => {
                let messages_tx = messages_tx.clone();
                jobs_dispatcher.work_on(Box::new(move || {
                    let action = (task.job)();
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
    use crate::middleware::Next;
    use crate::subscribers::SubscriberError;
    use enumset::EnumSetType;
    use rstest::{fixture, rstest};
    use std::assert_matches;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::mpsc;
    use std::sync::mpsc::TryRecvError;

    struct MClient;
    impl Client for MClient {
        type State = BasicState;
        type Action = FakeAction;
        type RuntimeAction = BasicRuntimeAction;
        type Flag = BasicFlag;
        type ServiceCommand = FakeServiceCommand;
    }

    struct WitnessServices {
        called: Rc<Cell<u32>>,
    }

    #[derive(Debug, PartialEq, Clone)]
    struct BasicState {
        called: Vec<FakeAction>,
    }

    #[derive(Debug, PartialEq, Clone)]
    enum CmdProduct {
        Direct(Vec<FakeAction>),
        Queue(Vec<FakeAction>),
        Async(Box<FakeAction>),
        Env(FakeServiceCommand),
    }

    #[derive(Debug, PartialEq, Clone)]
    enum FakeAction {
        BasicAction(&'static str),
        FlagAction(BasicFlag),
        CmdGeneratingAction(&'static str, CmdProduct),
    }

    impl Into<Message<FakeAction, BasicRuntimeAction>> for FakeAction {
        fn into(self) -> Message<FakeAction, BasicRuntimeAction> {
            Message::Action(self)
        }
    }

    #[derive(Debug, PartialEq)]
    struct BasicRuntimeAction;
    impl Into<Message<FakeAction, BasicRuntimeAction>> for BasicRuntimeAction {
        fn into(self) -> Message<FakeAction, BasicRuntimeAction> {
            Message::Runtime(self)
        }
    }

    #[derive(EnumSetType, Debug)]
    enum BasicFlag {
        A,
        B,
        C,
        D,
        E,
    }

    #[derive(Debug, PartialEq, Clone)]
    struct FakeServiceCommand;
    impl EnvironmentCommand for FakeServiceCommand {
        type Environment = WitnessServices;
        fn process(self, env: &mut Self::Environment) {
            env.called.update(|c| c + 1)
        }
    }

    struct BasicJobDispatcher {
        pub called: Rc<Cell<u32>>,
    }

    impl JobsDispatcher for BasicJobDispatcher {
        fn work_on(&self, job: Box<dyn FnOnce() + Send + 'static>) {
            self.called.update(|v| v + 1);
            job()
        }
    }

    type MessageWitness = Rc<RefCell<Vec<FakeAction>>>;
    struct WitnessMiddleware {
        messages: MessageWitness,
    }
    impl ChainableMiddleware<MClient> for WitnessMiddleware {
        fn execute(
            &mut self,
            state: &mut BasicState,
            action: FakeAction,
            mut next: Next<MClient>,
        ) -> ActionProducts<MClient> {
            self.messages.borrow_mut().push(action.clone());
            next.run(state, action)
        }
    }

    struct WitnessSubscriber {
        operations: Rc<RefCell<Vec<WitnessSubscriberChecks>>>,
        should_be_interested: bool,
        should_be_active: bool,
        should_notify_error: bool,
    }

    #[derive(PartialEq, Debug, Clone)]
    enum WitnessSubscriberChecks {
        Interested(EnumSet<BasicFlag>),
        Notify(BasicState),
        Active,
    }
    use crate::cmd::AsyncTask;
    use crate::messages::Message::Action;
    use crate::tests::FakeAction::{BasicAction, CmdGeneratingAction, FlagAction};
    use WitnessSubscriberChecks::{Active, Interested, Notify};

    impl Default for WitnessSubscriber {
        fn default() -> Self {
            WitnessSubscriber {
                operations: Rc::new(RefCell::new(vec![])),
                should_be_interested: true,
                should_be_active: true,
                should_notify_error: false,
            }
        }
    }

    impl Subscriber for WitnessSubscriber {
        type State = BasicState;
        type Flag = BasicFlag;

        fn notify(&mut self, new_state: &Self::State) -> Result<(), SubscriberError> {
            self.operations.borrow_mut().push(Notify(new_state.clone()));
            if self.should_notify_error {
                Err(SubscriberError::MissingState)
            } else {
                Ok(())
            }
        }

        fn is_active(&self) -> bool {
            self.operations.borrow_mut().push(Active);
            self.should_be_active
        }

        fn interested_in(&self, offered: &EnumSet<Self::Flag>) -> bool {
            self.operations
                .borrow_mut()
                .push(Interested(offered.clone()));
            self.should_be_interested
        }
    }

    type MessagingPair = (
        MessageSender<Message<FakeAction, BasicRuntimeAction>>,
        Receiver<Message<FakeAction, BasicRuntimeAction>>,
    );
    #[fixture]
    fn sender_receiver() -> MessagingPair {
        let (sender, receiver) = mpsc::channel();
        (MessageSender::new(sender), receiver)
    }

    fn witness_reducer(state: &mut BasicState, action: FakeAction) -> ActionProducts<MClient> {
        state.called.push(action);
        ActionProducts::none()
    }

    fn flags_producing_reducer(
        state: &mut BasicState,
        action: FakeAction,
    ) -> ActionProducts<MClient> {
        state.called.push(action.clone());
        let (cmds, flags) = match action {
            CmdGeneratingAction(_, CmdProduct::Direct(actions)) => {
                (vec![Cmd::Direct(actions)], EnumSet::empty())
            }
            FlagAction(flag) => (vec![], flag.into()),
            _ => unreachable!("Only needed types implemented"),
        };
        ActionProducts { cmds, flags }
    }

    fn cmd_producing_reducer(
        state: &mut BasicState,
        action: FakeAction,
    ) -> ActionProducts<MClient> {
        state.called.push(action.clone());
        let cmds = match action {
            BasicAction(_) => vec![],
            CmdGeneratingAction(_, cmd_product) => match cmd_product {
                CmdProduct::Direct(actions) => vec![Cmd::Direct(actions)],
                CmdProduct::Queue(actions) => vec![Cmd::Queue(actions)],
                CmdProduct::Async(boxed_action) => vec![Cmd::Async(AsyncTask {
                    name: "test_async".to_string(),
                    job: Box::new(move || *boxed_action),
                })],
                CmdProduct::Env(service_cmd) => vec![Cmd::Env(service_cmd)],
            },
            _ => unreachable!("Only needed types implemented"),
        };
        ActionProducts {
            cmds,
            flags: EnumSet::empty(),
        }
    }

    fn all_dirty_flags_reducer(
        state: &mut BasicState,
        action: FakeAction,
    ) -> ActionProducts<MClient> {
        state.called.push(action);
        ActionProducts {
            cmds: vec![],
            flags: EnumSet::all(),
        }
    }

    fn empty_runtime_reducer(_action: BasicRuntimeAction) -> RuntimeProducts<MClient> {
        RuntimeProducts::none()
    }

    #[fixture]
    fn state() -> BasicState {
        BasicState { called: vec![] }
    }

    #[fixture]
    fn config(
        #[default(witness_reducer)] reducers: Reducer<MClient>,
        sender_receiver: MessagingPair,
        state: BasicState,
    ) -> RuntimeConfig<MClient, BasicJobDispatcher> {
        let (messages_tx, messages_rx) = sender_receiver;
        RuntimeConfig {
            services: WitnessServices {
                called: Rc::new(Cell::new(0)),
            },
            state,
            middlewares: vec![],
            reducer: reducers,
            runtime_reducer: empty_runtime_reducer,
            jobs_dispatcher: BasicJobDispatcher {
                called: Rc::new(Cell::new(0)),
            },
            messages_rx,
            messages_tx,
        }
    }

    #[rstest]
    fn received_action_should_call_middlewares_and_reducer(
        mut config: RuntimeConfig<MClient, BasicJobDispatcher>,
    ) {
        let witness = Rc::new(RefCell::new(vec![]));
        config.middlewares.push(Box::new(WitnessMiddleware {
            messages: witness.clone(),
        }));

        let mut runtime = Runtime::new(config);
        runtime.process_message(Message::Action(FakeAction::BasicAction("basic")));

        assert_eq!(*witness.borrow(), vec![FakeAction::BasicAction("basic")]);
        assert_eq!(runtime.state.called, vec![FakeAction::BasicAction("basic")]);
    }

    #[rstest]
    fn direct_command_side_fx_should_execute_product_action(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<MClient, BasicJobDispatcher>,
    ) {
        let mut runtime = Runtime::new(config);
        // An action that will generate a direct command with a pair of (BasicAction) actions as product.
        let sub_action = CmdGeneratingAction(
            "second",
            CmdProduct::Direct(vec![BasicAction("basic2"), BasicAction("basic3")]),
        );
        // An action that will generate a direct command with a pair of actions as product: a basic action and the sub-action above
        let message = Message::Action(CmdGeneratingAction(
            "initial",
            CmdProduct::Direct(vec![BasicAction("basic1"), sub_action]),
        ));
        runtime.process_message(message);

        assert_eq!(runtime.state.called.len(), 5);
        assert_matches!(runtime.state.called[0], CmdGeneratingAction("initial", _));
        assert_matches!(runtime.state.called[1], BasicAction("basic1"));
        assert_matches!(runtime.state.called[2], CmdGeneratingAction("second", _));
        assert_matches!(runtime.state.called[3], BasicAction("basic2"));
        assert_matches!(runtime.state.called[4], BasicAction("basic3"));
    }

    #[rstest]
    fn queue_command_side_fx_should_not_execute_product_action_but_send_it(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<MClient, BasicJobDispatcher>,
    ) {
        let mut runtime = Runtime::new(config);
        // An action that will generate a queued command with a pair of (BasicAction) actions as product.
        let sub_action = CmdGeneratingAction(
            "second",
            CmdProduct::Queue(vec![BasicAction("basic2"), BasicAction("basic3")]),
        );
        // An action that will generate a queued command with a pair of actions as product: a basic action and the sub-action above
        let message = Message::Action(CmdGeneratingAction(
            "initial",
            CmdProduct::Queue(vec![BasicAction("basic1"), sub_action]),
        ));
        runtime.process_message(message);

        assert_eq!(runtime.state.called.len(), 1);
        assert_matches!(runtime.state.called[0], CmdGeneratingAction("initial", _));

        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Message::Action(BasicAction("basic1"))
        );
        assert_matches!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Message::Action(CmdGeneratingAction("second", _))
        );
        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect_err("Should return error, no more pending messages"),
            TryRecvError::Empty
        );
    }

    #[rstest]
    fn async_cmd_should_execute_task_send_result(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<MClient, BasicJobDispatcher>,
    ) {
        let job_witness = config.jobs_dispatcher.called.clone();
        let mut runtime = Runtime::new(config);
        let message = Message::Action(CmdGeneratingAction(
            "async",
            CmdProduct::Async(Box::new(BasicAction("async_result"))),
        ));

        runtime.process_message(message);

        assert_eq!(runtime.state.called.len(), 1);
        assert_matches!(runtime.state.called[0], CmdGeneratingAction("async", _));

        assert_eq!(job_witness.get(), 1);
        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Message::Action(BasicAction("async_result"))
        );
        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect_err("Should return error, no more pending messages"),
            TryRecvError::Empty
        );
    }

    #[rstest]
    fn env_cmd_should_execute_on_environment_send_result(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<MClient, BasicJobDispatcher>,
    ) {
        let services_witness = config.services.called.clone();
        let mut runtime = Runtime::new(config);
        let message = Message::Action(CmdGeneratingAction(
            "services",
            CmdProduct::Env(FakeServiceCommand),
        ));

        runtime.process_message(message);

        assert_eq!(services_witness.get(), 1);

        assert_eq!(runtime.state.called.len(), 1);
        assert_matches!(runtime.state.called[0], CmdGeneratingAction("services", _));

        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect_err("Should return error, no more pending messages"),
            TryRecvError::Empty
        );
    }

    #[rstest]
    fn recursive_action_execution_should_accumulate_flags(
        #[with(flags_producing_reducer)] config: RuntimeConfig<MClient, BasicJobDispatcher>,
    ) {
        let subscriber = WitnessSubscriber::default();
        let op_witness = subscriber.operations.clone();
        let mut runtime = Runtime::new(config);
        runtime.subscribers.push(Box::new(subscriber));

        let action: Vec<FakeAction> = EnumSet::<BasicFlag>::all()
            .into_iter()
            .map(FlagAction)
            .rev()
            .fold(vec![], |acc, flag_action| {
                vec![
                    flag_action,
                    CmdGeneratingAction("chain", CmdProduct::Direct(acc)),
                ]
            });

        runtime.process_message(Action(CmdGeneratingAction(
            "init",
            CmdProduct::Direct(action),
        )));

        let flag_ops: Vec<_> = op_witness
            .borrow()
            .iter()
            .cloned()
            .filter_map(|op| {
                if let Interested(flags) = op {
                    return Some(flags);
                }
                None
            })
            .collect();

        assert_eq!(flag_ops, vec![EnumSet::<BasicFlag>::all()]);
    }

    // todo: cmds can also generate actions
    // todo: test runtime actions

    #[rstest]
    #[case::active_and_interested(
        true,
        true,
        vec![Active, Interested(EnumSet::<BasicFlag>::all()), Notify(BasicState { called: vec![BasicAction("basic")] })]
    )]
    #[case::inactive(false, true, vec![Active])]
    #[case::not_interested(true, false, vec![Active, Interested(EnumSet::<BasicFlag>::all())])]
    #[case::inactive_and_not_interested(false, false, vec![Active])]
    fn received_action_generating_products_should_only_call_active_interested_subscribers(
        #[with(all_dirty_flags_reducer)] config: RuntimeConfig<MClient, BasicJobDispatcher>,
        #[case] active: bool,
        #[case] interested: bool,
        #[case] expected_checks: Vec<WitnessSubscriberChecks>,
    ) {
        let subscriber = WitnessSubscriber {
            should_be_active: active,
            should_be_interested: interested,
            ..WitnessSubscriber::default()
        };
        let op_witness = subscriber.operations.clone();
        let mut runtime = Runtime::new(config);
        runtime.subscribers.push(Box::new(subscriber));

        runtime.process_message(Message::Action(FakeAction::BasicAction("basic")));

        assert_eq!(*op_witness.borrow(), expected_checks);
    }
}
