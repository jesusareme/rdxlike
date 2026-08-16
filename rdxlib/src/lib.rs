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
use tracing::{error, info, warn};
use util::MessageSender;

pub trait Client {
    type State;
    type Action: Send + 'static + Into<Message<Self::Action, Self::RuntimeAction>>;
    type RuntimeAction: Send + 'static + Into<Message<Self::Action, Self::RuntimeAction>>;
    type Flag: enumset::EnumSetType;
    type ServiceCommand: EnvironmentCommand<Action = Self::Action, RuntimeAction = Self::RuntimeAction>;
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

#[allow(clippy::struct_field_names)]
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
                        dirty = EnumSet::all();
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
        use Cmd::{Async, Direct, Env, Queue};
        match cmd {
            Direct(new_work_actions) => {
                pending.extend(new_work_actions.into_iter().map(Into::into));
            }

            Queue(new_work_actions) => {
                for action in new_work_actions {
                    if messages_tx.send_message(action).is_err() {
                        warn!("Action was never sent because receiver was dropped");
                        break;
                    }
                }
            }

            Async(task) => {
                let messages_tx = messages_tx.clone();
                jobs_dispatcher.work_on(Box::new(move || {
                    let action = (task.job)();
                    if messages_tx.send_message(action).is_err() {
                        warn!("Action was never sent because receiver was dropped");
                    }
                }));
            }

            Env(job) => {
                job.process(services, pending, messages_tx);
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
    use std::sync::mpsc::TryRecvError;
    use std::sync::{Arc, RwLock, mpsc};

    struct TestClient;
    impl Client for TestClient {
        type State = Vec<TestAction>;
        type Action = TestAction;
        type RuntimeAction = TestRuntimeAction;
        type Flag = TestFlag;
        type ServiceCommand = TestServiceCommand;
    }

    #[derive(Debug, PartialEq, Clone)]
    enum CmdProduct {
        Direct(Vec<TestAction>),
        Queue(Vec<TestAction>),
        Async(Box<TestAction>),
        Env(TestServiceCommand),
    }

    #[derive(Debug, PartialEq, Clone)]
    enum TestAction {
        BasicAction(&'static str),
        FlagAction(TestFlag),
        CmdGeneratingAction(&'static str, CmdProduct),
    }

    impl Into<Message<TestAction, TestRuntimeAction>> for TestAction {
        fn into(self) -> Message<TestAction, TestRuntimeAction> {
            Action(self)
        }
    }

    #[derive(Debug, PartialEq)]
    enum TestRuntimeAction {
        CreateSubscriber(WitnessSubscriber),
        CreateActions(Vec<TestAction>),
    }

    impl Into<Message<TestAction, TestRuntimeAction>> for TestRuntimeAction {
        fn into(self) -> Message<TestAction, TestRuntimeAction> {
            Message::Runtime(self)
        }
    }

    #[derive(EnumSetType, Debug)]
    enum TestFlag {
        A,
        B,
        C,
        D,
        E,
    }

    #[derive(Debug, PartialEq, Clone)]
    enum TestServiceCommand {
        Increment,
        IncrementAnd(Vec<TestAction>, Vec<TestAction>),
    }
    impl EnvironmentCommand for TestServiceCommand {
        type Environment = Rc<Cell<u32>>;
        type Action = TestAction;
        type RuntimeAction = TestRuntimeAction;

        fn process(
            self,
            env: &mut Self::Environment,
            pending: &mut VecDeque<Message<TestAction, TestRuntimeAction>>,
            messages_tx: &MessageSender<Message<TestAction, TestRuntimeAction>>,
        ) {
            env.update(|c| c + 1);
            if let TestServiceCommand::IncrementAnd(direct, queued) = self {
                pending.extend(direct.into_iter().map(Into::into));
                for action in queued {
                    messages_tx
                        .send_message(action)
                        .expect("Send should not fail in tests");
                }
            }
        }
    }

    struct WitnessJobDispatcher {
        pub called: Rc<Cell<u32>>,
    }

    impl JobsDispatcher for WitnessJobDispatcher {
        fn work_on(&self, job: BoxedThreadPoolJob) {
            self.called.update(|v| v + 1);
            job()
        }
    }

    type MessageWitness = Rc<RefCell<Vec<TestAction>>>;
    struct WitnessMiddleware {
        messages: MessageWitness,
    }
    impl ChainableMiddleware<TestClient> for WitnessMiddleware {
        fn execute(
            &mut self,
            state: &mut Vec<TestAction>,
            action: TestAction,
            mut next: Next<TestClient>,
        ) -> ActionProducts<TestClient> {
            self.messages.borrow_mut().push(action.clone());
            next.run(state, action)
        }
    }

    type OperationsLog = Arc<RwLock<Vec<WitnessSubscriberChecks>>>;

    #[derive(Debug)]
    struct WitnessSubscriber {
        name: &'static str,
        operations: OperationsLog,
        should_be_interested: bool,
        should_be_active: bool,
        should_notify_error: bool,
    }

    impl PartialEq for WitnessSubscriber {
        fn eq(&self, other: &Self) -> bool {
            self.name.eq(other.name)
        }
    }

    #[derive(PartialEq, Debug, Clone)]
    enum WitnessSubscriberChecks {
        Interested(EnumSet<TestFlag>),
        Notify(Vec<TestAction>),
        Active,
    }
    use crate::cmd::AsyncTask;
    use crate::messages::Message::Action;
    use crate::primitives::BoxedThreadPoolJob;
    use crate::tests::TestAction::{BasicAction, CmdGeneratingAction, FlagAction};
    use WitnessSubscriberChecks::{Active, Interested, Notify};

    impl Default for WitnessSubscriber {
        fn default() -> Self {
            WitnessSubscriber {
                name: "",
                operations: OperationsLog::default(),
                should_be_interested: true,
                should_be_active: true,
                should_notify_error: false,
            }
        }
    }

    impl Subscriber for WitnessSubscriber {
        type State = Vec<TestAction>;
        type Flag = TestFlag;

        fn notify(&mut self, new_state: &Self::State) -> Result<(), SubscriberError> {
            self.operations
                .write()
                .unwrap()
                .push(Notify(new_state.clone()));
            if self.should_notify_error {
                Err(SubscriberError::MissingState)
            } else {
                Ok(())
            }
        }

        fn is_active(&self) -> bool {
            self.operations.write().unwrap().push(Active);
            self.should_be_active
        }

        fn interested_in(&self, offered: &EnumSet<Self::Flag>) -> bool {
            self.operations
                .write()
                .unwrap()
                .push(Interested(offered.clone()));
            self.should_be_interested
        }
    }

    type MessagingPair = (
        MessageSender<Message<TestAction, TestRuntimeAction>>,
        Receiver<Message<TestAction, TestRuntimeAction>>,
    );
    #[fixture]
    fn sender_receiver() -> MessagingPair {
        let (sender, receiver) = mpsc::channel();
        (MessageSender::new(sender), receiver)
    }

    fn witness_reducer(
        state: &mut Vec<TestAction>,
        action: TestAction,
    ) -> ActionProducts<TestClient> {
        state.push(action);
        ActionProducts::none()
    }

    fn flags_producing_reducer(
        state: &mut Vec<TestAction>,
        action: TestAction,
    ) -> ActionProducts<TestClient> {
        state.push(action.clone());
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
        state: &mut Vec<TestAction>,
        action: TestAction,
    ) -> ActionProducts<TestClient> {
        state.push(action.clone());

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
        state: &mut Vec<TestAction>,
        action: TestAction,
    ) -> ActionProducts<TestClient> {
        state.push(action);
        ActionProducts {
            cmds: vec![],
            flags: EnumSet::all(),
        }
    }

    fn empty_runtime_reducer(_action: TestRuntimeAction) -> RuntimeProducts<TestClient> {
        RuntimeProducts::none()
    }

    fn products_producing_runtime_reducer(
        action: TestRuntimeAction,
    ) -> RuntimeProducts<TestClient> {
        match action {
            TestRuntimeAction::CreateSubscriber(subscriber) => RuntimeProducts {
                subscriber: Some(Box::new(subscriber)),
                actions: vec![],
            },

            TestRuntimeAction::CreateActions(actions) => RuntimeProducts {
                subscriber: None,
                actions,
            },
        }
    }

    #[fixture]
    fn state() -> Vec<TestAction> {
        vec![]
    }

    #[fixture]
    fn config(
        #[default(witness_reducer)] reducer: Reducer<TestClient>,
        #[default(empty_runtime_reducer)] runtime_reducer: RuntimeReducer<TestClient>,
        sender_receiver: MessagingPair,
        state: Vec<TestAction>,
    ) -> RuntimeConfig<TestClient, WitnessJobDispatcher> {
        let (messages_tx, messages_rx) = sender_receiver;
        RuntimeConfig {
            services: Default::default(),
            state,
            middlewares: vec![],
            reducer,
            runtime_reducer,
            jobs_dispatcher: WitnessJobDispatcher {
                called: Default::default(),
            },
            messages_rx,
            messages_tx,
        }
    }

    #[rstest]
    fn received_action_should_call_middlewares_and_reducer(
        mut config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let witness = MessageWitness::default();
        config.middlewares.push(Box::new(WitnessMiddleware {
            messages: witness.clone(),
        }));

        let mut runtime = Runtime::new(config);
        runtime.process_message(Action(BasicAction("basic")));

        assert_eq!(*witness.borrow(), vec![BasicAction("basic")]);
        assert_eq!(runtime.state, vec![BasicAction("basic")]);
    }

    #[rstest]
    fn direct_command_side_fx_should_execute_product_action(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let mut runtime = Runtime::new(config);
        // An action that will generate a direct command with a pair of (BasicAction) actions as product.
        let sub_action = CmdGeneratingAction(
            "second",
            CmdProduct::Direct(vec![BasicAction("basic2"), BasicAction("basic3")]),
        );
        // An action that will generate a direct command with a pair of actions as product: a basic action and the sub-action above
        let message = Action(CmdGeneratingAction(
            "initial",
            CmdProduct::Direct(vec![BasicAction("basic1"), sub_action]),
        ));
        runtime.process_message(message);

        assert_eq!(runtime.state.len(), 5);
        assert_matches!(runtime.state[0], CmdGeneratingAction("initial", _));
        assert_matches!(runtime.state[1], BasicAction("basic1"));
        assert_matches!(runtime.state[2], CmdGeneratingAction("second", _));
        assert_matches!(runtime.state[3], BasicAction("basic2"));
        assert_matches!(runtime.state[4], BasicAction("basic3"));
    }

    #[rstest]
    fn queue_command_side_fx_should_not_execute_product_action_but_send_it(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let mut runtime = Runtime::new(config);
        // An action that will generate a queued command with a pair of (BasicAction) actions as product.
        let sub_action = CmdGeneratingAction(
            "second",
            CmdProduct::Queue(vec![BasicAction("basic2"), BasicAction("basic3")]),
        );
        // An action that will generate a queued command with a pair of actions as product: a basic action and the sub-action above
        let message = Action(CmdGeneratingAction(
            "initial",
            CmdProduct::Queue(vec![BasicAction("basic1"), sub_action]),
        ));
        runtime.process_message(message);

        assert_eq!(runtime.state.len(), 1);
        assert_matches!(runtime.state[0], CmdGeneratingAction("initial", _));

        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Action(BasicAction("basic1"))
        );
        assert_matches!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Action(CmdGeneratingAction("second", _))
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
        #[with(cmd_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let job_witness = config.jobs_dispatcher.called.clone();
        let mut runtime = Runtime::new(config);
        let message = Action(CmdGeneratingAction(
            "async",
            CmdProduct::Async(Box::new(BasicAction("async_result"))),
        ));

        runtime.process_message(message);

        assert_eq!(runtime.state.len(), 1);
        assert_matches!(runtime.state[0], CmdGeneratingAction("async", _));

        assert_eq!(job_witness.get(), 1);
        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Action(BasicAction("async_result"))
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
        #[with(cmd_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let services_witness = config.services.clone();
        let mut runtime = Runtime::new(config);
        let message = Action(CmdGeneratingAction(
            "services",
            CmdProduct::Env(TestServiceCommand::Increment),
        ));

        runtime.process_message(message);

        assert_eq!(services_witness.get(), 1);

        assert_eq!(runtime.state.len(), 1);
        assert_matches!(runtime.state[0], CmdGeneratingAction("services", _));

        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect_err("Should return error, no more pending messages"),
            TryRecvError::Empty
        );
    }

    #[rstest]
    fn env_cmd_should_execute_its_direct_actions_and_send_its_queued_ones(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let services_witness = config.services.clone();
        let mut runtime = Runtime::new(config);
        let message = Action(CmdGeneratingAction(
            "services",
            CmdProduct::Env(TestServiceCommand::IncrementAnd(
                vec![BasicAction("direct")],
                vec![BasicAction("queued")],
            )),
        ));

        runtime.process_message(message);

        assert_eq!(services_witness.get(), 1);

        assert_eq!(runtime.state.len(), 2);
        assert_matches!(runtime.state[0], CmdGeneratingAction("services", _));
        assert_matches!(runtime.state[1], BasicAction("direct"));

        assert_eq!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Action(BasicAction("queued"))
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
    fn recursive_action_execution_should_accumulate_flags(
        #[with(flags_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let subscriber = WitnessSubscriber::default();
        let op_witness = subscriber.operations.clone();
        let mut runtime = Runtime::new(config);
        runtime.subscribers.push(Box::new(subscriber));

        let action: Vec<TestAction> = EnumSet::<TestFlag>::all()
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
            .read()
            .unwrap()
            .iter()
            .cloned()
            .filter_map(|op| {
                if let Interested(flags) = op {
                    return Some(flags);
                }
                None
            })
            .collect();

        assert_eq!(flag_ops, vec![EnumSet::<TestFlag>::all()]);
    }

    #[rstest]
    fn runtime_action_subscriber_product_should_add_subscriber_subscriber_is_called(
        #[with(witness_reducer, products_producing_runtime_reducer)] config: RuntimeConfig<
            TestClient,
            WitnessJobDispatcher,
        >,
    ) {
        let subscriber = WitnessSubscriber::default();
        let witness_op = subscriber.operations.clone();

        let mut runtime = Runtime::new(config);
        runtime.process_message(Message::Runtime(TestRuntimeAction::CreateSubscriber(
            subscriber,
        )));

        assert_eq!(runtime.subscribers.len(), 1);
        assert_eq!(
            *witness_op.read().unwrap(),
            vec![Active, Interested(EnumSet::all()), Notify(vec![])]
        );
    }

    #[rstest]
    fn runtime_action_action_product_should_add_action_and_execute_it_immediately(
        #[with(cmd_producing_reducer, products_producing_runtime_reducer)] config: RuntimeConfig<
            TestClient,
            WitnessJobDispatcher,
        >,
    ) {
        let mut runtime = Runtime::new(config);
        let indirect_action =
            CmdGeneratingAction("indirect", CmdProduct::Direct(vec![BasicAction("3")]));
        let resulting_actions = vec![BasicAction("1"), BasicAction("2"), indirect_action];
        runtime.process_message(Message::Runtime(TestRuntimeAction::CreateActions(
            resulting_actions.clone(),
        )));

        let mut executed_actions = resulting_actions;
        executed_actions.push(BasicAction("3"));
        assert_eq!(runtime.state, executed_actions);
    }

    #[rstest]
    #[case::active_and_interested(
        true,
        true,
        vec![Active, Interested(EnumSet::<TestFlag>::all()), Notify(vec![BasicAction("basic")])]
    )]
    #[case::inactive(false, true, vec![Active])]
    #[case::not_interested(true, false, vec![Active, Interested(EnumSet::<TestFlag>::all())])]
    #[case::inactive_and_not_interested(false, false, vec![Active])]
    fn received_action_generating_products_should_only_call_active_interested_subscribers(
        #[with(all_dirty_flags_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
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

        runtime.process_message(Action(BasicAction("basic")));

        assert_eq!(*op_witness.read().unwrap(), expected_checks);
    }
}
