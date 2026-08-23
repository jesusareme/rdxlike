//! A Redux-like runtime for Rust clients.
//!
//! This crate implements a free-style version of a Redux, Elm-inspired, runtime, designed to handle
//! typical workloads and features required by common mobile apps. It is not specifically focused on
//! performance but, at the same time, tries to stay away from unnecessary memory allocation and
//! executes potentially costly jobs concurrently whenever possible.
//!
//!
//! # Architecture
//!
//! A single `Runtime` owns the application state and drives a loop over incoming
//! [`Message`]s:
//!
//! - [`Message`]s are the incoming data and instructions basic unit, used for both,
//!   runtime-related messages and client-domain messages.
//! - [`Message::Action`] represents client-domain messages and runs through the [`middleware`]
//!   chain to be processed into [`ActionProducts`]s.
//! - [`Reducer`] is the main responsible for internal state mutation and returns [`ActionProducts`]:
//!   side effects to run ([`Cmd`]) plus dirty [`Client::Flag`]s describing which parts of the state changed.
//!   This is the last link executed in the [`ChainableMiddleware`]s chain.
//! - [`ChainableMiddleware`] represents a `middleware` implementation, in some ways similar to
//!   a [`Reducer`], in that they can read or modify any behavior around [`Reducer`], such as observe
//!   or mutate state, actions or products, before or after [`Reducer`] execution.
//!   Their effects can be composed by chaining several `middlewares` together, hence the Trait's name.
//! - [`Cmd`]s can be seen as operations that needs out of the runtime data or processing, generally executed
//!   as side effect of an [`Message::Action`], usually as an asynchronous and/or long-running operation (think:
//!   save a file, get some data from a remote API, start reading from a sensor according to a schedule, etc.)
//! - [`Message::Runtime`] represents runtime related requests to the `Runtime`. They run through
//!   the [`RuntimeReducer`] and return [`RuntimeProducts`]: usually a new [`Subscriber`] to register
//!   and/or follow-up actions.
//! - [`Subscriber`]s are the way out of the `Runtime`: they can produce derived artifacts,
//!   useful for the client, based on a read-only copy of the [`State`] (previously mutated
//!   by [`middleware`]s and [`Reducer`]). After a message and everything it cascaded into have
//!   been processed, subscribers interested in the accumulated dirty flags are notified once. A reference
//!   implementation of a [`Subscriber`] specifically addressed at generating client views is
//!   include as [`OutputSubscriber`]
//!
//! ### Threading model
//! Runtime should usually be run by the Client on its own thread to avoid any blocking effect on the calling Client, but
//! this crate doesn't enforce this in any way. The reason for this suggestion is messages dispatching
//! and state mutation, and product dispatching, all run on the main Runtime thread. Specifically,
//! [`Client::State`] is local to this thread, and only the minimum required slices of it are cloned
//! or copied before being sent into [`Subscriber`]s.
//!
//! Then, most of subsequent processes are ran on their own threads.
//! Side Effects [`Cmd::Async`]s are dispatched to a non-blocking [`JobsDispatcher`], implemented as
//! a [`ThreadPool`] on the included [`primitives`] module. [`Cmd::Env`]s are dispatched to the Client's
//! [`Environment`] facilities, which are encouraged to be non-blocking whenever possible, and
//! provided a [`Message`]s [`MessageSender`] to enable easy asynchronous feedback into the Runtime.
//!
//! Finally, each [`Subscriber`] runs on its own dedicated thread, making concurrent state derivation possible. Output
//! logic for these subscribers is run on the subscriber's own thread, so Client should not have any
//! expectation on updates coming from some specific thread.
//!
//! Most of this threading model is implemented by using message passing between threads via channels.
//! Some other primitives, like [`Mutex`]es or [`RwLock`]s, are sparsely used to enable mechanisms
//! such as cooperative cancellation of asynchronous tasks, or `mpmc` channels.
//!
//! # Getting started
//!
//! A [`Client`] is the type-level description of an application: its state, its actions, its dirty
//! flags and its environment commands. Everything else in this crate is generic over it.
//!
//! [`RuntimeRunner::create`] is the starting point: it prepares communication to [`runtime`] and hands
//! back the [`RuntimeHandle`] together with the [`RuntimeRunner`] itself. The
//! [`RuntimeHandle`] stays with the client to create [`MessageSender`]s ([`RuntimeHandle::create_sender`])
//! — which accept [`Message`]s as the only asynchronous way to feed Runtime state and products, and
//! can be handed to `Runtime` dependencies such as Services — and to stop the Runtime through
//! [`RuntimeHandle::cancel`] (or by being dropped). Filling a [`RuntimeConfig`] and calling
//! [`RuntimeRunner::run`] then builds the `Runtime` and starts running its loop.
//!
//! ```
//! use enumset::{EnumSet, EnumSetType};
//! use rdxlib::cmd::EnvironmentCommand;
//! use rdxlib::messages::Message;
//! use rdxlib::primitives::ThreadPool;
//! use rdxlib::products::{ActionProducts, RuntimeProducts};
//! use rdxlib::subscribers::{Subscriber, SubscriberError};
//! use rdxlib::util::{MessageSend, MessageSender};
//! use rdxlib::{Client, RuntimeConfig, RuntimeRunner};
//! use std::sync::{Arc, Mutex};
//!
//! enum Counter {}
//! impl Client for Counter {
//!     type State = i32;
//!     type Action = Add;
//!     type RuntimeAction = AddSubscriber;
//!     type Flag = Changed;
//!     type Environment = ();
//!     type ServiceCommand = EmptyServices;
//! }
//!
//! struct Add(i32);
//! impl From<Add> for Message<Add, AddSubscriber> {
//!     fn from(action: Add) -> Self {
//!         Message::Action(action)
//!     }
//! }
//!
//! struct AddSubscriber(TotalsObserver);
//! impl From<AddSubscriber> for Message<Add, AddSubscriber> {
//!     fn from(action: AddSubscriber) -> Self {
//!         Message::Runtime(action)
//!     }
//! }
//!
//! #[derive(EnumSetType)]
//! enum Changed {
//!     Total,
//! }
//!
//! struct EmptyServices;
//! impl EnvironmentCommand for EmptyServices {
//!     type Client = Counter;
//!
//!     fn process(
//!         self,
//!         _env: &mut (),
//!         _messages_tx: &MessageSender<Message<Add, AddSubscriber>>,
//!     ) -> Vec<Message<Add, AddSubscriber>> {
//!         vec![]
//!     }
//! }
//!
//! struct TotalsObserver {
//!     totals: Arc<Mutex<Vec<i32>>>,
//! }
//! impl Subscriber for TotalsObserver {
//!     type Client = Counter;
//!
//!     fn notify(&mut self, new_state: &i32) -> Result<(), SubscriberError> {
//!         self.totals.lock().unwrap().push(*new_state);
//!         Ok(())
//!     }
//!
//!     fn is_active(&self) -> bool {
//!         true
//!     }
//!
//!     fn interested_in(&self, offered: &EnumSet<Changed>) -> bool {
//!         true
//!     }
//! }
//!
//! fn reducer(total: &mut i32, Add(amount): Add) -> ActionProducts<Counter> {
//!     *total += amount;
//!     ActionProducts::none().with_dirty(Changed::Total)
//! }
//!
//! fn runtime_reducer(AddSubscriber(reporter): AddSubscriber) -> RuntimeProducts<Counter> {
//!     RuntimeProducts::subscriber(reporter)
//! }
//!
//! let totals = Arc::new(Mutex::new(vec![]));
//!
//! let (mut handle, runner) = RuntimeRunner::create();
//! let sender = handle.create_sender();
//!
//! sender.send_message(AddSubscriber(TotalsObserver { totals: totals.clone() })).unwrap();
//! sender.send_message(Add(2)).unwrap();
//! sender.send_message(Add(3)).unwrap();
//!
//! handle.cancel().expect("runtime is ready to work, this should not return error");
//! let final_state = runner.run(RuntimeConfig {
//!     services: (),
//!     state: 0,
//!     middlewares: vec![],
//!     reducer,
//!     runtime_reducer,
//!     jobs_dispatcher: ThreadPool::new(1).expect("one worker thread"),
//! }).expect("no fatal error, so the final state comes back");
//!
//! assert_eq!(final_state, 5);
//! assert_eq!(*totals.lock().unwrap(), vec![0, 2, 5]);
//! assert!(handle.cancel().is_err(), "Now it should return error because was already cancelled");
//! ```
//!
//! Here, the `AddSubscriber` runtime action registers `TotalsObserver`, which is only interested
//! in registering each final state, `[0, 2, 5]`.
//!
//! [`RuntimeHandle::cancel`] closes the runtime's input: whatever is already queued is still
//! processed and [`RuntimeRunner::run`] returns the final state once it drains. Dropping the handle does
//! the same, so a client that simply lets it go out of scope shuts the runtime down too.

pub mod cmd;
pub mod error;
pub mod messages;
pub mod middleware;
pub mod primitives;
pub mod products;
pub mod subscribers;
pub mod util;

use crate::cmd::{Cmd, EnvironmentCommand};
use crate::messages::{Message, Operation};
use crate::middleware::{ChainableMiddleware, MiddlewareStore};
use crate::primitives::{JobsDispatcher, ThreadPool};
use crate::products::{ActionProducts, RuntimeProducts};
use crate::util::MessageSend;
use Operation::Run;
use enumset::EnumSet;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use subscribers::Subscriber;
use tracing::{error, info, warn};
use util::MessageSender;
use crate::error::{RuntimeError, RuntimeFatalError};
use crate::messages::Operation::Stop;

/// The set of types a client application plugs into the runtime.
///
/// Implemented by a marker style trait; every generic item in this crate is
/// parameterized by it so a client only has to provide the corresponding type.
pub trait Client {
    /// The whole application model owned by the `Runtime`.
    ///
    /// Client is free to implement and organize state as needed, given Subscribers can observe a
    /// complete (read-only) copy of the state.
    type State;

    /// Business actions, the only way to change [`Client::State`], via [`Reducer`]
    type Action: Send + 'static + Into<ClientMessage<Self>>;

    /// Actions that act on the runtime itself instead of on the model.
    /// Currently used to add [`Subscriber`]s as well as trigger new [`Action`]s,
    /// via [`RuntimeReducer`]
    type RuntimeAction: Send + 'static + Into<ClientMessage<Self>>;

    /// Coarse "what changed" markers [`Reducer`] returns so subscribers can cheaply skip
    /// state they do not care about.
    ///
    /// Most useful when used to model disconnected parts of client´s state that are not usually
    /// changing simultaneously or causally.
    type Flag: enumset::EnumSetType;

    /// The client's set of services, owned by the runtime and passed to every
    /// [`Client::ServiceCommand`] by mutable reference.
    type Environment;

    /// Commands that need access to the client's environment (services, I/O, handles).
    type ServiceCommand: EnvironmentCommand<Client = Self>;
}

/// [`Message`] type a given [`Client`] exchanges with its `Runtime`.
/// Subtypes are [`Message::Action`] for model related messages, and [`Message::Runtime`] for runtime-related
/// actions such as adding a Subscriber corresponding with a new view created on client side.
pub type ClientMessage<C> = Message<<C as Client>::Action, <C as Client>::RuntimeAction>;

/// The client's single entry point for mutating state.
///
/// Runs after the whole middleware chain and returns the side effects and dirty flags the
/// runtime should act on.
///
/// It should not block, access I/O and, in general, avoid costly operations such as cloning big data structures.
pub type Reducer<C> = fn(&mut <C as Client>::State, <C as Client>::Action) -> ActionProducts<C>;

/// Handles [`Message::Runtime`] messages, which never touch the state but can affect the `Runtime` execution
/// environment through the types offered on [`RuntimeProducts`].
pub type RuntimeReducer<C> = fn(<C as Client>::RuntimeAction) -> RuntimeProducts<C>;

/// Everything the `Runtime` needs to start, which should be provided by the client.
pub struct RuntimeConfig<C: Client, JD: JobsDispatcher = ThreadPool> {
    /// The client's environment, handed to every [`Cmd::Env`] command for the duration of the Runtime.
    pub services: C::Environment,

    /// Initial application state.
    pub state: C::State,

    /// Middlewares wrapping the reducer and running in registration order.
    pub middlewares: Vec<Box<dyn ChainableMiddleware<C>>>,

    /// The client reducer sitting at the end of the middleware chain.
    pub reducer: Reducer<C>,

    /// The reducer for runtime actions.
    pub runtime_reducer: RuntimeReducer<C>,

    /// Where [`Cmd::Async`] tasks are sent to run off the runtime thread.
    pub jobs_dispatcher: JD,
}

/// Owner of the client state and the loop that processes messages it sends.
///
/// Built and driven by [`RuntimeRunner::run`], which blocks until [`RuntimeHandle`] is dropped or
/// [`RuntimeHandle::cancel`] is called, and then returns the final state it was holding.
///
/// Runtime is, by design, not thread-safe (no [`Send`]) and blocks when running so it is expected to be built
/// and run on its own thread. Communication facilities such as [`MessageSender`] and
/// [`RuntimeHandle`] are thread-safe and provide the ways to communicate in a safe way with Runtime by message-passing.
#[allow(clippy::struct_field_names)]
pub(crate) struct Runtime<C: Client, JD: JobsDispatcher = ThreadPool> {
    services: C::Environment,
    state: C::State,
    middlewares: MiddlewareStore<C>,
    subscribers: Vec<Box<dyn Subscriber<Client = C>>>,
    messages_rx: Receiver<Operation<ClientMessage<C>>>,
    messages_tx: MessageSender<ClientMessage<C>>,
    runtime_reducer: RuntimeReducer<C>,
    jobs_dispatcher: JD,
}

/// Small utility that owns the Runtime's message channel until [`Self::run`]
/// turns it into a live `Runtime`.
///
/// `Runtime` is `!Send`, [`RuntimeRunner`] is `Send` so that final Runtime execution can be
/// easily moved to the final destination thread that will own the Runtime's loop.
///
/// Create one with [`RuntimeRunner::create`], which also hands back the Runtime's
/// [`RuntimeHandle`]. Keep the [`RuntimeHandle`] on the Client's controlling thread to create
/// [`MessageSender`]s ([`RuntimeHandle::create_sender`]) and to stop
/// the loop ([`RuntimeHandle::cancel`] or by dropping it).
pub struct RuntimeRunner<C: Client> {
    sender: Sender<Operation<ClientMessage<C>>>,
    receiver: Receiver<Operation<ClientMessage<C>>>,
}

impl<C: Client> RuntimeRunner<C> {
    /// Prepares the `Runtime`, returning its single [`RuntimeHandle`] and the [`RuntimeRunner`]
    /// that will build and finally run it .
    #[must_use]
    pub fn create() -> (RuntimeHandle<C>, RuntimeRunner<C>) {
        let (sender, receiver) = mpsc::channel::<Operation<ClientMessage<C>>>();
        let handle = RuntimeHandle::from_sender(sender.clone());
        (handle, RuntimeRunner { sender, receiver })
    }

    /// Builds the `Runtime` from `config` and drives its loop, returning the final state once it
    /// drains. `Runtime` is `!Send`, so this assembles and runs it on the current thread.
    ///
    /// Messages sent before [`Self::run`] are queued and delivered as soon as the loop starts.
    ///
    /// # Errors
    /// Returns a [`RuntimeFatalError`] if the run loop hit an unrecoverable error.
    pub fn run<JD: JobsDispatcher>(
        self,
        config: RuntimeConfig<C, JD>,
    ) -> Result<C::State, RuntimeFatalError> {
        self.into_runtime(config).run()
    }

    fn into_runtime<JD: JobsDispatcher>(self, config: RuntimeConfig<C, JD>) -> Runtime<C, JD> {
        Runtime {
            services: config.services,
            state: config.state,
            middlewares: MiddlewareStore::new(config.middlewares, config.reducer),
            subscribers: vec![],
            messages_rx: self.receiver,
            messages_tx: MessageSender::from_sender(self.sender),
            runtime_reducer: config.runtime_reducer,
            jobs_dispatcher: config.jobs_dispatcher,
        }
    }
}

impl<C: Client, JD: JobsDispatcher> Runtime<C, JD> {
    /// Starts running the run-loop and processing incoming messages.
    /// Blocks until the run-loop is canceled via [`RuntimeHandle`].
    /// Returns the final `C::State` if actively stopped, or [`RuntimeHandle`] dropped by Client.
    ///
    /// # Errors
    /// Returns a [`RuntimeFatalError`] if Runtime suffered from an unrecoverable error. State is
    /// dropped in that case, as it may have been left incoherent..
    pub fn run(mut self) -> Result<C::State, RuntimeFatalError> {
        info!("Started run loop for RdxLib...");

        while let Ok(op) = self.messages_rx.recv() {
            match op {
                Run(message) => self.process_message(message),
                Stop(Some(source)) => return Err(RuntimeFatalError(source)),
                Stop(None) => break,
            }
        }

        info!("Finished run loop for RdxLib...");
        Ok(self.state)
    }

    fn process_message(&mut self, message: ClientMessage<C>) {
        let mut pending: VecDeque<ClientMessage<C>> = VecDeque::new();
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
        subscribers: &mut Vec<Box<dyn Subscriber<Client = C>>>,
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
        services: &mut C::Environment,
        jobs_dispatcher: &JD,
        messages_tx: &MessageSender<ClientMessage<C>>,
        pending: &mut VecDeque<ClientMessage<C>>,
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
                pending.extend(job.process(services, messages_tx));
            }
        }
    }
}

/// Unique handle to manage Runtime event loop cancellation and replicate Runtime message senders.
///
/// Cancellation can be forced by dropping this handle or calling [`Self::cancel`]. Runtime will
/// drain its messages queue and then stop processing, causing [`RuntimeRunner::run`] to return without
/// errors (if pending messages did not cause any unrecoverable error).
pub struct RuntimeHandle<C: Client> {
    sender: Sender<Operation<ClientMessage<C>>>,
}

impl<C: Client> RuntimeHandle<C>
{
    /// Creates a [`MessageSender`] to address messages ([`Client::Action`] or
    /// [`Client::RuntimeAction`]) to `Runtime`.
    ///
    /// [`MessageSender`]s can be obtained by cloning other instances too.
    #[must_use]
    pub fn create_sender(&self) -> MessageSender<ClientMessage<C>> {
        MessageSender::new(self.sender.clone())
    }

    /// Cancels `Runtime` event loop, causing it to drain its messages queue and return from
    /// [`RuntimeRunner::run`]
    ///
    /// # Errors
    /// Returns [`RuntimeError`] if target `Runtime` is no longer running.
    pub fn cancel(&self) -> Result<(), RuntimeError> {
        self.sender.send(Stop(None)).map_err(|_| RuntimeError::NoLongerRunning)
    }

    pub(crate) fn from_sender(sender: Sender<Operation<ClientMessage<C>>>) -> Self {
        RuntimeHandle {
            sender,
        }
    }
}

impl<C: Client> Drop for RuntimeHandle<C> {
    fn drop(&mut self) {
        _ = self.sender.send(Stop(None));
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
    use std::sync::{Arc, RwLock};

    struct TestClient;
    impl Client for TestClient {
        type State = Vec<TestAction>;
        type Action = TestAction;
        type RuntimeAction = TestRuntimeAction;
        type Flag = TestFlag;
        type Environment = Rc<Cell<u32>>;
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
        type Client = TestClient;

        fn process(
            self,
            env: &mut Rc<Cell<u32>>,
            messages_tx: &MessageSender<Message<TestAction, TestRuntimeAction>>,
        ) -> Vec<Message<TestAction, TestRuntimeAction>> {
            env.update(|c| c + 1);
            match self {
                TestServiceCommand::IncrementAnd(direct, queued) => {
                    for action in queued {
                        messages_tx
                            .send_message(action)
                            .expect("Send should not fail in tests");
                    }
                    direct.into_iter().map(Into::into).collect()
                }
                TestServiceCommand::Increment => vec![],
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
            next: &mut dyn Next<TestClient>,
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
        type Client = TestClient;

        fn notify(&mut self, new_state: &Vec<TestAction>) -> Result<(), SubscriberError> {
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

        fn interested_in(&self, offered: &EnumSet<TestFlag>) -> bool {
            self.operations
                .write()
                .unwrap()
                .push(Interested(offered.clone()));
            self.should_be_interested
        }
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
                    id: "test_async".to_string(),
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
        state: Vec<TestAction>,
    ) -> RuntimeConfig<TestClient, WitnessJobDispatcher> {
        RuntimeConfig {
            services: Default::default(),
            state,
            middlewares: vec![],
            reducer,
            runtime_reducer,
            jobs_dispatcher: WitnessJobDispatcher {
                called: Default::default(),
            },
        }
    }

    type TestRuntime = Runtime<TestClient, WitnessJobDispatcher>;

    fn started_runtime(
        config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) -> (TestRuntime, RuntimeHandle<TestClient>) {
        let (handle, runner) = RuntimeRunner::create();
        let runtime = runner.into_runtime(config);
        (runtime, handle)
    }

    #[rstest]
    fn received_action_should_call_middlewares_and_reducer(
        mut config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let witness = MessageWitness::default();
        config.middlewares.push(Box::new(WitnessMiddleware {
            messages: witness.clone(),
        }));

        let (mut runtime, _handle) = started_runtime(config);
        runtime.process_message(Action(BasicAction("basic")));

        assert_eq!(*witness.borrow(), vec![BasicAction("basic")]);
        assert_eq!(runtime.state, vec![BasicAction("basic")]);
    }

    #[rstest]
    fn direct_command_side_fx_should_execute_product_action(
        #[with(cmd_producing_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let (mut runtime, _handle) = started_runtime(config);
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
        let (mut runtime, _handle) = started_runtime(config);
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

        assert_matches!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Run(Action(BasicAction("basic1")))
        );
        assert_matches!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Run(Action(CmdGeneratingAction("second", _)))
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
        let (mut runtime, _handle) = started_runtime(config);
        let message = Action(CmdGeneratingAction(
            "async",
            CmdProduct::Async(Box::new(BasicAction("async_result"))),
        ));

        runtime.process_message(message);

        assert_eq!(runtime.state.len(), 1);
        assert_matches!(runtime.state[0], CmdGeneratingAction("async", _));

        assert_eq!(job_witness.get(), 1);
        assert_matches!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Run(Action(BasicAction("async_result")))
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
        let (mut runtime, _handle) = started_runtime(config);
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
        let (mut runtime, _handle) = started_runtime(config);
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

        assert_matches!(
            runtime
                .messages_rx
                .try_recv()
                .expect("Should not fail, next message available"),
            Run(Action(BasicAction("queued")))
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
        let (mut runtime, _handle) = started_runtime(config);
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

        let (mut runtime, _handle) = started_runtime(config);
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
        let (mut runtime, _handle) = started_runtime(config);
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
        let (mut runtime, _handle) = started_runtime(config);
        runtime.subscribers.push(Box::new(subscriber));

        runtime.process_message(Action(BasicAction("basic")));

        assert_eq!(*op_witness.read().unwrap(), expected_checks);
    }

    #[rstest]
    fn cancelled_runtime_should_process_pending_messages_and_end_run_loop(
        #[with(all_dirty_flags_reducer)] config: RuntimeConfig<TestClient, WitnessJobDispatcher>,
    ) {
        let subscriber = WitnessSubscriber::default();
        let op_witness = subscriber.operations.clone();
        let (mut runtime, handle) = started_runtime(config);
        runtime.subscribers.push(Box::new(subscriber));

        let sender = handle.create_sender();
        sender
            .send_message(BasicAction("before_cancel"))
            .expect("Send should not fail in tests");
        handle.cancel().expect("Runtime should still be running");
        sender
            .send_message(BasicAction("after_cancel"))
            .expect("Send should not fail in tests");

        let final_state = runtime.run().expect("Run loop should end without a fatal error");

        assert_eq!(final_state, vec![BasicAction("before_cancel")]);

        let notified: Vec<_> = op_witness
            .read()
            .unwrap()
            .iter()
            .cloned()
            .filter_map(|op| {
                if let Notify(state) = op {
                    Some(state)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(notified, vec![vec![BasicAction("before_cancel")]]);
    }

    #[rstest]
    fn dropped_handle_should_end_run_loop(config: RuntimeConfig<TestClient, WitnessJobDispatcher>) {
        let (runtime, handle) = started_runtime(config);

        drop(handle);

        let final_state = runtime.run().expect("Run loop should end without a fatal error");

        assert!(final_state.is_empty());
    }
}
