mod cmd;
mod middlewares;
mod model_views;
mod reducers;
mod services;
mod subscribers;

use crate::util::{ClockSource, ExpenseId};
use crate::{
    MoniDomainError, MoniError,
    action::{Action, LibAction, ModelAction, RunningAction, WorkingAction},
    util::VersionedArc,
};
use enumset::{EnumSet, EnumSetType};
use jiff::{Timestamp, Zoned};
use rdxlib::cmd::Cmd;
use serde::{Deserialize, Serialize};
use services::Services;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};

use crate::inout::PlainListItem;
use crate::runtime::subscribers::statistics_subscriber;
use rdxlib::error::RuntimeError;
pub use services::PersistenceError;
use tracing::{debug, error, info};

use crate::runtime::cmd::ServiceCommand;
use crate::runtime::middlewares::MoniMiddleware;
use crate::runtime::reducers::reducer;
use rdxlib::messages::Message;
use rdxlib::primitives::ThreadPool;
use rdxlib::products::{ActionProducts, RuntimeProducts};
use rdxlib::subscribers::ViewId;
use rdxlib::util::{DropCancellation, MessageSend, TaskId};
use rdxlib::{Client, RuntimeConfig, RuntimeHandle, RuntimeRunner};

use crate::action::Action::Init;
use crate::action::LibSubscription;
use crate::action::LibSubscription::{Errors, PlainListView, StatisticsSub};
#[cfg(test)]
use crate::testing::ref_id;
use boltffi::data;
use rdxlib::middleware::ChainableMiddleware;
#[cfg(test)]
use std::cmp::Ordering;

pub(crate) enum MoniLibClient {}
impl Client for MoniLibClient {
    type State = State;
    type Action = Action;
    type RuntimeAction = LibAction;
    type Flag = Dirty;
    type Environment = Services;
    type ServiceCommand = ServiceCommand;
}

pub type MoniMessage = Message<Action, LibAction>;
pub type MoniProducts = ActionProducts<MoniLibClient>;
pub type MoniCommand = Cmd<MoniLibClient>;

const MODEL_VERSION: u16 = 1;

#[derive(EnumSetType, Debug)]
pub enum Dirty {
    FinancesCurrentMonth,
    FinancesBeforeThisMonth,
    Categories,
    Statistics,
    Views,
}

pub struct RuntimeEnvironment {
    pub logging_enabled_pre_action: bool,
    pub logging_enabled_post_action: bool,
    pub path: String,
    pub clock: Arc<dyn ClockSource + Send + Sync>,
}

pub struct RuntimeInit {
    pub handle: RuntimeHandle<MoniLibClient>,
    pub thread: JoinHandle<()>,
}

pub fn start(config: RuntimeEnvironment) -> Result<RuntimeInit, MoniError> {
    let (handle, runner) = RuntimeRunner::create();
    let sender = handle.create_sender();

    let services = Services::new(&sender, config.path, &config.clock)?;
    let jobs = ThreadPool::new(8)?;
    sender.send_message(Init)?;

    let middlewares = vec![
        MoniMiddleware::Logger {
            prev: config.logging_enabled_pre_action,
            post: config.logging_enabled_post_action,
        },
        MoniMiddleware::Clock(config.clock),
    ];

    debug!("MoniLib ready to run...");

    let thread = Builder::new()
        .name("messages".to_string())
        .spawn(move || {
            let runtime_cfg = RuntimeConfig {
                services,
                state: State::default(),
                middlewares: middlewares.into_iter().map(MoniMiddleware::boxed).collect(),
                reducer,
                runtime_reducer,
                jobs_dispatcher: jobs,
            };
            match runner.run(runtime_cfg) {
                Ok(_) => info!("MoniLib run loop ended"),
                Err(e) => error!("MoniLib run loop ended with fatal error {e:?}"),
            }
            info!("MoniLib thread ended");
        })?;

    Ok(RuntimeInit { handle, thread })
}

fn runtime_reducer(lib_message: LibAction) -> RuntimeProducts<MoniLibClient> {
    match lib_message {
        LibAction::Subscription(subscription) => subscription_reducer(subscription),
    }
}

fn subscription_reducer(subscription: LibSubscription) -> RuntimeProducts<MoniLibClient> {
    match subscription {
        PlainListView(token, out) => match subscribers::plain_list_view_subscriber(token, out) {
            Ok(new_subscription) => RuntimeProducts {
                subscriber: Some(Box::new(new_subscription)),
                actions: vec![RunningAction::ListViewPrepare(token).into()],
            },
            Err(cause) => unstarted_subscriber("plain list view", cause),
        },
        Errors(out) => match subscribers::errors_subscriber(out) {
            Ok(new_subscription) => RuntimeProducts::subscriber(new_subscription),
            Err(cause) => unstarted_subscriber("errors", cause),
        },
        StatisticsSub(out) => match statistics_subscriber(out) {
            Ok(new_subscription) => RuntimeProducts::subscriber(new_subscription),
            Err(cause) => unstarted_subscriber("statistics", cause),
        },
    }
}

fn unstarted_subscriber(name: &str, cause: RuntimeError) -> RuntimeProducts<MoniLibClient> {
    let message =
        format!("Unable to start the {name} subscriber, subscription is dropped: {cause}");
    error!(message);
    RuntimeProducts {
        subscriber: None,
        actions: vec![RunningAction::Error(cause.into()).into()],
    }
}

#[derive(Debug)]
pub(crate) enum AppState {
    Zero(Vec<WorkingAction>),
    Failed,
    Working(ModelState),
}

impl Default for AppState {
    fn default() -> Self {
        AppState::Zero(vec![])
    }
}

#[derive(Debug, Default)]
pub(crate) struct State {
    app: AppState,
    running: RunningState,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct ModelState {
    model_version: u16,
    movements: VersionedArc<Vec<Expense>>,
    ids: Ids,
    #[serde(skip)]
    statistics_all: Option<Statistics>,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone, Default)]
pub struct Ids {
    next_expense_id: ExpenseId,
}

impl Default for ModelState {
    fn default() -> Self {
        ModelState {
            model_version: MODEL_VERSION,
            movements: VersionedArc::from(vec![]),
            statistics_all: None,
            ids: Ids::default(),
        }
    }
}

#[data]
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Copy, Clone)]
pub enum ExpenseCategory {
    Essential,
    Important,
    Optional,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct Expense {
    id: ExpenseId,
    date: Zoned,
    amount: i64,
    comment: Option<String>,
    category: ExpenseCategory,
}

impl Expense {
    pub(crate) fn new(
        id: ExpenseId,
        date: Zoned,
        amount: i64,
        comment: Option<String>,
        category: ExpenseCategory,
    ) -> Self {
        Expense {
            id,
            date,
            amount,
            comment,
            category,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_default_with(id: ExpenseId, date: Zoned, amount: Option<i64>) -> Self {
        Expense {
            id,
            date,
            amount: amount.unwrap_or(1230),
            ..Expense::default()
        }
    }
}

impl From<Expense> for PlainListItem {
    fn from(val: Expense) -> Self {
        PlainListItem {
            id: val.id.into(),
            date: val.date.into(),
            amount: val.amount,
            comment: val.comment,
            category: val.category,
        }
    }
}

#[cfg(test)]
impl PartialOrd<Expense> for Expense {
    fn partial_cmp(&self, other: &Expense) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
impl Ord for Expense {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date.cmp(&other.date)
    }
}

#[cfg(test)]
impl Default for Expense {
    fn default() -> Self {
        Expense {
            id: ExpenseId::from(ref_id()),
            date: Zoned::now(),
            amount: 1230,
            comment: Some("comment".to_string()),
            category: ExpenseCategory::Essential,
        }
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Statistics {
    pub at_movements_version: u64,
    pub requested_at: Timestamp,
    pub items_len: usize,
    pub results: Option<StatisticsResults>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone, Copy)]
pub struct StatisticsResults {
    pub sum: i64,
    pub max_expense: i64,
    pub min_expense: i64,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct PlainListViewState {
    hint: Option<ExpenseId>,
}

#[derive(Debug, Default)]
struct RunningState {
    time: Zoned,
    errors: Vec<MoniError>,
    plain_list: HashMap<ViewId, PlainListViewState>,
    tasks: LongLivingTasks,
}

#[derive(Debug, Default)]
struct LongLivingTasks {
    recurrent_add: HashSet<TaskId>,
    statistics_calc_running: Option<DropCancellation>,
}

impl MoniMiddleware {
    fn boxed(self) -> Box<dyn ChainableMiddleware<MoniLibClient>> {
        Box::new(self)
    }
}
