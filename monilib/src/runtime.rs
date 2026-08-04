mod middlewares;
mod model_views;
mod reducers;
mod subscribers;
mod cmd;
mod services;

use crate::util::{ClockSource, ExpenseId};
use crate::{MoniDomainError, MoniError, action::{Action::Init, *}, util::VersionedArc};
use LibAction::{ErrorsSubscription, PlainListViewSubscription};
use enumset::{EnumSet, EnumSetType};
use jiff::{Timestamp, Zoned};
use model_views::ClockedModelStateView;
use rdxlib::cmd::Cmd;
use serde::{Deserialize, Serialize};
use services::Services;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::Receiver;

use tracing::debug;
pub use services::PersistenceError;
use crate::action::LibAction::StatisticsSubscription;
use crate::inout::PlainListItem;
use crate::runtime::subscribers::statistics_subscriber;

use crate::runtime::cmd::ServiceCommand;
use crate::runtime::middlewares::MoniMiddleware;
use crate::runtime::reducers::reducer;
use rdxlib::messages::Message;
use rdxlib::products::{ActionProducts, RuntimeProducts};
use rdxlib::subscribers::ViewId;
use rdxlib::threadpool::ThreadPool;
use rdxlib::util::{MessageSend, MessageSender};
use rdxlib::{Client, Runtime, RuntimeConfig};

#[cfg(test)]
use crate::testing::ref_id;
#[cfg(test)]
use std::cmp::Ordering;
use boltffi::data;

pub(crate) struct MoniLibClient;
impl Client for MoniLibClient {
    type State = State;
    type Action = Action;
    type RuntimeAction = LibAction;
    type Flag = Dirty;
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
    Views
}

pub struct RuntimeEnvironment {
    pub messages_rx: Receiver<MoniMessage>,
    pub actions_tx: MessageSender<MoniMessage>,
    pub logging_enabled: bool,
    pub path: String,
    pub clock: Arc<dyn ClockSource + Send + Sync>,
}

    pub fn new(config: RuntimeEnvironment) -> Runtime<MoniLibClient> {
        let environment = Services::new(&config.actions_tx, config.path, &config.clock);

        let mut funs= vec![];
        if config.logging_enabled {
            funs.push(MoniMiddleware::Logger);
        }
        funs.push(MoniMiddleware::Clock(config.clock));
        funs.push(MoniMiddleware::Cleaner);

        let state = State::Zero(vec![]);

        config
            .actions_tx
            .send_message(Init)
            .expect("Unable to prepare init of MoniLib");

        let runtime_cfg = RuntimeConfig {
            services: environment,
            state,
            middlewares: vec![],
            reducer,
            runtime_reducer,
            jobs_dispatcher: ThreadPool::new(8),
            messages_rx: config.messages_rx,
            messages_tx: config.actions_tx,
        };

        debug!("MoniLib ready to run...");

        Runtime::new(runtime_cfg)
    }


fn runtime_reducer(
    lib_message: LibAction,
) -> RuntimeProducts<MoniLibClient> {
    match lib_message {
        PlainListViewSubscription(token, out) => {
            let new_subscription = subscribers::plain_list_view_subscriber(token, out);
            RuntimeProducts {
                subscriber: Some(Box::new(new_subscription)),
                actions: vec![RunningAction::ListViewPrepare(token).into()]
            }
        }
        ErrorsSubscription(out) => {
            let new_subscription = subscribers::errors_subscriber(out);
            RuntimeProducts::subscriber(new_subscription)
        }
        StatisticsSubscription(out) => {
            let new_subscription = statistics_subscriber(out);
            RuntimeProducts::subscriber(new_subscription)
        }
    }
}

#[derive(Debug)]
pub(crate) enum State {
    Zero(Vec<WorkingAction>),
    Working(WorkingState),
}

#[derive(Debug, Default)]
pub(crate) struct WorkingState {
    model: ModelState,
    running: RunningState,
}

impl WorkingState {
    fn model_view(&mut self) -> ClockedModelStateView<'_> {
        ClockedModelStateView {
            model_state: &mut self.model,
            time: &self.running.time,
            errors: &mut self.running.errors,
        }
    }
}

#[data]
#[derive(PartialEq, Debug, Serialize, Deserialize, Copy, Clone)]
pub enum ExpenseCategory {
    Essential,
    Important,
    Optional,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
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
impl Eq for Expense {}

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

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Statistics {
    pub at_movements_version: u64,
    pub requested_at: Timestamp,
    pub items_len: usize,
    pub results: Option<StatisticsResults>
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone, Copy)]
pub struct StatisticsResults {
    pub sum: i64,
    pub max_expense: i64,
    pub min_expense: i64,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct ModelState {
    model_version: u16,
    counter: u32,
    info: String,
    movements: VersionedArc<Vec<Expense>>,
    statistics_all: Option<Statistics>,
}

impl Default for ModelState {
    fn default() -> Self {
        ModelState {
            model_version: MODEL_VERSION,
            counter: 0,
            info: String::new(),
            movements: VersionedArc::from(vec![]),
            statistics_all: None,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct PlainListViewState {
    hint: Option<ExpenseId>,
}

#[derive(Debug, Default)]
struct RunningState {
    // counting_cancellation: Option<DropCancellation>,
    time: Zoned,
    errors: Vec<MoniError>,
    plain_list: HashMap<ViewId, PlainListViewState>
}