mod cmd;
mod middlewares;
mod modelviews;
mod reducers;
mod services;
mod subscribers;
mod threadpool;

use crate::{action::{Action::Init, *}, runtime::cmd::{DebounceAction, DebounceCmd, Subscription::Debounce}, util::{MessageSender, MessageSend, VersionedArc}, MoniError, MoniDomainError};
use Cmd::*;
use DebounceAction::{Bump, Cancel};
use DebounceCmd::DelayedSave;
use LibAction::{ErrorsSubscription, PlainListViewSubscription};
use boltffi::data;
use cmd::{Cmd, Subscription::Time};
use enumset::{EnumSet, EnumSetType};
use jiff::{Timestamp, Zoned};
use middlewares::{Middleware, MiddlewareConfig};
use modelviews::ClockedModelStateView;
use serde::{Deserialize, Serialize};
use services::{Service, Services};
use std::sync::Arc;
use std::{
    collections::VecDeque,
    sync::mpsc::Receiver,
};
#[cfg(test)]
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::{Add, AddAssign};
use subscribers::Subscriber;
use tracing::{debug, error, info};
use crate::util::{ClockSource, ExpenseId};
pub use services::PersistenceError;
use crate::action::LibAction::StatisticsSubscription;
use crate::action::Message;
use crate::inout::{PlainListItem, ViewToken};
use crate::runtime::subscribers::statistics_subscriber;
#[cfg(test)]
use crate::testing::ref_id;

const MODEL_VERSION: u16 = 1;

struct Products {
    cmds: Vec<Cmd>,
    dirty: EnumSet<Dirty>,
}

impl Add<Products> for Products {
    type Output = Products;

    #[allow(clippy::suspicious_op_assign_impl)]
    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add(mut self, rhs: Products) -> Self::Output {
        self.cmds.extend(rhs.cmds);
        self.dirty |= rhs.dirty;
        self
    }
}

impl AddAssign<Products> for Products {
    #[allow(clippy::suspicious_op_assign_impl)]
    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add_assign(&mut self, rhs: Products) {
        self.cmds.extend(rhs.cmds);
        self.dirty |= rhs.dirty;
    }
}

impl Products {
    fn none() -> Self {
        Products {
            cmds: vec![],
            dirty: EnumSet::empty(),
        }
    }

    fn cmd(cmd: impl Into<Cmd>) -> Self {
        Products {
            cmds: vec![cmd.into()],
            dirty: EnumSet::empty(),
        }
    }

    fn cmds(cmds: Vec<Cmd>) -> Self {
        Products {
            cmds,
            dirty: EnumSet::empty(),
        }
    }

    fn with_delayed_save(mut self) -> Self {
        self.cmds.push(DelayedSave(Bump).into());
        self
    }

    fn with_dirty(mut self, flags: EnumSet<Dirty>) -> Self {
        self.dirty |= flags;
        self
    }

    fn with_dirty_flag(mut self, flag: Dirty) -> Self {
        self.dirty |= flag;
        self
    }
}

#[derive(EnumSetType, Debug)]
enum Dirty {
    FinancesCurrentMonth,
    FinancesBeforeThisMonth,
    Categories,
    Statistics,
    Views
}

pub struct RuntimeConfig {
    pub messages_rx: Receiver<Message>,
    pub actions_tx: MessageSender,
    pub logging_enabled: bool,
    pub path: String,
    pub clock: Arc<dyn ClockSource + Send + Sync>,
}

pub struct Runtime {
    environment: Services,
    middleware: Middleware,
    state: State,
    messages_rx: Receiver<Message>,
    actions_tx: MessageSender,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Self {
        let environment = Services::new(&config.actions_tx, config.path, &config.clock);
        let m_config = MiddlewareConfig {
            logging_middleware: config.logging_enabled,
            clock_source: config.clock,
        };
        let middleware = Middleware::new(m_config, reducers::reducer);

        let state = State::Zero(vec![]);

        config
            .actions_tx
            .send_message(Init)
            .expect("Unable to prepare init of MoniLib");

        debug!("MoniLib ready to run...");

        Runtime {
            environment,
            middleware,
            state,
            messages_rx: config.messages_rx,
            actions_tx: config.actions_tx,
        }
    }

    pub fn run(self) {
        let Runtime {
            environment,
            mut middleware,
            mut state,
            messages_rx: message_rx,
            actions_tx,
        } = self;

        info!("Started running MoniLib...");

        let mut subscribers: Vec<Box<dyn Subscriber>> = vec![];

        let async_threads_pool = threadpool::ThreadPool::new(8);

        for message in message_rx.iter() {
            let mut actions: VecDeque<Message> = VecDeque::new();
            actions.push_back(message);
            let mut dirty = EnumSet::empty();

            while let Some(message) = actions.pop_front() {
                match message {
                    Message::Action(action) => {
                        let effects = middleware.run(&mut state, action);
                        dirty |= effects.dirty;

                        for cmd in effects.cmds {
                            process_command(
                                cmd,
                                &environment,
                                &async_threads_pool,
                                &actions_tx,
                                &mut actions,
                            );
                        }
                    }
                    Message::Lib(lib_message) => {
                        if let Some(result) = process_lib_messages(lib_message, &mut subscribers) {
                            actions.push_back(result.into());
                        }
                    }
                }
            }

            subscribers.retain(|s| s.is_active());
            subscribers
                .iter_mut()
                .filter(|s| s.interested_in(&dirty))
                .filter_map(|s| s.notify(&state).err() )
                .for_each(|e| error!("Subscriber error: {e}"));

        }
    }
}

fn process_command(
    cmd: Cmd,
    environment: &Services,
    threads_pool: &threadpool::ThreadPool,
    actions_tx: &impl MessageSend,
    actions: &mut VecDeque<Message>,
) {
    match cmd {
        Direct(new_work_actions) => {
            actions.extend(new_work_actions.into_iter().map(Into::into));
        }
        Queue(new_work_actions) => {
            new_work_actions.into_iter().for_each(|a| {
                _ = actions_tx.send_message(a).inspect_err(|e| {
                    error!("Error while sending new actions from Queue command: {e:?}");
                });
            });
        }
        Async(cmd) => {
            threads_pool.submit(cmd.into_job(), actions_tx);
        }
        Persistence(basic_service_cmd) => {
            environment.persistence.execute(basic_service_cmd);
        }
        Subscribe(subs) => match subs {
            Time(cmd) => {
                environment.timers.submit(cmd.into());
            }
            Debounce(cmd) => {
                let DelayedSave(ref action) = cmd;
                match action {
                    Bump => environment.timers.submit(cmd.into()),
                    Cancel => environment.timers.remove(cmd.into()),
                }
            }
        },
    }
}

fn process_lib_messages(
    lib_message: LibAction,
    subscribers: &mut Vec<Box<dyn Subscriber>>,
) -> Option<impl Into<Message>> {
    match lib_message {
        PlainListViewSubscription(token, out) => {
            let new_subscription = subscribers::plain_list_view_subscriber(token, out);
            subscribers.push(Box::new(new_subscription));
            Some(RunningAction::ListViewPrepare(token))
        }
        ErrorsSubscription(out) => {
            let new_subscription = subscribers::errors_subscriber(out);
            subscribers.push(Box::new(new_subscription));
            None
        }
        StatisticsSubscription(out) => {
            let new_subscription = statistics_subscriber(out);
            subscribers.push(Box::new(new_subscription));
            None
        }
    }
}

#[derive(Debug)]
enum State {
    Zero(Vec<WorkingAction>),
    Working(WorkingState),
}

#[derive(Debug, Default)]
struct WorkingState {
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
    plain_list: HashMap<ViewToken, PlainListViewState>
}

impl threadpool::ThreadPool {
    pub fn submit(
        &self,
        action_job: impl FnOnce() -> Action + Send + 'static,
        action_sender: &impl MessageSend,
    ) {
        let async_actions_tx = action_sender.clone();
        self.work_on(move || {
            let action = action_job();
            async_actions_tx.send_message(action).unwrap();
        });
    }
}
