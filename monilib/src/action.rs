use tracing::Subscriber;
use crate::{inout::{LibOutput}, runtime::{Expense, PersistenceError}, MoniError, MoniExpensePlainListSnapshot};
use crate::inout::{MoniStatistics, ViewToken};
use crate::runtime::Statistics;
use crate::util::ExpenseId;

#[derive(Debug)]
pub enum Message {
    Action(Action),
    Lib(LibAction),
}

#[derive(Debug)]
pub enum LibAction {
    PlainListViewSubscription(ViewToken, LibOutput<MoniExpensePlainListSnapshot>),
    ErrorsSubscription(LibOutput<Vec<MoniError>>),
    StatisticsSubscription(LibOutput<MoniStatistics>),
}

#[derive(Debug)]
pub enum Action {
    NoOp,
    Init,
    InitResult(Result<Option<String>, PersistenceError>),
    Working(WorkingAction),
}

impl From<Action> for Message {
    fn from(action: Action) -> Self {
        Message::Action(action)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkingAction {
    Model(ModelAction),
    Running(RunningAction),
    Save,

    Watchdog,
    WatchdogWatching,

    SuccessfulSave,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunningAction {
    ListViewHint(ViewToken, ExpenseId),
    ListViewPrepare(ViewToken),
}


#[derive(Debug, Clone, PartialEq)]
pub enum ModelAction {
    Add(Expense),
    Update(Expense),
    Delete(ExpenseId),
    StatisticsAll,
    
    StatisticsAllResult(Statistics)
}

impl From<WorkingAction> for Action {
    fn from(a: WorkingAction) -> Self {
        Action::Working(a)
    }
}

impl From<WorkingAction> for Message {
    fn from(a: WorkingAction) -> Self {
        Action::from(a).into()
    }
}

impl From<ModelAction> for WorkingAction {
    fn from(a: ModelAction) -> Self {
        WorkingAction::Model(a)
    }
}

impl From<RunningAction> for WorkingAction {
    fn from(a: RunningAction) -> Self {
        WorkingAction::Running(a)
    }
}

impl From<LibAction> for Message {
    fn from(value: LibAction) -> Self {
        Message::Lib(value)
    }
}

impl From<RunningAction> for Action {
    fn from(a: RunningAction) -> Self {
        WorkingAction::from(a).into()
    }
}

impl From<RunningAction> for Message {
    fn from(a: RunningAction) -> Self {
        Action::from(a).into()
    }
}

impl From<ModelAction> for Action {
    fn from(a: ModelAction) -> Self {
        WorkingAction::from(a).into()
    }
}

impl From<ModelAction> for Message {
    fn from(a: ModelAction) -> Self {
        Action::from(a).into()
    }
}
