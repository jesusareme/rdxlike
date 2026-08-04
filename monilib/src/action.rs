use rdxlib::messages::Message;
use rdxlib::subscribers::ViewId;
use crate::{inout::{LibOutput}, runtime::{Expense, PersistenceError}, MoniError, MoniExpensePlainListSnapshot};
use crate::inout::{MoniStatistics};
use crate::runtime::{MoniMessage, Statistics};
use crate::util::ExpenseId;

pub enum LibAction {
    PlainListViewSubscription(ViewId, LibOutput<MoniExpensePlainListSnapshot>),
    ErrorsSubscription(LibOutput<Vec<MoniError>>),
    StatisticsSubscription(LibOutput<MoniStatistics>),
}

#[derive(Debug, PartialEq)]
pub enum Action {
    NoOp,
    Init,
    InitResult(Result<Option<String>, PersistenceError>),
    Working(WorkingAction),
}

impl From<Action> for Message<Action, LibAction> {
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
    ListViewHint(ViewId, ExpenseId),
    ListViewPrepare(ViewId),
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

impl From<WorkingAction> for Message<Action, LibAction> {
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

impl From<LibAction> for MoniMessage {
    fn from(value: LibAction) -> Self {
        Message::Runtime(value)
    }
}

impl From<RunningAction> for Action {
    fn from(a: RunningAction) -> Self {
        WorkingAction::from(a).into()
    }
}

impl From<RunningAction> for MoniMessage {
    fn from(a: RunningAction) -> Self {
        Action::from(a).into()
    }
}

impl From<ModelAction> for Action {
    fn from(a: ModelAction) -> Self {
        WorkingAction::from(a).into()
    }
}

impl From<ModelAction> for MoniMessage {
    fn from(a: ModelAction) -> Self {
        Action::from(a).into()
    }
}
