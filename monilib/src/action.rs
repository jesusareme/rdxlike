use crate::inout::{ExpenseAddIntent, MoniStatistics};
use crate::runtime::{MoniMessage, Statistics};
use crate::util::ExpenseId;
use crate::{
    MoniError, MoniExpensePlainListSnapshot,
    inout::LibOutput,
    runtime::{Expense, PersistenceError},
};
use rdxlib::messages::Message;
use rdxlib::subscribers::ViewId;
use std::time::Duration;
use uuid::Uuid;

pub(crate) enum LibAction {
    Subscription(LibSubscription),
}

pub(crate) enum LibSubscription {
    PlainListView(ViewId, LibOutput<MoniExpensePlainListSnapshot>),
    Errors(LibOutput<Vec<MoniError>>),
    StatisticsSub(LibOutput<MoniStatistics>),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action {
    Init,
    InitResult(Result<Option<String>, PersistenceError>),
    Working(WorkingAction),
    Running(RunningAction),
}

impl From<Action> for Message<Action, LibAction> {
    fn from(action: Action) -> Self {
        Message::Action(action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkingAction {
    Model(ModelAction),

    Save,
    SuccessfulSave,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RunningAction {
    Error(MoniError),
    ListViewHint(ViewId, ExpenseId),
    ListViewPrepare(ViewId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ModelAction {
    Add(ExpenseAddIntent),
    Update(Expense),
    Delete(ExpenseId),
    StatisticsAll,
    StatisticsAllResult(Option<Statistics>),
    CancelStatistics,

    // Action to set a recurrent timer as example of cancellable state
    AddEveryXInterval(Uuid, Duration, Box<WorkingAction>),
    StopAddingEveryXInterval(Uuid),
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

impl From<RunningAction> for Action {
    fn from(a: RunningAction) -> Self {
        Action::Running(a)
    }
}

impl From<LibAction> for MoniMessage {
    fn from(value: LibAction) -> Self {
        Message::Runtime(value)
    }
}

impl From<LibSubscription> for LibAction {
    fn from(s: LibSubscription) -> Self {
        LibAction::Subscription(s)
    }
}

impl From<LibSubscription> for MoniMessage {
    fn from(s: LibSubscription) -> Self {
        LibAction::from(s).into()
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
