use super::{Expense, ModelState, Statistics, StatisticsResults};
use crate::action::{Action, WorkingAction};
use std::{thread, time::Duration};
use jiff::{Timestamp, Zoned};
use crate::action::ModelAction::StatisticsAllResult;
use crate::util::VersionedArc;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Direct(Vec<WorkingAction>),
    Queue(Vec<WorkingAction>),
    Async(AsyncCmd),
    Persistence(PersistenceCmd),
    Subscribe(Subscription),
}

impl From<AsyncCmd> for Cmd {
    fn from(cmd: AsyncCmd) -> Self {
        Cmd::Async(cmd)
    }
}

impl From<PersistenceCmd> for Cmd {
    fn from(cmd: PersistenceCmd) -> Self {
        Cmd::Persistence(cmd)
    }
}

impl From<Subscription> for Cmd {
    fn from(subscription: Subscription) -> Self {
        Cmd::Subscribe(subscription)
    }
}

impl From<TimeSubscriptionCmd> for Cmd {
    fn from(cmd: TimeSubscriptionCmd) -> Self {
        Cmd::Subscribe(Subscription::Time(cmd))
    }
}

impl From<DebounceCmd> for Cmd {
    fn from(cmd: DebounceCmd) -> Self {
        Cmd::Subscribe(Subscription::Debounce(cmd))
    }
}

#[derive(Debug, PartialEq)]
pub enum AsyncCmd {
    StatisticsCalculation(VersionedArc<Vec<Expense>>, Timestamp),
}

impl AsyncCmd {
    pub fn into_job(self) -> impl FnOnce() -> Action + Send + 'static {
        match self {
            AsyncCmd::StatisticsCalculation(expenses, request_time) => Box::new(move || {
                // Supposedly long data extraction...
                let version = expenses.version();
                let len = expenses.len();
                let amounts: Vec<_> = expenses
                    .iter()
                    .map(|e| e.amount)
                    .collect();

                drop(expenses);

                // Supposedly even longer calculation...
                thread::sleep(Duration::from_secs(2));
                let results = if len > 0 {
                    let acc = StatisticsResults {
                        sum: 0,
                        max_expense: i64::MIN,
                        min_expense: i64::MAX,
                    };
                    Some(
                        amounts.into_iter().fold(acc, |mut st, a| {
                            st.sum += a;
                            if a > st.max_expense {
                                st.max_expense = a;
                            };
                            if a < st.min_expense {
                                st.min_expense = a
                            };
                            st
                        })
                    )
                } else {
                    None
                };

                StatisticsAllResult(
                    Statistics {
                        at_movements_version: version,
                        requested_at: request_time,
                        items_len: len,
                        results,
                    }
                ).into()
            }),
        }
    }
}



#[derive(Debug, PartialEq)]
pub enum Subscription {
    Time(TimeSubscriptionCmd),
    Debounce(DebounceCmd),
}

#[derive(Debug, PartialEq)]
pub enum TimeSubscriptionCmd {
    Watchdog,
}

#[derive(Debug, PartialEq)]
pub enum DebounceCmd {
    DelayedSave(DebounceAction),
}

#[derive(Debug, PartialEq)]
pub enum DebounceAction {
    Bump,
    Cancel,
}

#[derive(Debug, PartialEq)]
pub enum PersistenceCmd {
    CreateOrOpenFile,
    Save(ModelState),
}