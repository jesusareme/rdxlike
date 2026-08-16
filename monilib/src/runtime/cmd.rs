use crate::action::ModelAction::StatisticsAllResult;
use crate::action::{Action, LibAction, RunningAction, WorkingAction};
use crate::runtime::cmd::AsyncCmd::StatisticsCalculation;
use crate::runtime::cmd::DebounceCmd::DelayedSave;
use crate::runtime::cmd::Subscription::{Debounce, Time};
use crate::runtime::services::{Service, Services};
use crate::runtime::{Expense, ModelState, MoniProducts, Statistics, StatisticsResults};
use crate::runtime::{MoniCommand, MoniMessage};
use crate::util::{CancellationCheck, VersionedArc};
use jiff::Timestamp;
use rdxlib::cmd::{AsyncTask, Cmd, EnvironmentCommand};
use rdxlib::util::MessageSender;
use std::collections::VecDeque;
use std::panic::UnwindSafe;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub(crate) enum ServiceCommand {
    Persistence(PersistenceCmd),
    Subscribe(Subscription),
}

impl EnvironmentCommand for ServiceCommand {
    type Environment = Services;
    type Action = Action;
    type RuntimeAction = LibAction;

    fn process(
        self,
        env: &mut Self::Environment,
        pending: &mut VecDeque<MoniMessage>,
        _messages_tx: &MessageSender<MoniMessage>,
    ) {
        let result = match self {
            ServiceCommand::Persistence(p_cmd) => env.persistence.execute(p_cmd),
            ServiceCommand::Subscribe(s_cmd) => match s_cmd {
                Time(cmd) => env.timers.send(cmd.into()),
                Debounce(cmd) => env.timers.send(cmd.into()),
            },
        };

        if let Err(error) = result {
            pending.push_back(RunningAction::Error(error).into());
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum AsyncCmd {
    StatisticsCalculation(VersionedArc<Vec<Expense>>, Timestamp, CancellationCheck),
}

impl AsyncCmd {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            StatisticsCalculation(_, _, _) => "StatisticsCalculation",
        }
    }
}

impl AsyncCmd {
    pub(crate) fn into_job(self) -> Box<dyn FnOnce() -> Action + Send + UnwindSafe + 'static> {
        match self {
            StatisticsCalculation(expenses, request_time, cancellation_check) => {
                fn calculate_statistics(
                    expenses: VersionedArc<Vec<Expense>>,
                    request_time: Timestamp,
                    cancellation_check: &CancellationCheck,
                ) -> Option<Statistics> {
                    let version = expenses.version();
                    let len = expenses.len();
                    let amounts: Vec<_> = expenses.iter().map(|e| e.amount).collect();
                    drop(expenses);

                    cancellation_check.still_working()?;

                    // Let's suppose this is a long calculation worth moving to a different thread...
                    let results = (len > 0).then(|| {
                        amounts.into_iter().fold(
                            StatisticsResults {
                                sum: 0,
                                max_expense: i64::MIN,
                                min_expense: i64::MAX,
                            },
                            |mut st, a| {
                                st.sum += a;
                                st.max_expense = st.max_expense.max(a);
                                st.min_expense = st.min_expense.min(a);
                                st
                            },
                        )
                    });

                    cancellation_check.still_working()?;

                    Some(Statistics {
                        at_movements_version: version,
                        requested_at: request_time,
                        items_len: len,
                        results,
                    })
                }

                Box::new(move || {
                    StatisticsAllResult(calculate_statistics(
                        expenses,
                        request_time,
                        &cancellation_check,
                    ))
                    .into()
                })
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Subscription {
    Time(TimeSubscriptionCmd),
    Debounce(DebounceCmd),
}

#[derive(Debug, PartialEq)]
pub(crate) enum TimeSubscriptionCmd {
    EveryXInterval(Uuid, Duration, WorkingAction),
    CancelEveryXInterval(Uuid),
}

#[derive(Debug, PartialEq)]
pub(crate) enum DebounceCmd {
    DelayedSave(DebounceAction),
}

#[derive(Debug, PartialEq)]
pub(crate) enum DebounceAction {
    Bump,
    Cancel,
}

#[derive(Debug, PartialEq)]
pub(crate) enum PersistenceCmd {
    CreateOrOpenFile,
    Save(ModelState),
}

impl From<PersistenceCmd> for ServiceCommand {
    fn from(cmd: PersistenceCmd) -> Self {
        ServiceCommand::Persistence(cmd)
    }
}

impl From<Subscription> for ServiceCommand {
    fn from(subscription: Subscription) -> Self {
        ServiceCommand::Subscribe(subscription)
    }
}

impl From<TimeSubscriptionCmd> for Subscription {
    fn from(cmd: TimeSubscriptionCmd) -> Self {
        Time(cmd)
    }
}

impl From<DebounceCmd> for Subscription {
    fn from(cmd: DebounceCmd) -> Self {
        Debounce(cmd)
    }
}

impl From<AsyncCmd> for MoniCommand {
    fn from(cmd: AsyncCmd) -> Self {
        Cmd::Async(AsyncTask {
            name: cmd.name().to_string(),
            job: cmd.into_job(),
        })
    }
}

impl From<PersistenceCmd> for MoniCommand {
    fn from(cmd: PersistenceCmd) -> Self {
        Cmd::Env(cmd.into())
    }
}

impl From<Subscription> for MoniCommand {
    fn from(subscription: Subscription) -> Self {
        Cmd::Env(subscription.into())
    }
}

impl From<TimeSubscriptionCmd> for MoniCommand {
    fn from(cmd: TimeSubscriptionCmd) -> Self {
        Subscription::from(cmd).into()
    }
}

impl From<DebounceCmd> for MoniCommand {
    fn from(cmd: DebounceCmd) -> Self {
        Subscription::from(cmd).into()
    }
}

pub(crate) trait DelayedSaveProduct {
    fn with_delayed_save(self) -> Self;
}

impl DelayedSaveProduct for MoniProducts {
    fn with_delayed_save(self) -> Self {
        self.with_cmd(DelayedSave(DebounceAction::Bump))
    }
}
