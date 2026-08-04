use crate::action::ModelAction::StatisticsAllResult;
use crate::action::Action;
use crate::runtime::MoniCommand;
use crate::runtime::cmd::DebounceCmd::DelayedSave;
use crate::runtime::cmd::Subscription::{Debounce, Time};
use crate::runtime::services::{Service, Services};
use crate::runtime::{Expense, ModelState, MoniProducts, Statistics, StatisticsResults};
use crate::util::VersionedArc;
use jiff::Timestamp;
use rdxlib::cmd::{AsyncTask, Cmd, EnvironmentCommand};
use std::{thread, time::Duration};

#[derive(Debug)]
pub enum ServiceCommand {
    Persistence(PersistenceCmd),
    Subscribe(Subscription),
}

impl EnvironmentCommand for ServiceCommand {
    type Environment = Services;

    fn process(self, env: &mut Self::Environment) {
        match self {
            ServiceCommand::Persistence(p_cmd) => {
                env.persistence.execute(p_cmd);
            }
            ServiceCommand::Subscribe(s_cmd) => {
                match s_cmd {
                    Time(cmd) => {
                        env.timers.submit(cmd.into());
                    }
                    Debounce(cmd) => {
                        let DelayedSave(ref action) = cmd;
                        match action {
                            DebounceAction::Bump => env.timers.submit(cmd.into()),
                            DebounceAction::Cancel => env.timers.remove(cmd.into()),
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum AsyncCmd {
    StatisticsCalculation(VersionedArc<Vec<Expense>>, Timestamp),
}

impl AsyncCmd {
    pub fn name(&self) -> &'static str {
        match self {
            AsyncCmd::StatisticsCalculation(_, _) => "StatisticsCalculation"
        }
    }
}

impl AsyncCmd {
    pub fn into_job(self) ->Box<dyn FnOnce() -> Action + Send + 'static> {
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
        Cmd::Async(
            AsyncTask {
                name: cmd.name().to_string(),
                job: cmd.into_job(),
            }
        )
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

pub trait DelayedSaveProduct {
    fn with_delayed_save(self) -> Self;
}

impl DelayedSaveProduct for MoniProducts {
    fn with_delayed_save(self) -> Self {
        self.with_cmd(DelayedSave(DebounceAction::Bump))
    }
}