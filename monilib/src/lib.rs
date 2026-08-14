mod action;
mod inout;
mod persistence;
mod runtime;
mod util;

pub mod error;
#[cfg(test)]
mod testing;

pub use crate::error::{LibErrorCause, MoniDomainError, MoniError, MoniErrorType};
use crate::inout::MoniStatistics;
pub use crate::inout::{
    LibOutput, MoniExpense, MoniExpensePlainListSnapshot, MoniExpenseUpdate, MoniValidationError,
    MoniValidationErrorCause, PlainListItem,
};
pub use crate::runtime::ExpenseCategory;
use crate::runtime::{MoniMessage, RuntimeEnvironment};
use crate::util::ExpenseId;
use action::*;
use boltffi::{EventSubscription, data, export, ffi_stream};
use log::warn;
use rdxlib::messages::Message;
use rdxlib::subscribers::{ViewId, ViewOutput};
use rdxlib::util::{MessageSend, MessageSender};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{
    sync::{Arc, mpsc},
    thread::Builder,
};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use util::{ClockSource, SystemClockSource};
use uuid::Uuid;

#[data]
#[derive(Clone, Debug)]
pub enum MoniLogLevel {
    Debug,
    Info,
}

impl AsRef<str> for MoniLogLevel {
    fn as_ref(&self) -> &str {
        match self {
            MoniLogLevel::Debug => "debug",
            MoniLogLevel::Info => "info",
        }
    }
}

#[data]
#[derive(Clone, Debug)]
pub enum LibClockSource {
    System,
}

#[data]
#[derive(Clone, Debug)]
pub struct LibConfig {
    pub log_level: MoniLogLevel,
    pub clock: LibClockSource,
}

pub struct PlainListViewHandler {
    token: ViewId,
    action_sender: MessageSender<Message<Action, LibAction>>,
}

#[export]
impl PlainListViewHandler {
    pub fn hint(&self, hint: u64) -> Result<(), MoniError> {
        self.action_sender
            .send_message(RunningAction::ListViewHint(
                self.token,
                ExpenseId::from(hint),
            ))?;
        Ok(())
    }

    #[ffi_stream(item = MoniExpensePlainListSnapshot)]
    pub fn subscribe(&self) -> Arc<EventSubscription<MoniExpensePlainListSnapshot>> {
        let out = LibOutput::new(256);

        self.action_sender
            .send_message(LibAction::PlainListViewSubscription(
                self.token,
                out.clone(),
            ))
            .expect("TODO: panic message");

        out.into()
    }
}

pub struct MoniLib {
    action_sender: MessageSender<MoniMessage>,
    clock: Arc<dyn ClockSource + Send + Sync>,
    lib_thread_handle: JoinHandle<()>,
}

#[export]
impl MoniLib {
    pub fn new(path: String, config: LibConfig) -> Result<Self, MoniError> {
        _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(config.log_level))
            .try_init()
            .inspect_err(|e| {
                warn!(
                    "Unable to initialize logging, maybe MoniLib init called more than once?: {e:?}"
                )
            });

        info!("Hi from MoniLib!");

        inout::try_state_path(&path)?;

        let (root_message_tx, message_rx) = mpsc::channel::<MoniMessage>();
        let dispatcher = MessageSender::new(root_message_tx);
        let actions_tx = dispatcher.clone();

        let clock = match config.clock {
            LibClockSource::System => Arc::new(SystemClockSource),
        };
        let shared_clock = clock.clone();

        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), MoniError>>();

        let builder = Builder::new().name("messages".to_string());
        let actions_handler = builder.spawn(move || {
            let config = RuntimeEnvironment {
                messages_rx: message_rx,
                actions_tx,
                logging_enabled_pre_action: true,
                logging_enabled_post_action: true,
                path,
                clock: shared_clock,
            };
            match runtime::new(config) {
                Ok(runtime) => {
                    if ready_tx.send(Ok(())).is_err() {
                        error!("Error while trying to response back after successful runtime init");
                        return;
                    }
                    runtime.run();
                }
                Err(error) => {
                    _ = ready_tx.send(Err(error));
                }
            }
        })?;

        ready_rx
            .recv()
            .map_err(|_| MoniError::from(LibErrorCause::Threading))??;

        Ok(MoniLib {
            action_sender: dispatcher,
            clock,
            lib_thread_handle: actions_handler,
        })
    }

    pub fn create_plain_list_view(&self) -> PlainListViewHandler {
        PlainListViewHandler {
            token: Uuid::now_v7().into(),
            action_sender: self.action_sender.clone(),
        }
    }

    pub fn add_expense(&self, expense: MoniExpense) -> Result<(), MoniError> {
        let intent = expense.into_add_intent(self.clock.as_ref())?;
        self.action_sender.send_message(ModelAction::Add(intent))?;
        Ok(())
    }

    pub fn update_expense(&self, update: MoniExpenseUpdate) -> Result<(), MoniError> {
        let expense = update.into_updatable_expense(self.clock.as_ref())?;
        self.action_sender
            .send_message(ModelAction::Update(expense))?;
        Ok(())
    }

    pub fn delete_expense(&self, delete: u64) -> Result<(), MoniError> {
        self.action_sender
            .send_message(ModelAction::Delete(ExpenseId::from(delete)))?;
        Ok(())
    }

    pub fn calculate_statistics_all(&self) -> Result<(), MoniError> {
        self.action_sender
            .send_message(ModelAction::StatisticsAll)?;
        Ok(())
    }

    pub fn save(&self) -> Result<(), MoniError> {
        self.action_sender.send_message(WorkingAction::Save)?;
        Ok(())
    }

    #[ffi_stream(item = Vec<MoniError>)]
    pub fn errors(&self) -> Arc<EventSubscription<Vec<MoniError>>> {
        let out = LibOutput::new(8);

        self.action_sender
            .send_message(LibAction::ErrorsSubscription(out.clone()))
            .expect("TODO: panic message");

        out.into()
    }

    #[ffi_stream(item = MoniStatistics)]
    pub fn statistics(&self) -> Arc<EventSubscription<MoniStatistics>> {
        let out = LibOutput::new(8);

        self.action_sender
            .send_message(LibAction::StatisticsSubscription(out.clone()))
            .expect("TODO: panic message");

        out.into()
    }

    pub fn schedule_repeat_expense(
        &self,
        expense: MoniExpense,
        interval: Duration,
    ) -> Result<Uuid, MoniError> {
        let uuid = Uuid::new_v4();
        self.action_sender
            .send_message(ModelAction::AddEveryXInterval(
                uuid,
                interval,
                Box::new(WorkingAction::Model(ModelAction::Add(
                    expense.into_add_intent(self.clock.as_ref())?,
                ))),
            ))?;
        Ok(uuid)
    }

    pub fn cancel_repeat_expense(&self, id: Uuid) -> Result<(), MoniError> {
        self.action_sender
            .send_message(ModelAction::StopAddingEveryXInterval(id))?;
        Ok(())
    }

    pub fn has_finished(&self) -> bool {
        self.lib_thread_handle.is_finished()
    }
}
