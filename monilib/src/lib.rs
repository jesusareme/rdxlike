mod action;
mod inout;
mod persistence;
mod runtime;
mod util;

#[cfg(test)]
mod testing;

use crate::inout::MoniStatistics;
use crate::runtime::{MoniMessage, RuntimeEnvironment};
use crate::util::ExpenseId;
use action::*;
use boltffi::{EventSubscription, data, error, export, ffi_stream};
use log::warn;
use rdxlib::messages::Message;
use rdxlib::subscribers::{ViewId, ViewOutput};
use rdxlib::util::{MessageSend, MessageSender};
use std::error::Error;
use std::{
    fmt::{Display, Formatter},
    sync::{
        Arc,
        mpsc,
    },
    thread::Builder,
};
use tracing::info;
use tracing_subscriber::EnvFilter;
use util::{ClockSource, RandomIdSource, SystemClockSource};
use uuid::Uuid;

pub use crate::inout::{
    LibOutput, MoniExpense, MoniExpenseUpdate, MoniExpensePlainListSnapshot, MoniValidationError,
    MoniValidationErrorCause, PlainListItem,
};
pub use crate::runtime::ExpenseCategory;


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

#[error]
#[derive(Debug, Clone)]
pub struct MoniError {
    pub id: Uuid,
    pub error_type: MoniErrorType,
}

#[data]
#[derive(Debug, Clone)]
pub enum MoniErrorType {
    Domain(MoniDomainError),
    Lib(LibErrorCause),
}

#[data]
#[derive(Debug, Clone)]
pub enum MoniDomainError {
    Validation(MoniValidationError),
    ExpenseNotFound(Uuid),
}

impl MoniError {
    pub fn new(error_type: MoniErrorType) -> Self {
        MoniError {
            id: Uuid::new_v4(),
            error_type,
        }
    }
}

impl From<MoniErrorType> for MoniError {
    fn from(error_type: MoniErrorType) -> Self {
        MoniError::new(error_type)
    }
}

impl From<MoniDomainError> for MoniError {
    fn from(error: MoniDomainError) -> Self {
        MoniError::new(MoniErrorType::Domain(error))
    }
}

impl From<LibErrorCause> for MoniError {
    fn from(cause: LibErrorCause) -> Self {
        MoniError::new(MoniErrorType::Lib(cause))
    }
}

impl Display for MoniDomainError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MoniDomainError::Validation(e) => write!(f, "Validation error {e}"),
            MoniDomainError::ExpenseNotFound(id) => write!(f, "ExpenseNotFound: {id}"),
        }
    }
}

impl Error for MoniError {}

#[data]
#[derive(Debug, Clone)]
pub enum LibErrorCause {
    Sender,
    Path,
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

impl From<MoniValidationError> for MoniError {
    fn from(value: MoniValidationError) -> Self {
        MoniError::new(MoniErrorType::Domain(MoniDomainError::Validation(value)))
    }
}

impl Display for MoniErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MoniErrorType::Domain(e) => e.fmt(f),
            MoniErrorType::Lib(LibErrorCause::Sender) => write!(f, "Lib fatal error, unable to connect."),
            MoniErrorType::Lib(LibErrorCause::Path) => write!(f, "Lib fatal error, path is not available"),
        }
    }
}

impl Display for MoniError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.error_type.fmt(f)
    }
}

pub struct PlainListViewHandler {
    token: ViewId,
    action_sender: MessageSender<Message<Action, LibAction>>,
}

#[export]
impl PlainListViewHandler {
    pub fn hint(&self, hint: Uuid) -> Result<(), MoniError> {
        self.action_sender
            .send_message(RunningAction::ListViewHint(self.token, ExpenseId::from(hint)))
            .map_err(|_| LibErrorCause::Sender)?;
        Ok(())
    }

    #[ffi_stream(item = MoniExpensePlainListSnapshot)]
    pub fn subscribe(&self) -> Arc<EventSubscription<MoniExpensePlainListSnapshot>> {
        let out = LibOutput::new(256);

        self.action_sender
            .send_message(LibAction::PlainListViewSubscription(self.token, out.clone()))
            .expect("TODO: panic message");

        out.into()
    }
}

pub struct MoniLib {
    action_sender: MessageSender<MoniMessage>,
    clock: Arc<dyn ClockSource + Send + Sync>,
    ids: RandomIdSource,
}

#[export]
impl MoniLib {
    pub fn new(path: String, config: LibConfig) -> Result<Self, MoniError> {
        _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(config.log_level))
            .try_init().inspect_err(|e|
            warn!("Unable to initialize logging, maybe MoniLib init called more than once?: {e:?}")
        );

        info!("Hi from MoniLib!");

        inout::try_state_path(&path)?;


        let (root_message_tx, message_rx) = mpsc::channel::<MoniMessage>();
        let dispatcher = MessageSender::new(root_message_tx);
        let actions_tx = dispatcher.clone();

        let clock = match config.clock {
            LibClockSource::System => Arc::new(SystemClockSource),
        };
        let clock_thread = clock.clone();

        let builder = Builder::new().name("messages".to_string());
        let _actions_handler = builder
            .spawn(move || {
                let config = RuntimeEnvironment {
                    messages_rx: message_rx,
                    actions_tx,
                    logging_enabled: true,
                    path,
                    clock: clock_thread,
                };
                let runtime = runtime::new(config);
                runtime.run();
            })
            .unwrap();

        Ok(MoniLib {
            action_sender: dispatcher,
            clock,
            ids: RandomIdSource,
        })
    }

    pub fn create_plain_list_view(&self) -> PlainListViewHandler {
        PlainListViewHandler {
            token: Uuid::now_v7().into(),
            action_sender: self.action_sender.clone(),
        }
    }

    pub fn add_expense(&self, expense: MoniExpense) -> Result<(), MoniError> {
        let expense = expense.into_expense(self.clock.as_ref(), &self.ids)?;
        self.action_sender
            .send_message(ModelAction::Add(expense))
            .map_err(|_| LibErrorCause::Sender)?;
        Ok(())
    }

    pub fn update_expense(&self, update: MoniExpenseUpdate) -> Result<(), MoniError> {
        let expense = update.into_updatable_expense(self.clock.as_ref())?;
        self.action_sender
            .send_message(ModelAction::Update(expense))
            .map_err(|_| LibErrorCause::Sender)?;
        Ok(())
    }

    pub fn delete_expense(&self, delete: Uuid) -> Result<(), MoniError> {
        self.action_sender
            .send_message(ModelAction::Delete(ExpenseId::from(delete)))
            .map_err(|_| LibErrorCause::Sender)?;
        Ok(())
    }

    pub fn calculate_statistics_all(&self) -> Result<(), MoniError> {
        self.action_sender
            .send_message(ModelAction::StatisticsAll)
            .map_err(|_| LibErrorCause::Sender)?;
        Ok(())
    }

    pub fn save(&self) {
        self.action_sender
            .send_message(WorkingAction::Save)
            .unwrap();
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

    pub fn watchdog(&self) {
        self.action_sender
            .send_message(WorkingAction::Watchdog)
            .unwrap();
    }
}
