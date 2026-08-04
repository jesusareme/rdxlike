mod timers;
pub use timers::Timers;

use super::{ModelState, MoniMessage, cmd::PersistenceCmd};
use crate::action::{Action, WorkingAction};
use crate::util::ClockSource;
use rdxlib::util::MessageSend;
use std::sync::Arc;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::mpsc::{self, Sender},
    thread::{self, JoinHandle},
};
use tracing::{debug, error};

pub struct Services {
    pub persistence: PersistenceService,
    pub timers: Timers,
}

impl Services {
    pub fn new(
        actions_sender: &impl MessageSend<Message=MoniMessage>,
        base_path: impl AsRef<Path>,
        clock: &Arc<dyn ClockSource + Send + Sync>,
    ) -> Self {
        let persistence_core = PersistenceServiceApi::new(base_path);
        Services {
            persistence: PersistenceService::new(actions_sender, persistence_core),
            timers: Timers::new(actions_sender, clock),
        }
    }
}

/// Service will most probably own the resources they need to execute actions, on its own thread.
pub trait Service {
    type Action: Send + 'static;
    type Context: ?Sized;
    type Cmd;
    fn execute(&self, to_execute: Self::Cmd);
    fn chooser(cmd: Self::Cmd, context: &Self::Context) -> Self::Action;
}

#[derive(Debug)]
pub enum PersistenceError {
    Parsing { source: serde_json::Error },
    Writing { source: io::Error },
}

impl Display for PersistenceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Persistence error with cause: {:?}", self)
    }
}

impl Error for PersistenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PersistenceError::Parsing { source } => Some(source),
            PersistenceError::Writing { source } => Some(source),
        }
    }
}

impl From<io::Error> for PersistenceError {
    fn from(value: io::Error) -> Self {
        PersistenceError::Writing { source: value }
    }
}

impl From<serde_json::Error> for PersistenceError {
    fn from(value: serde_json::Error) -> Self {
        PersistenceError::Parsing { source: value }
    }
}

pub struct PersistenceService {
    service_sender: Sender<Option<PersistenceCmd>>,
    thread_handle: JoinHandle<()>,
}

impl PersistenceService {
    pub fn new(action_sender: &impl MessageSend<Message=MoniMessage>, context: impl PersistenceApi + Send + 'static) -> Self {
        let (sender, receiver) = mpsc::channel::<Option<PersistenceCmd>>();
        let action_sender = action_sender.clone();
        let builder = thread::Builder::new().name("PersistenceService.thread".to_string());
        let handle = builder.spawn(move || {
            debug!("PersistenceService started");
            while let Some(to_execute) = receiver.recv().unwrap() {
                let action = Self::chooser(to_execute, &context);
                if action_sender.send_message(action).is_err() {
                    error!("PersistenceService: Unable to send resulting action");
                    break;
                }
            }
            debug!("BasicService ended");
        });

        PersistenceService {
            service_sender: sender,
            thread_handle: handle.unwrap(),
        }
    }
}

pub trait PersistenceApi {
    fn open_or_create_state(&self) -> io::Result<String>;
    fn save(&self, content: &str) -> io::Result<()>;
}

pub struct PersistenceServiceApi {
    base_path: PathBuf,
}

impl PersistenceServiceApi {
    fn new(base_path: impl AsRef<Path>) -> Self {
        let mut base_path = base_path.as_ref().to_path_buf();
        base_path.push("state.json");
        PersistenceServiceApi { base_path }
    }
}

impl PersistenceApi for PersistenceServiceApi {
    fn open_or_create_state(&self) -> io::Result<String> {
        let content = match fs::read_to_string(&self.base_path) {
            Ok(content) => content,
            Err(_) => {
                File::create(&self.base_path)?;
                String::new()
            }
        };
        Ok(content)
    }

    fn save(&self, content: &str) -> io::Result<()> {
        let mut tmp_patch = self.base_path.clone();
        tmp_patch.pop();
        tmp_patch.push("state.json.tmp");
        {
            let mut file = File::create(&tmp_patch)?;
            file.write_all(content.as_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp_patch, &self.base_path)?;
        _ = fs::remove_file(tmp_patch);
        Ok(())
    }
}

impl Service for PersistenceService {
    type Action = Action;
    type Context = dyn PersistenceApi;
    type Cmd = PersistenceCmd;
    fn execute(&self, to_execute: PersistenceCmd) {
        self.service_sender.send(Some(to_execute)).unwrap();
    }

    fn chooser(cmd: Self::Cmd, context: &Self::Context) -> Self::Action {
        match cmd {
            PersistenceCmd::CreateOrOpenFile => create_or_load_file(context),
            PersistenceCmd::Save(model) => save_state(model, context),
        }
    }
}

impl Drop for PersistenceService {
    fn drop(&mut self) {
        self.service_sender.send(None).unwrap();
    }
}

fn create_or_load_file(api: &dyn PersistenceApi) -> Action {
    match api.open_or_create_state() {
        Ok(content) => {
            let content = (!content.is_empty()).then_some(content);
            Action::InitResult(Ok(content))
        }
        Err(error) => Action::InitResult(Err(PersistenceError::from(error))),
    }
}

fn save_state(model: ModelState, api: &dyn PersistenceApi) -> Action {
    let content = match serde_json::to_string(&model) {
        Ok(content) => content,
        Err(error) => return Action::InitResult(Err(PersistenceError::from(error))),
    };
    drop(model);
    match api.save(&content) {
        Ok(_) => WorkingAction::SuccessfulSave.into(),
        Err(error) => Action::InitResult(Err(PersistenceError::from(error))),
    }
}

pub trait SubscriptionService<S> {
    fn subscribe(&mut self, subscription: S, action_tx: &impl MessageSend);
}

pub trait UnsubscriptableSubscriptionService<S>: SubscriptionService<S> {
    fn unsubscribe(&mut self, subscription: S);
}
