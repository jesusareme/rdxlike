//! Products model what reducers hand back to the runtime.
//!
//! Reducers do not perform effects or notify anyone; they describe what should happen next
//! and return it as one of the product types here.

use crate::Client;
use crate::cmd::Cmd;
use crate::subscribers::Subscriber;
use enumset::EnumSet;
use std::ops::{Add, AddAssign};

/// The outcomes of a business action are: side effects to run (as `cmds`) and what
/// parts of the state the action got dirty (as `flags`).
///
pub struct ActionProducts<C: Client> {
    /// Side effects for the runtime to carry out, in order.
    pub cmds: Vec<Cmd<C>>,

    /// Parts of the state this action touched.
    pub flags: EnumSet<C::Flag>,
}

impl<C: Client> ActionProducts<C> {
    /// No side effects, nothing got dirty.
    #[must_use]
    pub fn none() -> Self {
        ActionProducts {
            cmds: vec![],
            flags: EnumSet::empty(),
        }
    }

    /// A single side effect, nothing got dirty.
    #[must_use]
    pub fn cmd(cmd: impl Into<Cmd<C>>) -> Self {
        ActionProducts {
            cmds: vec![cmd.into()],
            flags: EnumSet::empty(),
        }
    }

    /// Several side effects, nothing got dirty.
    #[must_use]
    pub fn cmds(cmds: Vec<Cmd<C>>) -> Self {
        ActionProducts {
            cmds,
            flags: EnumSet::empty(),
        }
    }

    /// Adds one more side effect, keeping the existing ones and flags.
    #[must_use]
    pub fn with_cmd(mut self, cmd: impl Into<Cmd<C>>) -> Self {
        self.cmds.push(cmd.into());
        self
    }

    /// Marks more flags dirty, keeping everything already there.
    #[must_use]
    pub fn with_dirty(mut self, flags: impl Into<EnumSet<C::Flag>>) -> Self {
        self.flags |= flags.into();
        self
    }
}

/// The outcomes of a runtime action are: changes to the runtime rather than to the model
/// (`subscriber`) and `actions`.
pub struct RuntimeProducts<C: Client> {
    /// A subscriber to be register immediately by the Runtime.
    /// This Subscriber will be executed and passed current state later
    /// in this same loop execution.
    ///
    /// This has the effect of marking every flag dirty.
    pub subscriber: Option<Box<dyn Subscriber<Client = C>>>,

    /// Business actions to handle right after this runtime action, in the same loop execution,
    /// right after adding the subscriber (if any).
    pub actions: Vec<C::Action>,
}

impl<C: Client> RuntimeProducts<C> {
    /// Products that register a single subscriber and nothing else.
    pub fn subscriber(
        subscriber: impl Subscriber<Client = C> + 'static,
    ) -> Self {
        RuntimeProducts {
            subscriber: Some(Box::new(subscriber)),
            actions: vec![],
        }
    }

    /// Products for a runtime action that has no side effect.
    #[must_use]
    pub fn none() -> Self {
        RuntimeProducts {
            subscriber: None,
            actions: vec![],
        }
    }
}

impl<C: Client> Default for ActionProducts<C> {
    fn default() -> Self {
        ActionProducts::none()
    }
}

impl<C: Client> Add<ActionProducts<C>> for ActionProducts<C> {
    type Output = ActionProducts<C>;

    fn add(mut self, rhs: ActionProducts<C>) -> Self::Output {
        self += rhs;
        self
    }
}

impl<C: Client> AddAssign<ActionProducts<C>> for ActionProducts<C> {
    #[allow(clippy::suspicious_op_assign_impl)]
    fn add_assign(&mut self, rhs: ActionProducts<C>) {
        self.cmds.extend(rhs.cmds);
        self.flags |= rhs.flags;
    }
}
