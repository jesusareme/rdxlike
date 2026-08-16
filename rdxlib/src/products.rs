use std::ops::{Add, AddAssign};
use enumset::EnumSet;
use crate::Client;
use crate::cmd::Cmd;
use crate::subscribers::Subscriber;

pub struct RuntimeProducts<C: Client> {
	pub subscriber: Option<Box<dyn Subscriber<Flag = C::Flag, State = C::State>>>,
	pub actions: Vec<C::Action>,
}

impl<C: Client> RuntimeProducts<C> {
	pub fn subscriber(subscriber: impl Subscriber<Flag = C::Flag, State = C::State> + 'static) -> Self {
		RuntimeProducts {
			subscriber: Some(Box::new(subscriber)),
			actions: vec![],
		}
	}

	#[must_use]
	pub fn none() -> Self {
		RuntimeProducts {
			subscriber: None,
			actions: vec![],
		}
	}
}

pub struct ActionProducts<C: Client> {
	pub cmds: Vec<Cmd<C>>,
	pub flags: EnumSet<C::Flag>,
}

impl<C: Client> ActionProducts<C> {
	#[must_use]
	pub fn none() -> Self {
		ActionProducts {
			cmds: vec![],
			flags: EnumSet::empty(),
		}
	}

	#[must_use]
	pub fn cmd(cmd: impl Into<Cmd<C>>) -> Self {
		ActionProducts {
			cmds: vec![cmd.into()],
			flags: EnumSet::empty(),
		}
	}

	#[must_use]
	pub fn cmds(cmds: Vec<Cmd<C>>) -> Self {
		ActionProducts {
			cmds,
			flags: EnumSet::empty(),
		}
	}

	#[must_use]
	pub fn with_cmd(mut self, cmd: impl Into<Cmd<C>>) -> Self {
		self.cmds.push(cmd.into());
		self
	}

	#[must_use]
	pub fn with_dirty(mut self, flags: impl Into<EnumSet<C::Flag>>) -> Self {
		self.flags |= flags.into();
		self
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
