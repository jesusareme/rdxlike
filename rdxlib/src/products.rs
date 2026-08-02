use std::ops::{Add, AddAssign};
use enumset::EnumSet;
use crate::Client;
use crate::cmd::Cmd;
use crate::subscribers::Subscriber;

pub struct RuntimeProducts<C: Client> {
	pub subscriber: Option<Box<dyn Subscriber<Flag= C::Flag, State= C::State>>>,
	pub actions: Vec<C::Action>,
}

pub struct ActionProducts<C: Client> {
	pub cmds: Vec<Cmd<C::Action, C::ServiceCommand>>,
	pub dirty: EnumSet<C::Flag>,
}

impl<C: Client> ActionProducts<C> {
	pub fn none() -> Self {
		ActionProducts {
			cmds: vec![],
			dirty: EnumSet::empty(),
		}
	}

	pub fn cmd(cmd: impl Into<Cmd<C::Action, C::ServiceCommand>>) -> Self {
		ActionProducts {
			cmds: vec![cmd.into()],
			dirty: EnumSet::empty(),
		}
	}

	pub fn cmds(cmds: Vec<Cmd<C::Action, C::ServiceCommand>>) -> Self {
		ActionProducts {
			cmds,
			dirty: EnumSet::empty(),
		}
	}

	pub fn with_cmd(mut self, cmd: impl Into<Cmd<C::Action, C::ServiceCommand>>) -> Self {
		self.cmds.push(cmd.into());
		self
	}

	pub fn with_dirty(mut self, flags: impl Into<EnumSet<C::Flag>>) -> Self {
		self.dirty |= flags.into();
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
		self.dirty |= rhs.dirty;
	}
}
