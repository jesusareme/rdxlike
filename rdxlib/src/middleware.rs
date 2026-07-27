use enumset::EnumSetType;
use crate::{ActionProducts, Reducer};
use crate::cmd::SCmd;

pub trait ChainableMiddleware: Sized {
	type State;
	type Action: Send + 'static;
	type Flag: EnumSetType;
	type ServiceCmd: SCmd;

	fn execute(
		&mut self,
		state: &mut Self::State,
		action: Self::Action,
		next: impl NextChainable,
	) -> ActionProducts<Self>;
}

pub struct MiddlewareStore<CM: ChainableMiddleware> {
	funs: Vec<CM>,
	reducer: fn(&mut CM::State, CM::Action) -> ActionProducts<CM>,
}

pub trait NextChainable {
	type CM: ChainableMiddleware;

	fn run(&mut self,
		   state: &mut <Self::CM as ChainableMiddleware>::State,
		   action: <Self::CM as ChainableMiddleware>::Action
	) -> ActionProducts<Self::CM>;
}

struct Next<'n, CM: ChainableMiddleware> {
	remaining: &'n mut [CM],
	reducer: fn(&mut CM::State, CM::Action) -> ActionProducts<CM>,
}

impl<'n, CM: ChainableMiddleware> NextChainable for Next<'n, CM> {
	type CM = CM;

	fn run(&mut self,
	       state: &mut <Self::CM as ChainableMiddleware>::State,
	       action: <Self::CM as ChainableMiddleware>::Action
	) -> ActionProducts<Self::CM> {
		match self.remaining.split_first_mut() {
			None => (self.reducer)(state, action),
			Some((current, rest)) => current.execute(
				state,
				action,
				Next {
					remaining: rest,
					reducer: self.reducer,
				},
			),
		}
	}
}

impl<CM: ChainableMiddleware> MiddlewareStore<CM> {
	pub fn run(&mut self, state: &mut CM::State, action: CM::Action) -> ActionProducts<CM> {
		Next {
			remaining: &mut self.funs,
			reducer: self.reducer,
		}.run(state, action)
	}

	pub fn new(funs: Vec<CM>, reducer: Reducer<CM>) -> Self {
		MiddlewareStore {
			funs,
			reducer,
		}
	}
}

impl<'n, CM: ChainableMiddleware> Next<'n, CM> {
	fn run(&mut self, state: &mut CM::State, action: CM::Action) -> ActionProducts<CM> {
		match self.remaining.split_first_mut() {
			None => (self.reducer)(state, action),
			Some((current, rest)) => {
				current.execute(
					state,
					action,
					Next {
						remaining: rest,
						reducer: self.reducer,
					}
				)
			}
		}
	}
}