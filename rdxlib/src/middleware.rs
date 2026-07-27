use enumset::EnumSetType;
use crate::{ActionProducts, Reducer};
use crate::cmd::SCmd;

pub trait ChainableMiddleware: Sized {
	type State;
	type Action: Send + 'static;
	type Flag: EnumSetType;
	type ServiceCmd: SCmd;

	fn pre(&mut self, state: &mut Self::State, action: &mut Self::Action);
	fn post(&mut self, final_state: &mut Self::State, products: &mut ActionProducts<Self>);
}

pub struct MiddlewareStore<CM: ChainableMiddleware> {
	funs: Vec<CM>,
	reducer: fn(&mut CM::State, CM::Action) -> ActionProducts<CM>,
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

struct Next<'n, CM: ChainableMiddleware> {
	remaining: &'n mut [CM],
	reducer: fn(&mut CM::State, CM::Action) -> ActionProducts<CM>,
}

impl<'n, CM: ChainableMiddleware> Next<'n, CM> {
	fn run(&mut self, state: &mut CM::State, mut action: CM::Action) -> ActionProducts<CM> {
		match self.remaining.split_first_mut() {
			None => (self.reducer)(state, action),
			Some((current, rest)) => {
				current.pre(state, &mut action);
					let mut products = Next {
						remaining: rest,
						reducer: self.reducer,
					}.run(state, action);
				current.post(state, &mut products);
				products
			}
		}
	}
}