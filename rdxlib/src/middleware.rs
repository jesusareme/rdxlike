use crate::{ActionProducts, Client, Reducer};

pub trait ChainableMiddleware<C: Client> {
    fn execute(
        &mut self,
        state: &mut C::State,
        action: C::Action,
        next: Next<C>,
    ) -> ActionProducts<C>;
}

pub struct MiddlewareStore<C: Client> {
    funs: Vec<Box<dyn ChainableMiddleware<C>>>,
    reducer: fn(&mut C::State, C::Action) -> ActionProducts<C>,
}

#[cfg(test)]
impl<C: Client> MiddlewareStore<C> {
    pub fn funs(&self) -> &Vec<Box<dyn ChainableMiddleware<C>>> {
        &self.funs
    }
}

pub struct Next<'n, C: Client> {
    remaining:  &'n mut [Box<dyn ChainableMiddleware<C>>],
    reducer: fn(&mut C::State, C::Action) -> ActionProducts<C>,
}

impl<'n, C: Client> Next<'n, C> {
    pub fn run(
        &mut self,
        state: &mut C::State,
        action: C::Action,
    ) -> ActionProducts<C> {
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

impl<C: Client> MiddlewareStore<C> {
    pub fn run(&mut self, state: &mut C::State, action: C::Action) -> ActionProducts<C> {
        Next {
            remaining: &mut self.funs,
            reducer: self.reducer,
        }
        .run(state, action)
    }

    pub fn new(funs: Vec<Box<dyn ChainableMiddleware<C>>>, reducer: Reducer<C>) -> Self {
        MiddlewareStore { funs, reducer }
    }
}