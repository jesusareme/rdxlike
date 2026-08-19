//! The chain of interceptors an action passes through before reaching the reducer.
//!
//! Each middleware sees the incoming action and state, and the outcoming products, so it
//! can log, replace the action, alter state and add side effects, or stop or bypass the
//! chain entirely.

use crate::{ActionProducts, Client, Reducer};

/// Middlewares are required to implement the `ChainableMiddleware` trait.
///
/// Calling [`Next::run`] passes control to the rest of the chain and finally to the
/// reducer; not calling it short-circuits, and the reducer never sees the action.
///
/// Middlewares should be seen as an additional layer of control and observation over the Runtime
/// for specific, mostly temporary use cases, and *never as a primary way of mutating client state*.
/// That's why currently Middlewares are fixed at configuration time and cannot be,
/// unlike `Subscribers`, added or removed.
///
/// Middlewares specifically allow for debugging operations and control that would otherwise be
/// impossible to achieve under the severe constraints a Redu/Elm-inspired system imposes, such as
/// simulating error conditions.
pub trait ChainableMiddleware<C: Client> {
    /// Common identity operation is passing received `action` to `next` and then returning whatever
    /// it produces without interferece. But complete access to actions, `state` and generated products
    /// is available.
    ///
    /// All middlewares run on the same main Runtime thread as [`Reducer`], therefore same recommendations
    /// apply: middleware should not block the thread or execute costly operations, unless forcing those
    /// situations is exactly what your middleware is designed for :)
    fn execute(
        &mut self,
        state: &mut C::State,
        action: C::Action,
        next: &mut dyn Next<C>,
    ) -> ActionProducts<C>;
}

/// A middleware's handle on the rest of the chain.
///
/// The Runtime always hands over the real chain, which ends at the [`Reducer`]. Being a trait
/// rather than a concrete type means a middleware can also be exercised in isolation against any
/// stand-in, so clients can unit-test their own middlewares without standing up a whole
/// [`Runtime`](crate::Runtime). Any closure of the right shape is already a `Next`.
pub trait Next<C: Client> {
    /// Runs the next middleware, or the reducer when none is left, and returns its products.
    ///
    /// The action, state or products may be swapped for different ones here.
    fn run(&mut self, state: &mut C::State, action: C::Action) -> ActionProducts<C>;
}

impl<C, F> Next<C> for F
where
    C: Client,
    F: FnMut(&mut C::State, C::Action) -> ActionProducts<C>,
{
    fn run(&mut self, state: &mut C::State, action: C::Action) -> ActionProducts<C> {
        self(state, action)
    }
}

pub(crate) struct MiddlewareStore<C: Client> {
    funs: Vec<Box<dyn ChainableMiddleware<C>>>,
    reducer: Reducer<C>,
}

/// The stretch of chain still ahead, walked one middleware at a time until the reducer closes it.
struct NextMiddlewares<'n, C: Client> {
    remaining: &'n mut [Box<dyn ChainableMiddleware<C>>],
    reducer: Reducer<C>,
}

impl<C: Client> Next<C> for NextMiddlewares<'_, C> {
    fn run(&mut self, state: &mut C::State, action: C::Action) -> ActionProducts<C> {
        match self.remaining.split_first_mut() {
            None => (self.reducer)(state, action),
            Some((current, rest)) => current.execute(
                state,
                action,
                &mut NextMiddlewares {
                    remaining: rest,
                    reducer: self.reducer,
                },
            ),
        }
    }
}

impl<C: Client> MiddlewareStore<C> {
    /// Sends an action through the whole chain and returns the products that come back up.
    pub fn run(&mut self, state: &mut C::State, action: C::Action) -> ActionProducts<C> {
        NextMiddlewares {
            remaining: &mut self.funs,
            reducer: self.reducer,
        }
        .run(state, action)
    }

    /// Builds the chain from middlewares in execution order, closed by `reducer`.
    pub fn new(funs: Vec<Box<dyn ChainableMiddleware<C>>>, reducer: Reducer<C>) -> Self {
        MiddlewareStore { funs, reducer }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Client;
    use crate::cmd::EnvironmentCommand;
    use crate::messages::Message;
    use crate::products::ActionProducts;
    use crate::util::MessageSender;
    use Log::{Entered, Exited};
    use enumset::EnumSetType;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct TestClient;
    impl Client for TestClient {
        type State = Vec<TestAction>;
        type Action = TestAction;
        type RuntimeAction = TestRuntime;
        type Flag = TestFlag;
        type Environment = ();
        type ServiceCommand = EmptyService;
    }

    #[derive(Debug, Clone, PartialEq)]
    struct TestAction(&'static str);

    impl From<TestAction> for Message<TestAction, TestRuntime> {
        fn from(action: TestAction) -> Self {
            Message::Action(action)
        }
    }

    struct TestRuntime;

    impl From<TestRuntime> for Message<TestAction, TestRuntime> {
        fn from(runtime: TestRuntime) -> Self {
            Message::Runtime(runtime)
        }
    }

    #[derive(EnumSetType, Debug)]
    enum TestFlag {
        A,
        B,
    }

    #[derive(Debug, PartialEq, Clone)]
    struct EmptyService;
    impl EnvironmentCommand for EmptyService {
        type Client = TestClient;
        fn process(
            self,
            _: &mut (),
            _: &MessageSender<Message<TestAction, TestRuntime>>,
        ) -> Vec<Message<TestAction, TestRuntime>> {
            vec![]
        }
    }

    fn witness_reducer(
        state: &mut Vec<TestAction>,
        action: TestAction,
    ) -> ActionProducts<TestClient> {
        state.push(action);
        ActionProducts::none()
    }

    fn flag_b_reducer(
        state: &mut Vec<TestAction>,
        action: TestAction,
    ) -> ActionProducts<TestClient> {
        state.push(action);
        ActionProducts::none().with_dirty(TestFlag::B)
    }

    #[derive(Debug, PartialEq, Clone)]
    enum Log {
        Entered(&'static str),
        Exited(&'static str),
    }
    type Logger = Rc<RefCell<Vec<Log>>>;

    struct TestMiddleware {
        name: &'static str,
        logger: Logger,
    }
    impl ChainableMiddleware<TestClient> for TestMiddleware {
        fn execute(
            &mut self,
            state: &mut Vec<TestAction>,
            action: TestAction,
            next: &mut dyn Next<TestClient>,
        ) -> ActionProducts<TestClient> {
            self.logger.borrow_mut().push(Entered(self.name));
            let products = next.run(state, action);
            self.logger.borrow_mut().push(Exited(self.name));
            products
        }
    }

    struct ShortCircuitingMiddleware {
        name: &'static str,
        logger: Logger,
    }
    impl ChainableMiddleware<TestClient> for ShortCircuitingMiddleware {
        fn execute(
            &mut self,
            _state: &mut Vec<TestAction>,
            _action: TestAction,
            _next: &mut dyn Next<TestClient>,
        ) -> ActionProducts<TestClient> {
            self.logger.borrow_mut().push(Entered(self.name));
            ActionProducts::none().with_dirty(TestFlag::B)
        }
    }

    struct ActionReplacingMiddleware {
        new_action: TestAction,
    }
    impl ChainableMiddleware<TestClient> for ActionReplacingMiddleware {
        fn execute(
            &mut self,
            state: &mut Vec<TestAction>,
            _action: TestAction,
            next: &mut dyn Next<TestClient>,
        ) -> ActionProducts<TestClient> {
            next.run(state, self.new_action.clone())
        }
    }

    struct StateMutatingMiddleware {
        injected: TestAction,
    }
    impl ChainableMiddleware<TestClient> for StateMutatingMiddleware {
        fn execute(
            &mut self,
            state: &mut Vec<TestAction>,
            action: TestAction,
            next: &mut dyn Next<TestClient>,
        ) -> ActionProducts<TestClient> {
            state.push(self.injected.clone());
            next.run(state, action)
        }
    }

    struct ProductUpdatingMiddleware {
        flag: TestFlag,
    }
    impl ChainableMiddleware<TestClient> for ProductUpdatingMiddleware {
        fn execute(
            &mut self,
            state: &mut Vec<TestAction>,
            action: TestAction,
            next: &mut dyn Next<TestClient>,
        ) -> ActionProducts<TestClient> {
            next.run(state, action).with_dirty(self.flag)
        }
    }

    fn boxed(
        middleware: impl ChainableMiddleware<TestClient> + 'static,
    ) -> Box<dyn ChainableMiddleware<TestClient>> {
        Box::new(middleware)
    }

    #[test]
    fn empty_store_should_run_reducer_directly() {
        let mut store = MiddlewareStore::new(vec![], witness_reducer);
        let mut state = vec![];

        let products = store.run(&mut state, TestAction("action"));

        assert_eq!(state, vec![TestAction("action")]);
        assert!(products.flags.is_empty());
        assert!(products.cmds.is_empty());
    }

    #[test]
    fn middlewares_should_run_in_registration_order() {
        let logger = Logger::default();
        let mut store = MiddlewareStore::new(
            vec![
                boxed(TestMiddleware {
                    name: "first",
                    logger: logger.clone(),
                }),
                boxed(TestMiddleware {
                    name: "second",
                    logger: logger.clone(),
                }),
            ],
            witness_reducer,
        );
        let mut state = vec![];

        store.run(&mut state, TestAction("ping"));

        assert_eq!(
            *logger.borrow(),
            vec![
                Entered("first"),
                Entered("second"),
                Exited("second"),
                Exited("first")
            ]
        );
        assert_eq!(state, vec![TestAction("ping")]);
    }

    #[test]
    fn products_from_reducer_should_rise_up_through_middlewares() {
        let mut store = MiddlewareStore::new(
            vec![boxed(TestMiddleware {
                name: "only",
                logger: Logger::default(),
            })],
            flag_b_reducer,
        );
        let mut state = vec![];

        let products = store.run(&mut state, TestAction("action"));

        assert_eq!(products.flags, TestFlag::B);
    }

    #[test]
    fn middleware_that_skips_next_should_short_circuit_next_in_chain() {
        let logger = Logger::default();
        let mut store = MiddlewareStore::new(
            vec![
                boxed(ShortCircuitingMiddleware {
                    name: "stop",
                    logger: logger.clone(),
                }),
                boxed(TestMiddleware {
                    name: "never",
                    logger: logger.clone(),
                }),
            ],
            witness_reducer,
        );
        let mut state = vec![];

        let products = store.run(&mut state, TestAction("action"));

        assert_eq!(*logger.borrow(), vec![Entered("stop")]);
        assert!(state.is_empty());
        assert_eq!(products.flags, TestFlag::B);
    }

    #[test]
    fn middleware_replacing_action_should_propagate_reaching_reducer() {
        let mut store = MiddlewareStore::new(
            vec![boxed(ActionReplacingMiddleware {
                new_action: TestAction("replacement"),
            })],
            witness_reducer,
        );
        let mut state = vec![];

        store.run(&mut state, TestAction("basic"));

        assert_eq!(state, vec![TestAction("replacement")]);
    }

    #[test]
    fn middleware_mutating_state_should_apply_it_before_rest_of_chain_runs() {
        let mut store = MiddlewareStore::new(
            vec![
                boxed(StateMutatingMiddleware {
                    injected: TestAction("injected"),
                }),
                boxed(TestMiddleware {
                    name: "downstream",
                    logger: Logger::default(),
                }),
            ],
            witness_reducer,
        );
        let mut state = vec![];

        store.run(&mut state, TestAction("basic"));

        assert_eq!(state, vec![TestAction("injected"), TestAction("basic")]);
    }

    #[test]
    fn middleware_updating_products_updated_products_should_return_from_next() {
        let mut store = MiddlewareStore::new(
            vec![boxed(ProductUpdatingMiddleware { flag: TestFlag::A })],
            flag_b_reducer,
        );
        let mut state = vec![];

        let products = store.run(&mut state, TestAction("basic"));

        assert_eq!(products.flags, TestFlag::A | TestFlag::B);
    }
}
