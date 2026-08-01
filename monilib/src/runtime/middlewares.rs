use super::{Products, State, WorkingState};
use crate::action::{Action};
use crate::util::ClockSource;
use std::sync::Arc;
use tracing::debug;

pub struct MiddlewareConfig {
    pub(crate) logging_middleware: bool,
    pub(crate) clock_source: Arc<dyn ClockSource>,
}

type Reducer = fn(&mut State, Action) -> Products;

pub struct MiddlewareStore {
    funs: Vec<ChainableMiddleware>,
    reducer: Reducer,
}

impl MiddlewareStore {
    pub fn run(&mut self, state: &mut State, action: Action) -> Products {
        Next {
            remaining: &mut self.funs,
            reducer: self.reducer,
        }
        .run(state, action)
    }

    pub fn new(config: MiddlewareConfig, reducer: Reducer) -> Self {
        let mut funs: Vec<ChainableMiddleware> = vec![];
        if config.logging_middleware {
            funs.push(ChainableMiddleware::Logger);
        }
        funs.push(ChainableMiddleware::Clock(config.clock_source));
        funs.push(ChainableMiddleware::Cleaner);
        MiddlewareStore { funs, reducer }
    }
}

trait NextChainable {
    fn run(&mut self, state: &mut State, action: Action) -> Products;
}

struct Next<'n> {
    remaining: &'n mut [ChainableMiddleware],
    reducer: Reducer,
}

impl<'n> NextChainable for Next<'n> {
    fn run(&mut self, state: &mut State, action: Action) -> Products {
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

enum ChainableMiddleware {
    Logger,
    Clock(Arc<dyn ClockSource>),
    Cleaner
}

impl ChainableMiddleware {
    pub fn execute(
        &mut self,
        state: &mut State,
        action: Action,
        mut next: impl NextChainable,
    ) -> Products {
        use ChainableMiddleware::*;
        match self {
            Logger => {
                // if let State::Working(working) = &state {
                //     debug!("initial state before <{:?}> is {:?}", action, working.model);
                // }
                debug!("receiving action {action:?}");
                let products = next.run(state, action);
                // if let State::Working(working) = &state {
                //     debug!("ending state is {:?}", working.model);
                // }
                products
            }
            Clock(source) => {
                if let State::Working(WorkingState { model: _, running }) = state {
                    running.time = source.now_civil();
                }
                next.run(state, action)
            }
            Cleaner => {
                if let State::Working(working) = state {
                    working.running.errors.clear();
                }
                next.run(state, action)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use super::*;
    use crate::action::Action::NoOp;
    use crate::action::WorkingAction;
    use crate::runtime::Dirty;
    use crate::runtime::State::Working;
    use crate::runtime::cmd::Cmd::Subscribe;
    use crate::runtime::cmd::DebounceCmd::DelayedSave;
    use crate::runtime::cmd::Subscription::Debounce;
    use crate::runtime::cmd::{Cmd, DebounceAction};
    use crate::testing::{FakeClock, contemporary_ref_date};
    use crate::MoniDomainError;
    use uuid::Uuid;

    struct FakeNext<'a> {
        called: &'a mut u8,
    }
    impl NextChainable for FakeNext<'_> {
        fn run(&mut self, _state: &mut State, _action: Action) -> Products {
            *self.called += 1;
            Products::cmd(Cmd::Direct(vec![WorkingAction::WatchdogWatching]))
                .with_dirty(Dirty::FinancesCurrentMonth | Dirty::Categories)
                .with_delayed_save()
        }
    }

    struct FakeNextChainableClockTester;
    impl NextChainable for FakeNextChainableClockTester {
        fn run(&mut self, state: &mut State, action: Action) -> Products {
            let Working(WorkingState { model: _, running }) = state else {
                panic!("wrong final state")
            };
            assert_eq!(running.time, contemporary_ref_date());
            assert!(matches!(action, NoOp));
            Products::cmd(Cmd::Direct(vec![WorkingAction::WatchdogWatching]))
                .with_dirty(Dirty::FinancesCurrentMonth | Dirty::Categories)
                .with_delayed_save()
        }
    }

    struct FakeNextChainableCleanerTester;
    impl NextChainable for FakeNextChainableCleanerTester {
        fn run(&mut self, state: &mut State, action: Action) -> Products {
            let Working(WorkingState { model: _, running }) = state else {
                panic!("wrong final state")
            };
            assert!(running.errors.is_empty());
            assert!(matches!(action, NoOp));
            Products::cmd(Cmd::Direct(vec![WorkingAction::WatchdogWatching]))
                .with_dirty(Dirty::FinancesCurrentMonth | Dirty::Categories)
                .with_delayed_save()
        }
    }

    #[rstest]
    #[allow(clippy::arc_with_non_send_sync)]
    #[case(ChainableMiddleware::Clock(Arc::new(FakeClock::default())))]
    #[case(ChainableMiddleware::Logger)]
    #[case(ChainableMiddleware::Cleaner)]
    fn middleware_should_call_next_and_sink_products(#[case] mut middleware: ChainableMiddleware) {
        let working_state = WorkingState::default();
        let mut state = Working(working_state);

        let mut count = 0;
        let next_in_chain = FakeNext { called: &mut count };

        let products = middleware.execute(&mut state, NoOp, next_in_chain);

        assert_eq!(
            products.cmds,
            vec![
                Cmd::Direct(vec![WorkingAction::WatchdogWatching]),
                Subscribe(Debounce(DelayedSave(DebounceAction::Bump)))
            ]
        );
        assert_eq!(
            products.dirty,
            Dirty::FinancesCurrentMonth | Dirty::Categories
        );
        assert_eq!(count, 1);
    }

    #[test]
    #[allow(clippy::arc_with_non_send_sync)]
    fn clock_middleware_correct_state() {
        let mut middleware = ChainableMiddleware::Clock(Arc::new(FakeClock::default()));
        let working_state = WorkingState::default();
        let mut state = Working(working_state);

        let next_in_chain = FakeNextChainableClockTester;

        let products = middleware.execute(&mut state, NoOp, next_in_chain);

        assert_eq!(
            products.cmds,
            vec![
                Cmd::Direct(vec![WorkingAction::WatchdogWatching]),
                Subscribe(Debounce(DelayedSave(DebounceAction::Bump)))
            ]
        );
        assert_eq!(
            products.dirty,
            Dirty::FinancesCurrentMonth | Dirty::Categories
        );
    }

    #[test]
    fn cleaner_middleware_correct_state() {
        let mut middleware = ChainableMiddleware::Cleaner;
        let mut working_state = WorkingState::default();
        working_state
            .running
            .errors
            .push(MoniDomainError::ExpenseNotFound(Uuid::new_v4()).into());
        let mut state = Working(working_state);

        let next_in_chain = FakeNextChainableCleanerTester;

        let products = middleware.execute(&mut state, NoOp, next_in_chain);

        assert_eq!(
            products.cmds,
            vec![
                Cmd::Direct(vec![WorkingAction::WatchdogWatching]),
                Subscribe(Debounce(DelayedSave(DebounceAction::Bump)))
            ]
        );
        assert_eq!(
            products.dirty,
            Dirty::FinancesCurrentMonth | Dirty::Categories
        );
    }
}
