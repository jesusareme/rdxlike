use super::{MoniLibClient, MoniProducts, State};
use crate::action::Action;
use crate::util::ClockSource;
use rdxlib::middleware::{ChainableMiddleware, Next};
use std::sync::Arc;
use tracing::debug;

pub enum MoniMiddleware {
    Logger { prev: bool, post: bool },
    Clock(Arc<dyn ClockSource>),
}

impl ChainableMiddleware<MoniLibClient> for MoniMiddleware {
    fn execute(
        &mut self,
        state: &mut State,
        action: Action,
        mut next: Next<MoniLibClient>,
    ) -> MoniProducts {
        use MoniMiddleware::*;
        match self {
            Logger { prev, post } => {
                if *prev {
                    debug!("initial state before <{:?}> is {:?}", action, state);
                }
                debug!("receiving action {action:?}");
                let products = next.run(state, action);
                if *post {
                    debug!("ending state is {:?}", state);
                }
                products
            }
            Clock(source) => {
                state.running.time = source.now_civil();
                next.run(state, action)
            }
        }
    }
}
/*
todo!
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
    fn middleware_should_call_next_and_sink_products(#[case] mut middleware: MoniMiddleware) {
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
        let mut middleware = MoniMiddleware::Clock(Arc::new(FakeClock::default()));
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
        let mut middleware = MoniMiddleware::Cleaner;
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
*/
