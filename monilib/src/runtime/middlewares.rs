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
        use MoniMiddleware::{Clock, Logger};
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::WorkingAction;
    use crate::runtime::cmd::DebounceAction::Bump;
    use crate::runtime::cmd::DebounceCmd::DelayedSave;
    use crate::runtime::cmd::DelayedSaveProduct;
    use crate::runtime::{AppState, Dirty, ModelState};
    use crate::testing::{StuckClock, contemporary_ref_date};
    use jiff::Zoned;
    use rdxlib::cmd::Cmd;
    use rdxlib::middleware::MiddlewareStore;
    use rstest::rstest;
    use std::cell::Cell;
    use std::rc::Rc;

    fn next_products() -> MoniProducts {
        MoniProducts::cmd(Cmd::Direct(vec![WorkingAction::Save.into()]))
            .with_dirty(Dirty::FinancesCurrentMonth | Dirty::Categories)
            .with_delayed_save()
    }

    fn assert_products(products: MoniProducts) {
        assert_eq!(
            products.cmds,
            vec![
                Cmd::Direct(vec![WorkingAction::Save.into()]),
                DelayedSave(Bump).into(),
            ]
        );
        assert_eq!(
            products.flags,
            Dirty::FinancesCurrentMonth | Dirty::Categories
        );
    }

    fn fake_reducer(_: &mut State, action: Action) -> MoniProducts {
        assert!(matches!(action, Action::Init));
        next_products()
    }

    fn stuck_clock() -> MoniMiddleware {
        MoniMiddleware::Clock(Arc::new(StuckClock::default()))
    }

    struct FakeNext {
        pub calls: Rc<Cell<u8>>,
        pub expected_time: Option<Zoned>,
    }

    impl FakeNext {
        fn new() -> Self {
            FakeNext {
                calls: Rc::new(Cell::new(0)),
                expected_time: None,
            }
        }
    }

    impl ChainableMiddleware<MoniLibClient> for FakeNext {
        fn execute(
            &mut self,
            state: &mut State,
            action: Action,
            mut next: Next<MoniLibClient>,
        ) -> MoniProducts {
            self.calls.update(|calls| calls + 1);
            assert!(matches!(action, Action::Init));
            if let Some(expected) = &self.expected_time {
                assert_eq!(&state.running.time, expected);
            }
            next.run(state, action)
        }
    }

    fn chain(middleware: MoniMiddleware, next: FakeNext) -> MiddlewareStore<MoniLibClient> {
        MiddlewareStore::new(vec![middleware.boxed(), Box::new(next)], fake_reducer)
    }

    #[rstest]
    #[case::clock(stuck_clock())]
    #[case::logger_both(MoniMiddleware::Logger { prev: true, post: true })]
    #[case::logger_prev_only(MoniMiddleware::Logger { prev: true, post: false })]
    #[case::logger_post_only(MoniMiddleware::Logger { prev: false, post: true })]
    #[case::logger_none(MoniMiddleware::Logger { prev: false, post: false })]
    fn middleware_should_call_next_and_sink_products(#[case] middleware: MoniMiddleware) {
        let next = FakeNext::new();
        let calls = next.calls.clone();
        let mut store = chain(middleware, next);
        let mut state = State::default();

        let products = store.run(&mut state, Action::Init);

        assert_products(products);
        assert_eq!(calls.get(), 1);
    }

    #[rstest]
    #[case::zero(AppState::Zero(vec![]))]
    #[case::failed(AppState::Failed)]
    #[case::working(AppState::Working(ModelState::default()))]
    fn clock_middleware_sets_time_before_next_runs(#[case] app: AppState) {
        let mut next = FakeNext::new();
        next.expected_time = Some(contemporary_ref_date());
        let calls = next.calls.clone();
        let mut store = chain(stuck_clock(), next);
        let mut state = State {
            app,
            ..State::default()
        };
        assert_ne!(state.running.time, contemporary_ref_date());

        let products = store.run(&mut state, Action::Init);

        assert_products(products);
        assert_eq!(calls.get(), 1);
        assert_eq!(state.running.time, contemporary_ref_date());
    }
}
