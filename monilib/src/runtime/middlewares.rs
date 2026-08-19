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
        next: &mut dyn Next<MoniLibClient>,
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
    use rstest::rstest;

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

    struct FakeNext {
        calls: u8,
        expected_time: Option<Zoned>,
    }

    impl FakeNext {
        fn new() -> Self {
            FakeNext {
                calls: 0,
                expected_time: None,
            }
        }
    }

    impl Next<MoniLibClient> for FakeNext {
        fn run(&mut self, state: &mut State, action: Action) -> MoniProducts {
            self.calls += 1;
            assert!(matches!(action, Action::Init));
            if let Some(expected) = &self.expected_time {
                assert_eq!(&state.running.time, expected);
            }
            next_products()
        }
    }

    #[rstest]
    #[case::clock(MoniMiddleware::Clock(Arc::new(StuckClock::default())))]
    #[case::logger_both(MoniMiddleware::Logger { prev: true, post: true })]
    #[case::logger_prev_only(MoniMiddleware::Logger { prev: true, post: false })]
    #[case::logger_post_only(MoniMiddleware::Logger { prev: false, post: true })]
    #[case::logger_none(MoniMiddleware::Logger { prev: false, post: false })]
    fn middleware_should_call_next_once_and_sink_its_products_untouched(
        #[case] mut middleware: MoniMiddleware,
    ) {
        let mut next = FakeNext::new();
        let mut state = State::default();

        let products = middleware.execute(&mut state, Action::Init, &mut next);

        assert_products(products);
        assert_eq!(next.calls, 1);
    }

    #[rstest]
    #[case::zero(AppState::Zero(vec![]))]
    #[case::failed(AppState::Failed)]
    #[case::working(AppState::Working(ModelState::default()))]
    fn clock_middleware_should_set_time_from_source_before_rest_of_chain_runs(
        #[case] app: AppState,
    ) {
        let mut next = FakeNext::new();
        next.expected_time = Some(contemporary_ref_date());
        let mut middleware = MoniMiddleware::Clock(Arc::new(StuckClock::default()));
        let mut state = State {
            app,
            ..State::default()
        };
        assert_ne!(state.running.time, contemporary_ref_date());

        let products = middleware.execute(&mut state, Action::Init, &mut next);

        assert_products(products);
        assert_eq!(next.calls, 1);
        assert_eq!(state.running.time, contemporary_ref_date());
    }
}
