use super::{
    AppState, Dirty, EnumSet, Expense, ModelAction, ModelState, MoniDomainError, MoniError,
    MoniProducts, PlainListViewState, RunningAction, RunningState, State, Uuid, Zoned,
    cmd::{AsyncCmd, DelayedSaveProduct, PersistenceCmd, TimeSubscriptionCmd},
    debug,
};
use crate::LibErrorCause;
use crate::action::WorkingAction::{Model, Save, SuccessfulSave};
use crate::inout::ExpenseAddIntent;
use crate::runtime::Dirty::Statistics;
use crate::runtime::Statistics as StatisticsData;
use crate::runtime::cmd::DebounceCmd::DelayedSave;
use crate::runtime::model_views::ClockedModelStateView;
use crate::util::{DropCancellation, ExpenseId, IdSource};
use crate::{action::Action, runtime::cmd::DebounceAction::Cancel};
use rdxlib::cmd::Cmd::Direct;
use std::fmt::Debug;
use std::mem;
use tracing::error;

pub fn reducer(state: &mut State, action: Action) -> MoniProducts {
    use Action::{Init, InitResult, Running, Working};
    match action {
        Init => MoniProducts::cmd(PersistenceCmd::CreateOrOpenFile),

        InitResult(result) => {
            let model = match result {
                Ok(None) => ModelState::default(),
                Ok(Some(content)) => match serde_json::from_str::<ModelState>(&content) {
                    Ok(model) => model,
                    Err(error) => return failed_init(state, error),
                },
                Err(error) => return failed_init(state, error),
            };

            match mem::replace(&mut state.app, AppState::Working(model)) {
                AppState::Zero(pending) => {
                    let pending: Vec<Action> = pending.into_iter().map(Into::into).collect();
                    MoniProducts::cmd(Direct(pending))
                }
                _ => MoniProducts::none().with_dirty(EnumSet::all()),
            }
        }

        Running(action) => reducer_running(&mut state.running, action),

        Working(action) => match &mut state.app {
            AppState::Zero(pending) => {
                pending.push(action);
                MoniProducts::none()
            }
            AppState::Failed => {
                MoniProducts::none()
                //todo!
            }
            AppState::Working(model) => match action {
                Save => MoniProducts::cmds(vec![
                    PersistenceCmd::Save(model.clone()).into(),
                    DelayedSave(Cancel).into(),
                ]),
                SuccessfulSave => {
                    debug!("Successful SAVE!");
                    MoniProducts::none()
                }

                Model(action) => reducer_model(
                    &mut ClockedModelStateView::new(model, &mut state.running),
                    action,
                ),
            },
        },
    }
}

fn failed_init(state: &mut State, cause: impl Debug) -> MoniProducts {
    let error = MoniError::from(LibErrorCause::StateLoad(format!("{cause:?}")));
    error!("MoniLib was unable to initialize: {error}");
    state.running.errors.push(error);
    if let AppState::Zero(actions) = mem::replace(&mut state.app, AppState::Failed) {
        error!(
            "These pending actions received while initializing store will be discarded: {:?}",
            actions
        );
    }

    MoniProducts::none()
}

fn reducer_running(state: &mut RunningState, action: RunningAction) -> MoniProducts {
    match action {
        RunningAction::Error(error) => {
            state.errors.push(error);
            MoniProducts::none()
        }

        RunningAction::ListViewHint(token, id) => {
            if let Some(view_state) = state.plain_list.get_mut(&token) {
                view_state.hint = Some(id);
            }
            MoniProducts::none().with_dirty(Dirty::Views)
        }
        RunningAction::ListViewPrepare(token) => {
            state
                .plain_list
                .insert(token, PlainListViewState { hint: None });

            MoniProducts::none().with_dirty(Dirty::Views)
        }
    }
}

fn finances_dirty(date: &Zoned, first_of_month: &Zoned) -> Dirty {
    if date >= first_of_month {
        Dirty::FinancesCurrentMonth
    } else {
        Dirty::FinancesBeforeThisMonth
    }
}

fn reducer_model(state: &mut ClockedModelStateView, action: ModelAction) -> MoniProducts {
    match action {
        ModelAction::Add(expense_intent) => add_expense(state, expense_intent),

        ModelAction::Update(updated_expense) => update_expense(state, updated_expense),

        ModelAction::Delete(id) => delete_expense(state, id),

        ModelAction::StatisticsAll => request_statistics(state),

        ModelAction::StatisticsAllResult(statistics) => receive_statistics(state, statistics),

        ModelAction::CancelStatistics => {
            state.tasks.statistics_running = None;
            MoniProducts::none()
        }

        ModelAction::AddEveryXInterval(id, interval, action) => {
            state.tasks.recurrent_add.insert(id);
            MoniProducts::cmd(TimeSubscriptionCmd::EveryXInterval(id, interval, *action))
        }

        ModelAction::StopAddingEveryXInterval(id) => {
            if let Some(uuid) = state.tasks.recurrent_add.take(&id) {
                MoniProducts::cmd(TimeSubscriptionCmd::CancelEveryXInterval(uuid))
            } else {
                MoniProducts::none()
            }
        }
    }
}

fn add_expense(
    state: &mut ClockedModelStateView,
    expense_intent: ExpenseAddIntent,
) -> MoniProducts {
    let first_of_month = state
        .time
        .first_of_month()
        .expect("first_of_month from an injected Zoned cannot fail");

    let date = expense_intent.date.unwrap_or_else(|| state.time.clone());

    let dirty = finances_dirty(&date, &first_of_month);

    let idx = state
        .model_state
        .movements
        .partition_point(|e| e.date <= date);

    let id = state.model_state.ids.next_expense_id.get_and_inc();
    let expense = Expense::new(
        id,
        date,
        expense_intent.amount,
        expense_intent.comment,
        expense_intent.category,
    );

    state.model_state.movements.update_with(|expenses| {
        expenses.insert(idx, expense);
    });

    MoniProducts::none().with_dirty(dirty).with_delayed_save()
}

fn update_expense(state: &mut ClockedModelStateView, updated_expense: Expense) -> MoniProducts {
    let Some(idx) = state
        .model_state
        .movements
        .iter()
        .position(|current| current.id == updated_expense.id)
    else {
        state
            .errors
            .push(MoniDomainError::ExpenseNotFound(updated_expense.id.into()).into());
        return MoniProducts::none();
    };

    let previous = state
        .model_state
        .movements
        .get(idx)
        .expect("element should exist");

    if *previous == updated_expense {
        return MoniProducts::none();
    }

    let first_of_month = state
        .time
        .first_of_month()
        .expect("first_of_month from an injected Zoned cannot fail");
    let updated_dirty = finances_dirty(&updated_expense.date, &first_of_month);

    state.model_state.movements.update_with(|expenses| {
        let previous = expenses.get_mut(idx).expect("element should exist");
        let previous_dirty = finances_dirty(&previous.date, &first_of_month);

        if previous.date == updated_expense.date {
            // We set in-place, order is not changing
            *previous = updated_expense;
        } else {
            // Remove and guarantee order
            _ = expenses.remove(idx);
            let updated_idx = expenses.partition_point(|e| e.date <= updated_expense.date);
            expenses.insert(updated_idx, updated_expense);
        }

        MoniProducts::none()
            .with_dirty(previous_dirty | updated_dirty)
            .with_delayed_save()
    })
}

fn delete_expense(state: &mut ClockedModelStateView, id: ExpenseId) -> MoniProducts {
    let Some(idx) = state
        .model_state
        .movements
        .iter()
        .position(|current| current.id == id)
    else {
        state
            .errors
            .push(MoniDomainError::ExpenseNotFound(id.into()).into());
        return MoniProducts::none();
    };

    let first_of_month = state
        .time
        .first_of_month()
        .expect("first_of_month should fail here");

    let removed = state
        .model_state
        .movements
        .update_with(|expenses| expenses.remove(idx));

    let dirty = finances_dirty(&removed.date, &first_of_month);

    MoniProducts::none().with_dirty(dirty).with_delayed_save()
}

fn request_statistics(state: &mut ClockedModelStateView) -> MoniProducts {
    match &mut state.model_state.statistics_all {
        Some(s)
            if s.at_movements_version == state.model_state.movements.version()
                && state.tasks.statistics_running.is_none() =>
        {
            s.requested_at = state.time.timestamp();
            MoniProducts::none().with_dirty(Statistics)
        }

        _ => {
            if state.tasks.statistics_running.is_some() {
                // Only one statistics calculation running at any time (just to simplify example)
                debug!("StatisticsSub request was ignored as there was a previous one running");
                return MoniProducts::none();
            }

            let cancellation_token = DropCancellation::new(Uuid::new_v4());
            let cancellation_check = cancellation_token.cancellation_check();

            state.tasks.statistics_running = Some(cancellation_token);

            MoniProducts::cmd(AsyncCmd::StatisticsCalculation(
                state.model_state.movements.clone(),
                state.time.timestamp(),
                cancellation_check,
            ))
        }
    }
}

fn receive_statistics(
    state: &mut ClockedModelStateView,
    statistics: Option<StatisticsData>,
) -> MoniProducts {
    let Some(statistics) = statistics else {
        debug!("StatisticsSub calculation was cancelled");
        return MoniProducts::none();
    };

    if matches!(state.model_state.statistics_all, Some(s) if s.requested_at >= statistics.requested_at)
    {
        debug!(
            "StatisticsSub calculation was discarded as we have a more up to date version already calculated"
        );
        return MoniProducts::none();
    }
    state.model_state.statistics_all = Some(statistics);

    MoniProducts::none().with_dirty(Statistics)
}

#[cfg(test)]
mod reducer_model_test {
    use super::*;
    use crate::MoniErrorType;
    use crate::inout::ExpenseAddIntent;
    use crate::runtime::ExpenseCategory::*;
    use crate::runtime::cmd::DebounceAction::Bump;
    use crate::runtime::cmd::DebounceCmd::DelayedSave;
    use crate::runtime::cmd::ServiceCommand::Subscribe;
    use crate::runtime::cmd::Subscription::{Debounce, Time};
    use crate::runtime::{ExpenseCategory, Ids, LongLivingTasks};
    use crate::testing::{
        alternative_ref_uuid, contemporary_ref_date, ordered_by_index_map, ref_uuid,
    };
    use crate::util::ExpenseId;
    use crate::util::VersionedArc;
    use itertools::Itertools;
    use jiff::{Span, ToSpan};
    use proptest::prelude::*;
    use proptest::{prop_compose, proptest};
    use rdxlib::cmd::Cmd::Env;
    use rstest::rstest;
    use std::time::{Duration, SystemTime};
    use uuid::Uuid;

    fn expense_in_ref_month() -> Expense {
        let past = contemporary_ref_date().first_of_month().unwrap();
        Expense {
            id: ExpenseId::from(0),
            date: past,
            ..Expense::default()
        }
    }

    fn expense_just_before_ref_month() -> Expense {
        let past = contemporary_ref_date().first_of_month().unwrap() - 1.nanosecond();
        Expense {
            id: ExpenseId::from(0),
            date: past,
            ..Expense::default()
        }
    }

    fn expenses_list(date_last: Zoned, step_back: Span) -> Vec<Expense> {
        let calculate_date = |back: i64| &date_last - step_back * back;
        vec![
            Expense {
                id: ExpenseId::from(0),
                date: calculate_date(5),
                amount: -1000,
                ..Expense::default()
            },
            Expense {
                id: ExpenseId::from(1),
                date: calculate_date(4),
                amount: 20000,
                category: Optional,
                comment: None,
            },
            Expense {
                id: ExpenseId::from(2),
                date: calculate_date(3),
                amount: 3400,
                category: Important,
                comment: Some("Pair of shoes".to_string()),
            },
            Expense {
                id: ExpenseId::from(3),
                date: calculate_date(2),
                ..Expense::default()
            },
            Expense {
                id: ExpenseId::from(4),
                date: calculate_date(1),
                amount: 100,
                ..Expense::default()
            },
            Expense {
                id: ExpenseId::from(5),
                date: calculate_date(0),
                category: Essential,
                ..Expense::default()
            },
        ]
    }

    fn reduce(
        state: &mut ModelState,
        time: &Zoned,
        errors: &mut Vec<MoniError>,
        action: ModelAction,
    ) -> MoniProducts {
        reduce_with_tasks(state, time, errors, &mut LongLivingTasks::default(), action)
    }

    fn reduce_with_tasks(
        state: &mut ModelState,
        time: &Zoned,
        errors: &mut Vec<MoniError>,
        tasks: &mut LongLivingTasks,
        action: ModelAction,
    ) -> MoniProducts {
        reducer_model(
            &mut ClockedModelStateView {
                model_state: state,
                time,
                errors,
                tasks,
            },
            action,
        )
    }

    fn ref_interval() -> Duration {
        Duration::from_secs(30)
    }

    fn add_every_interval(id: Uuid) -> ModelAction {
        ModelAction::AddEveryXInterval(id, ref_interval(), Box::new(Save))
    }

    fn add_intent(expense: &Expense) -> ModelAction {
        ModelAction::Add(ExpenseAddIntent {
            date: Some(expense.date.clone()),
            amount: expense.amount,
            comment: expense.comment.clone(),
            category: expense.category,
        })
    }

    fn expense_category_strategy() -> impl Strategy<Value = ExpenseCategory> {
        prop_oneof![Just(Important), Just(Essential), Just(Optional),]
    }

    prop_compose! {
        fn arb_expenses()(
            amount in any::<i64>(),
            comment in any::<Option<String>>(),
            date in any::<SystemTime>(),
            category in expense_category_strategy(),
        ) -> Expense {
            Expense::new(
                ExpenseId::default(),
                Zoned::try_from(date).unwrap(),
                amount,
                comment,
                category,
            )
        }
    }

    prop_compose! {
        fn arb_expenses_and_date_offsets()
        (expenses in proptest::collection::vec(arb_expenses(), 1..=300))
        (new_dates in proptest::collection::vec(any::<SystemTime>(), expenses.len()), expenses in Just(expenses)) -> (Vec<Expense>, Vec<SystemTime>) {
            (expenses, new_dates)
        }
    }

    proptest! {
        #[test]
        fn expenses_add_proptest((expenses, new_dates) in arb_expenses_and_date_offsets()) {
            prop_assume!(expenses.len() == new_dates.len(), "Test configuration issue: not enough durations");

            let mut state = ModelState::default();
            let mut errors = vec![];
            let mut products = MoniProducts::none();
            expenses.clone().into_iter().for_each(|e| {
                products += reduce(
                    &mut state,
                    &contemporary_ref_date(),
                    &mut errors,
                    add_intent(&e),
                );
                assert!(state.movements.is_sorted_by_key(|e| e.date.clone()));
            });

            assert!(errors.is_empty());
            assert_eq!(state.movements.len(), expenses.len());

            let saves: Vec<_> = products.cmds.iter().filter(|cmd| {
                matches!(cmd, Env(Subscribe(Debounce(DelayedSave(Bump)))))
            }).collect();
            assert_eq!(saves.len(), expenses.len());

            let mut stored: Vec<Expense> = state.movements.to_vec();

            let ids: Vec<_> = stored.iter().map(|e| e.id).collect();

            products = MoniProducts::none();

            for (e, d) in stored.iter_mut().zip(new_dates) {
                let d = Zoned::try_from(d);
                prop_assume!(d.is_ok(), "Unexpected date unable to convert to Zoned");
                e.date = d.unwrap();
                products += reduce(
                    &mut state,
                    &contemporary_ref_date(),
                    &mut errors,
                    ModelAction::Update(e.clone()),
                );
                assert!(state.movements.is_sorted_by_key(|e| e.date.clone()));
            }

            assert_eq!(state.movements.len(), expenses.len());
            let saves: Vec<_> = products.cmds.iter().filter(|cmd| {
                matches!(cmd, Env(Subscribe(Debounce(DelayedSave(Bump)))))
            }).collect();
            assert_eq!(saves.len(), expenses.len());
            assert!(errors.is_empty());

            ids.into_iter().for_each(|id| {
                _ = reduce(
                    &mut state,
                    &contemporary_ref_date(),
                    &mut errors,
                    ModelAction::Delete(id),
                );
                assert!(state.movements.is_sorted_by_key(|e| e.date.clone()));
            });

            assert!(errors.is_empty());
            assert!(state.movements.is_empty());
        }
    }

    #[rstest]
    #[case::month_before(
        expense_just_before_ref_month(),
        EnumSet::only(Dirty::FinancesBeforeThisMonth)
    )]
    #[case::current_month(expense_in_ref_month(), EnumSet::only(Dirty::FinancesCurrentMonth))]
    fn reducer_model_add_expense(#[case] expense: Expense, #[case] dirty: EnumSet<Dirty>) {
        let mut state = ModelState::default();
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            add_intent(&expense),
        );

        assert_eq!(products.flags, dirty);
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );

        assert!(errors.is_empty());
        assert_eq!(*state.movements, vec![expense]);
    }

    #[test]
    fn reducer_model_add_expense_sorted() {
        let original = expenses_list(contemporary_ref_date(), 1.month());

        for expenses_perm in original.iter().permutations(original.len()) {
            let mut state = ModelState::default();
            let mut errors = vec![];

            let mut expected: Vec<Expense> = expenses_perm
                .iter()
                .enumerate()
                .map(|(assigned, expense)| Expense {
                    id: ExpenseId::from(assigned as u64),
                    ..(*expense).clone()
                })
                .collect();
            expected.sort_by(|a, b| a.date.cmp(&b.date));

            for expense in expenses_perm {
                _ = reduce(
                    &mut state,
                    &contemporary_ref_date(),
                    &mut errors,
                    add_intent(expense),
                );
            }

            assert!(errors.is_empty());
            assert_eq!(*state.movements, expected);
        }
    }

    #[test]
    fn reducer_model_add_expense_same_date_inserts_newest_last() {
        let shared = contemporary_ref_date().first_of_month().unwrap();
        let first = Expense {
            id: ExpenseId::from(0),
            date: shared.clone(),
            amount: 1,
            ..Expense::default()
        };
        let second = Expense {
            id: ExpenseId::from(1),
            date: shared.clone(),
            amount: 2,
            ..Expense::default()
        };
        let mut state = ModelState::default();
        let mut errors = vec![];

        _ = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            add_intent(&first),
        );
        _ = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            add_intent(&second),
        );

        assert!(errors.is_empty());
        assert_eq!(*state.movements, vec![first, second]);
    }

    #[test]
    fn reducer_model_add_expense_into_mid_list() {
        let expenses = expenses_list(contemporary_ref_date(), 1.month());
        let mid = Expense {
            id: ExpenseId::from(expenses.len() as u64),
            date: &expenses[3].date + 1.nanosecond(),
            ..Expense::default()
        };
        let mut state = ModelState {
            ids: Ids {
                next_expense_id: ExpenseId::from(expenses.len() as u64),
            },
            movements: VersionedArc::from(expenses),
            ..ModelState::default()
        };

        let mut errors = vec![];
        _ = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            add_intent(&mid),
        );

        assert!(errors.is_empty());
        assert_eq!(state.movements.len(), 7);
        assert_eq!(state.movements[4], mid);
    }

    #[test]
    fn reducer_model_add_expense_contains_sorted() {
        let expenses: Vec<Expense> = Vec::new();

        let mut state = ModelState::default();
        let mut products = Vec::new();
        for expense in &expenses {
            products.push(reduce(
                &mut state,
                &contemporary_ref_date(),
                &mut vec![],
                add_intent(&expense),
            ));
        }

        assert!(
            state
                .movements
                .iter()
                .zip(state.movements.iter().skip(1))
                .all(|(current, next)| current.date <= next.date)
        );
    }

    #[test]
    fn reducer_model_edit_expense_does_not_exist_should_push_error() {
        let mut state = ModelState::default();
        let mut errors = vec![];
        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Update(Expense::default()),
        );

        assert!(products.flags.is_empty());
        assert!(products.cmds.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0].error_type,
            MoniErrorType::Domain(MoniDomainError::ExpenseNotFound(id))
                if *id == u64::from(Expense::default().id)
        ));
    }

    #[test]
    fn reducer_model_edit_expense_same_fields_no_products() {
        let expense = expense_in_ref_month();
        let mut state = ModelState {
            movements: VersionedArc::from(vec![expense.clone()]),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Update(expense.clone()),
        );

        assert!(products.flags.is_empty());
        assert!(products.cmds.is_empty());
        assert!(errors.is_empty());
        assert_eq!(*state.movements, vec![expense]);
    }

    #[test]
    fn reducer_model_edit_expense_fields_updated_products_and_delayed_save() {
        let original = expense_in_ref_month();
        let updated = Expense {
            amount: original.amount + 10000,
            comment: Some("updated".to_string()),
            ..original.clone()
        };
        let mut state = ModelState {
            movements: VersionedArc::from(vec![original.clone()]),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Update(updated.clone()),
        );

        assert_eq!(products.flags, EnumSet::only(Dirty::FinancesCurrentMonth));
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );
        assert!(errors.is_empty());
        assert_eq!(*state.movements, vec![updated]);
    }

    #[rstest]
    #[case::back(2.months(), [0, 2, 1])]
    #[case::forward(-2.months(), [1, 0, 2])]
    #[case::same(0.months(), [0, 1, 2])]
    #[case::collides_last_and_wins(1.month(), [0, 2, 1])]
    #[case::collides_first_and_loses(-1.month(), [0, 1, 2])]
    fn reducer_model_edit_expense_move_dates_ordering(
        #[case] offset: Span,
        #[case] expected_ordering: [usize; 3],
    ) {
        let original: Vec<_> = expenses_list(contemporary_ref_date(), 1.month())
            .into_iter()
            .skip(3)
            .collect();
        let mut updated_element = original.get(1).unwrap().clone();
        updated_element.date += offset;
        updated_element.amount = -1; // force update when date is same as original.
        let mut expected_result = original.clone();
        expected_result[1] = updated_element.clone();
        let expected_result = ordered_by_index_map(expected_result, expected_ordering);
        let mut state = ModelState {
            movements: VersionedArc::from(original),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Update(updated_element.clone()),
        );

        assert_eq!(*state.movements, expected_result);
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );
        assert!(errors.is_empty());
    }

    #[rstest]
    #[case::before_to_before(1, -1.month(), EnumSet::only(Dirty::FinancesBeforeThisMonth))]
    #[case::before_stays(1, 0.months(), EnumSet::only(Dirty::FinancesBeforeThisMonth))]
    #[case::before_to_current(1, 1.month(), Dirty::FinancesBeforeThisMonth | Dirty::FinancesCurrentMonth)]
    #[case::current_to_before(2, -1.month(), Dirty::FinancesBeforeThisMonth | Dirty::FinancesCurrentMonth)]
    #[case::current_stays(2, 0.months(), EnumSet::only(Dirty::FinancesCurrentMonth))]
    #[case::current_to_same_month_previous_year(2, -12.months(), Dirty::FinancesBeforeThisMonth | Dirty::FinancesCurrentMonth)]
    fn reducer_model_edit_expense_move_dates_dirty_flags(
        #[case] index: usize,
        #[case] offset: Span,
        #[case] expected_dirty: EnumSet<Dirty>,
    ) {
        let original: Vec<_> = expenses_list(contemporary_ref_date(), 1.month())
            .into_iter()
            .skip(3)
            .collect();
        let mut updated_element = original.get(index).unwrap().clone();
        updated_element.date += offset;
        updated_element.amount = -1; // force update when date is same as original.
        let mut state = ModelState {
            movements: VersionedArc::from(original),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Update(updated_element.clone()),
        );

        assert_eq!(products.flags, expected_dirty);
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn reducer_model_edit_expense_same_month_previous_year_flags_before() {
        let original = Expense {
            date: &contemporary_ref_date() - 12.months(),
            ..Expense::default()
        };
        let updated = Expense {
            amount: original.amount + 10000,
            ..original.clone()
        };
        let mut state = ModelState {
            movements: VersionedArc::from(vec![original]),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Update(updated.clone()),
        );

        assert_eq!(
            products.flags,
            EnumSet::only(Dirty::FinancesBeforeThisMonth)
        );
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );
        assert!(errors.is_empty());
        assert_eq!(*state.movements, vec![updated]);
    }

    #[rstest]
    #[case::month_before(
        expense_just_before_ref_month(),
        EnumSet::only(Dirty::FinancesBeforeThisMonth)
    )]
    #[case::current_month(expense_in_ref_month(), EnumSet::only(Dirty::FinancesCurrentMonth))]
    fn reducer_model_delete_expense(#[case] expense: Expense, #[case] dirty: EnumSet<Dirty>) {
        let mut state = ModelState {
            movements: VersionedArc::from(vec![expense.clone()]),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Delete(expense.id),
        );

        assert_eq!(products.flags, dirty);
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );
        assert!(errors.is_empty());
        assert!(state.movements.is_empty());
    }

    #[test]
    fn reducer_model_delete_expense_does_not_exist_should_push_error() {
        let mut state = ModelState::default();
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Delete(ExpenseId::from(99)),
        );

        assert!(products.flags.is_empty());
        assert!(products.cmds.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0].error_type,
            MoniErrorType::Domain(MoniDomainError::ExpenseNotFound(id))
                if *id == 99
        ));
    }

    #[rstest]
    #[case::first(0)]
    #[case::mid(3)]
    #[case::last(5)]
    fn reducer_model_delete_expense_preserves_order(#[case] index: usize) {
        let expenses = expenses_list(contemporary_ref_date(), 1.month());
        let removed = expenses[index].clone();
        let mut expected = expenses.clone();
        expected.remove(index);
        let mut state = ModelState {
            movements: VersionedArc::from(expenses),
            ..ModelState::default()
        };
        let mut errors = vec![];

        let products = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Delete(removed.id),
        );

        assert!(errors.is_empty());
        assert_eq!(*state.movements, expected);
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Debounce(DelayedSave(Bump))))]
        );
    }

    #[test]
    fn reducer_model_add_every_interval_should_register_task_and_subscribes_timer() {
        let mut state = ModelState::default();
        let mut errors = vec![];
        let mut tasks = LongLivingTasks::default();

        let products = reduce_with_tasks(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            &mut tasks,
            add_every_interval(ref_uuid()),
        );

        assert!(products.flags.is_empty());
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Time(TimeSubscriptionCmd::EveryXInterval(
                ref_uuid(),
                ref_interval(),
                Save
            ))))]
        );
        assert!(errors.is_empty());
        assert!(tasks.recurrent_add.contains(&ref_uuid()));
    }

    #[test]
    fn reducer_model_add_every_interval_distinct_ids_should_register_independently() {
        let mut state = ModelState::default();
        let mut errors = vec![];
        let mut tasks = LongLivingTasks::default();

        for id in [ref_uuid(), alternative_ref_uuid()] {
            reduce_with_tasks(
                &mut state,
                &contemporary_ref_date(),
                &mut errors,
                &mut tasks,
                add_every_interval(id),
            );
        }

        assert!(errors.is_empty());
        assert_eq!(tasks.recurrent_add.len(), 2);
        assert!(tasks.recurrent_add.contains(&ref_uuid()));
        assert!(tasks.recurrent_add.contains(&alternative_ref_uuid()));
    }

    #[test]
    fn reducer_model_stop_adding_every_interval_should_unregister_task_and_cancel_timer() {
        let mut state = ModelState::default();
        let mut errors = vec![];
        let mut tasks = LongLivingTasks::default();
        tasks.recurrent_add.insert(ref_uuid());

        let products = reduce_with_tasks(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            &mut tasks,
            ModelAction::StopAddingEveryXInterval(ref_uuid()),
        );

        assert!(products.flags.is_empty());
        assert_eq!(
            products.cmds,
            vec![Env(Subscribe(Time(
                TimeSubscriptionCmd::CancelEveryXInterval(ref_uuid())
            )))]
        );
        assert!(errors.is_empty());
        assert!(tasks.recurrent_add.is_empty());
    }

    #[test]
    fn reducer_model_stop_adding_every_interval_unknown_id_should_generate_no_products() {
        let mut state = ModelState::default();
        let mut errors = vec![];
        let mut tasks = LongLivingTasks::default();
        tasks.recurrent_add.insert(ref_uuid());

        let products = reduce_with_tasks(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            &mut tasks,
            ModelAction::StopAddingEveryXInterval(alternative_ref_uuid()),
        );

        assert!(products.flags.is_empty());
        assert!(products.cmds.is_empty());
        assert!(errors.is_empty());
        assert!(tasks.recurrent_add.contains(&ref_uuid()));
    }

    #[test]
    fn reducer_model_stop_adding_every_interval_twice_should_cancel_only_once() {
        let mut state = ModelState::default();
        let mut errors = vec![];
        let mut tasks = LongLivingTasks::default();

        reduce_with_tasks(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            &mut tasks,
            add_every_interval(ref_uuid()),
        );

        let first_stop = reduce_with_tasks(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            &mut tasks,
            ModelAction::StopAddingEveryXInterval(ref_uuid()),
        );
        let second_stop = reduce_with_tasks(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            &mut tasks,
            ModelAction::StopAddingEveryXInterval(ref_uuid()),
        );

        assert_eq!(
            first_stop.cmds,
            vec![Env(Subscribe(Time(
                TimeSubscriptionCmd::CancelEveryXInterval(ref_uuid())
            )))]
        );
        assert!(second_stop.cmds.is_empty());
        assert!(errors.is_empty());
        assert!(tasks.recurrent_add.is_empty());
    }
}
