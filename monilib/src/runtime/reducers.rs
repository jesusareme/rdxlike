use super::{cmd::*, *};
use crate::runtime::modelviews::ClockedModelStateView;
use crate::{
    action::{Action, WorkingAction},
    runtime::{State::Zero, cmd::DebounceAction::Cancel},
};
use std::mem;
use crate::runtime::Dirty::Statistics;

pub fn reducer(state: &mut State, action: Action) -> Products {
    use Action::*;
    match action {
        Init => Products::cmd(PersistenceCmd::CreateOrOpenFile),

        InitResult(result) => {
            let model = match result {
                Ok(None) => ModelState::default(),
                Ok(Some(content)) => match serde_json::from_str::<ModelState>(&content) {
                    Ok(model) => model,
                    Err(error) => return failed_init(error),
                },
                Err(error) => return failed_init(error),
            };

            let working = State::Working(WorkingState {
                model,
                running: RunningState::default(),
            });

            match mem::replace(state, working) {
                Zero(pending) => Products::cmd(Direct(pending)),
                _ => Products::none().with_dirty(EnumSet::all()),
            }
        }
        NoOp => Products::none(),

        Working(action) => match state {
            Zero(pending) => {
                pending.push(action);
                Products::none()
            }
            State::Working(working) => reducer_working(working, action),
        },
    }
}

fn failed_init(error: impl std::fmt::Debug) -> Products {
    panic!("MoniLib was unable to initialize\n{:?}", error)
}

fn reducer_working(state: &mut WorkingState, action: WorkingAction) -> Products {
    use DebounceCmd::*;
    use WorkingAction::*;
    match action {
        Save => Products::cmds(vec![
            PersistenceCmd::Save(state.model.clone()).into(),
            DelayedSave(Cancel).into(),
        ]),
        SuccessfulSave => {
            debug!("Successful SAVE!");
            Products::none()
        }

        Watchdog => Products::cmd(TimeSubscriptionCmd::Watchdog),

        Model(action) => reducer_model(state.model_view(), action),
        Running(action) => reducer_running(&mut state.running, action),

        WatchdogWatching => {
            debug!("watchdog watching!");
            Products::none()
        } // Action::DelayedSave => {
          //
          // }

          // Action::AddToInfo(text) => Products::none(),
          // Action::AddFromLongCalculation => {
          // 	let counter = state.counter.clone(); // we can move values for later use
          // 	let cmd = Cmd::BasicService(BasicServiceCmd::DoLongCalculation { counter });
          // 	Products::cmd(cmd).with_dirty(Dirty::AllViews)
          // }
          // Action::AddEverySecond(interval) => {
          // 	let operation = DropCancellation::new(Uuid::new_v4());
          // 	let handle = operation.cancellation_handle();
          // 	state.counting = Some(operation);
          //
          // 	let cmd = Cmd::Subscription(Subscription::Time(TimeSubscriptionCmd::EveryXSeconds {
          // 		interval,
          // 		handle,
          // 	}));
          // 	Products::cmd(cmd)
          // }
          // Action::AddFromAsync => {
          // 	let counter = state.counter;
          // 	let cmd = Cmd::Async(Box::new(move || {
          // 		thread::sleep(Duration::from_secs(1));
          // 		Action::Add(counter * 2)
          // 	}));
          // 	Products::cmd(cmd)
          // }
    }
}

fn reducer_running(state: &mut RunningState, action: RunningAction) -> Products {
    match action {
        RunningAction::ListViewHint(token, id) => {
            if let Some(view_state) = state.plain_list.get_mut(&token) {
                view_state.hint = Some(id);
            }
            Products::none().with_dirty_flag(Dirty::Views)
        }
        RunningAction::ListViewPrepare(token) => {
            state
                .plain_list
                .insert(token, PlainListViewState { hint: None });

            Products::none().with_dirty_flag(Dirty::Views)
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

fn reducer_model(state: ClockedModelStateView, action: ModelAction) -> Products {
    match action {
        ModelAction::Add(expense) => {
            let first_of_month = state
                .time
                .first_of_month()
                .expect("first_of_month from an injected Zoned cannot fail");

            let dirty = finances_dirty(&expense.date, &first_of_month);

            let idx = state
                .model_state
                .movements
                .partition_point(|e| e.date <= expense.date);
            state.model_state.movements.update_with(|expenses| {
                expenses.insert(idx, expense);
            });

            Products::none().with_dirty_flag(dirty).with_delayed_save()
        }
        
        ModelAction::Update(updated_expense) => {
            match state
                .model_state
                .movements
                .iter()
                .position(|current| current.id == updated_expense.id)
            {
                None => {
                    state
                        .errors
                        .push(MoniDomainError::ExpenseNotFound(updated_expense.id.into()).into());
                    Products::none()
                }
                Some(idx) => {
                    let previous = state
                        .model_state
                        .movements
                        .get(idx)
                        .expect("element should exist");

                    if *previous == updated_expense {
                        return Products::none();
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
                            let updated_idx =
                                expenses.partition_point(|e| e.date <= updated_expense.date);
                            expenses.insert(updated_idx, updated_expense);
                        }

                        Products::none()
                            .with_dirty(previous_dirty | updated_dirty)
                            .with_delayed_save()
                    })
                }
            }
        }
        
        ModelAction::Delete(id) => {
            match state
                .model_state
                .movements
                .iter()
                .position(|current| current.id == id)
            {
                None => {
                    state
                        .errors
                        .push(MoniDomainError::ExpenseNotFound(id.into()).into());
                    Products::none()
                }
                Some(idx) => {
                    let first_of_month = state
                        .time
                        .first_of_month()
                        .expect("first_of_month should fail here");

                    let removed = state
                        .model_state
                        .movements
                        .update_with(|expenses| expenses.remove(idx));

                    let dirty = finances_dirty(&removed.date, &first_of_month);

                    Products::none().with_dirty_flag(dirty).with_delayed_save()
                }
            }
        },

        ModelAction::StatisticsAll => {
            match &mut state.model_state.statistics_all {
                Some(s) if s.at_movements_version == state.model_state.movements.version()
                => {
                    s.requested_at = state.time.timestamp();
                    Products::none().with_dirty_flag(Statistics)
                },
                _ => Products::cmd(AsyncCmd::StatisticsCalculation(state.model_state.movements.clone(),
                                                                   state.time.timestamp(),
                )),
            }
        },

        ModelAction::StatisticsAllResult(statistics) => {
            if matches!(state.model_state.statistics_all, Some(s) if s.requested_at >= statistics.requested_at) {
                return Products::none()
            }

            state.model_state.statistics_all = Some(statistics);
            Products::none().with_dirty_flag(Statistics)
        }
    }
}

#[cfg(test)]
mod reducer_model_test {
    use super::*;
    use crate::MoniErrorType;
    use crate::testing::{contemporary_ref_date, ordered_by_index_map};
    use ExpenseCategory::*;
    use itertools::Itertools;
    use jiff::{Span, ToSpan};
    use proptest::prelude::*;
    use proptest::{prop_compose, proptest};
    use rstest::rstest;
    use std::str::FromStr;
    use std::time::{SystemTime};
    use uuid::Uuid;

    fn expense_in_ref_month() -> Expense {
        let past = contemporary_ref_date().first_of_month().unwrap();
        Expense {
            date: past,
            ..Expense::default()
        }
    }

    fn expense_just_before_ref_month() -> Expense {
        let past = contemporary_ref_date().first_of_month().unwrap() - 1.nanosecond();
        Expense {
            date: past,
            ..Expense::default()
        }
    }

    fn expenses_list(date_last: Zoned, step_back: Span) -> Vec<Expense> {
        let calculate_date = |back: i64| &date_last - step_back * back;
        vec![
            Expense {
                id: ExpenseId::from(
                    Uuid::from_str("01234567-0123-0123-0123-000000000001").unwrap(),
                ),
                date: calculate_date(5),
                amount: -1000,
                ..Expense::default()
            },
            Expense {
                id: ExpenseId::from(
                    Uuid::from_str("01234567-0123-0123-0123-000000000002").unwrap(),
                ),
                date: calculate_date(4),
                amount: 20000,
                category: Optional,
                comment: None,
            },
            Expense {
                id: ExpenseId::from(
                    Uuid::from_str("01234567-0123-0123-0123-000000000003").unwrap(),
                ),
                date: calculate_date(3),
                amount: 3400,
                category: Important,
                comment: Some("Pair of shoes".to_string()),
            },
            Expense {
                id: ExpenseId::from(
                    Uuid::from_str("01234567-0123-0123-0123-000000000004").unwrap(),
                ),
                date: calculate_date(2),
                ..Expense::default()
            },
            Expense {
                id: ExpenseId::from(
                    Uuid::from_str("01234567-0123-0123-0123-000000000005").unwrap(),
                ),
                date: calculate_date(1),
                amount: 100,
                ..Expense::default()
            },
            Expense {
                id: ExpenseId::from(
                    Uuid::from_str("01234567-0123-0123-0123-000000000006").unwrap(),
                ),
                date: calculate_date(0),
                category: ExpenseCategory::Essential,
                ..Expense::default()
            },
        ]
    }

    fn reduce(
        state: &mut ModelState,
        time: &Zoned,
        errors: &mut Vec<MoniError>,
        action: ModelAction,
    ) -> Products {
        reducer_model(
            ClockedModelStateView {
                model_state: state,
                time,
                errors,
            },
            action,
        )
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
                ExpenseId::from(Uuid::now_v7()),
                Zoned::try_from(date).unwrap(),
                amount,
                comment,
                category,
            )
        }
    }

    prop_compose! {
        fn arb_expenses_and_date_offsets()
        (expenses in proptest::collection::vec(arb_expenses(), 1..=500))
        (new_dates in proptest::collection::vec(any::<SystemTime>(), expenses.len()), expenses in Just(expenses)) -> (Vec<Expense>, Vec<SystemTime>) {
            (expenses, new_dates)
        }
    }

    proptest! {
        #[test]
        fn expenses_add_proptest((mut expenses, new_dates) in arb_expenses_and_date_offsets()) {
            prop_assume!(expenses.len() == new_dates.len(), "Test configuration issue: not enough durations");

            let uuids: Vec<_> = expenses.iter().map(|e| e.id).collect();

            let mut state = ModelState::default();
            let mut errors = vec![];
            let mut products = Products::none();
            expenses.clone().into_iter().for_each(|e| {
                products += reduce(
                    &mut state,
                    &contemporary_ref_date(),
                    &mut errors,
                    ModelAction::Add(e),
                );
                assert!(state.movements.is_sorted_by_key(|e| e.date.clone()));
            });

            assert!(errors.is_empty());
            assert_eq!(state.movements.len(), expenses.len());

            let saves: Vec<_> = products.cmds.iter().filter(|cmd| {
                matches!(cmd, Subscribe(Debounce(DelayedSave(Bump))))
            }).collect();
            assert_eq!(saves.len(), expenses.len());

            products = Products::none();

            for (e, d) in expenses.iter_mut().zip(new_dates) {
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
                matches!(cmd, Subscribe(Debounce(DelayedSave(Bump))))
            }).collect();
            assert_eq!(saves.len(), expenses.len());
            assert!(errors.is_empty());

            uuids.into_iter().for_each(|id| {
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
            ModelAction::Add(expense.clone()),
        );

        match products.cmds.first().unwrap() {
            Direct(_) => {}
            Queue(_) => {}
            Async(_) => {}
            Persistence(_) => {}
            Subscribe(_) => {}
        }

        assert_eq!(products.dirty, dirty);
        assert_eq!(
            products.cmds,
            vec![Cmd::Subscribe(Debounce(DelayedSave(Bump)))]
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

            for expense in expenses_perm {
                _ = reduce(
                    &mut state,
                    &contemporary_ref_date(),
                    &mut errors,
                    ModelAction::Add(expense.clone()),
                );
            }

            assert!(errors.is_empty());
            assert_eq!(*state.movements, original);
        }
    }

    #[test]
    fn reducer_model_add_expense_same_date_inserts_newest_last() {
        let shared = contemporary_ref_date().first_of_month().unwrap();
        let first = Expense {
            id: ExpenseId::from(Uuid::from_str("01234567-0123-0123-0123-000000000001").unwrap()),
            date: shared.clone(),
            amount: 1,
            ..Expense::default()
        };
        let second = Expense {
            id: ExpenseId::from(Uuid::from_str("01234567-0123-0123-0123-000000000002").unwrap()),
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
            ModelAction::Add(first.clone()),
        );
        _ = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Add(second.clone()),
        );

        assert!(errors.is_empty());
        assert_eq!(*state.movements, vec![first, second]);
    }

    #[test]
    fn reducer_model_add_expense_into_mid_list() {
        let expenses = expenses_list(contemporary_ref_date(), 1.month());
        let mid = Expense {
            date: &expenses[3].date + 1.nanosecond(),
            ..Expense::default()
        };
        let mut state = ModelState {
            movements: VersionedArc::from(expenses),
            ..ModelState::default()
        };

        let mut errors = vec![];
        _ = reduce(
            &mut state,
            &contemporary_ref_date(),
            &mut errors,
            ModelAction::Add(mid.clone()),
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
                ModelAction::Add(expense.clone()),
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

        assert!(products.dirty.is_empty());
        assert!(products.cmds.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0].error_type,
            MoniErrorType::Domain(MoniDomainError::ExpenseNotFound(id))
                if *id == Uuid::from(Expense::default().id)
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

        assert!(products.dirty.is_empty());
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

        assert_eq!(products.dirty, EnumSet::only(Dirty::FinancesCurrentMonth));
        assert_eq!(
            products.cmds,
            vec![Subscribe(Debounce(DelayedSave(Bump)))]
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
            vec![Subscribe(Debounce(DelayedSave(Bump)))]
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

        assert_eq!(products.dirty, expected_dirty);
        assert_eq!(
            products.cmds,
            vec![Cmd::Subscribe(Debounce(DelayedSave(Bump)))]
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
            products.dirty,
            EnumSet::only(Dirty::FinancesBeforeThisMonth)
        );
        assert_eq!(
            products.cmds,
            vec![Subscribe(Debounce(DelayedSave(Bump)))]
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

        assert_eq!(products.dirty, dirty);
        assert_eq!(
            products.cmds,
            vec![Cmd::Subscribe(Debounce(DelayedSave(Bump)))]
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
            ModelAction::Delete(ExpenseId::from(
                Uuid::from_str("01234567-0123-0123-0123-000000000099").unwrap(),
            )),
        );

        assert!(products.dirty.is_empty());
        assert!(products.cmds.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0].error_type,
            MoniErrorType::Domain(MoniDomainError::ExpenseNotFound(id))
                if *id == Uuid::from_str("01234567-0123-0123-0123-000000000099").unwrap()
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
            vec![Cmd::Subscribe(Debounce(DelayedSave(Bump)))]
        );
    }
}
