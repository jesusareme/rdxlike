use super::{
    AppState, Dirty, Expense, MoniLibClient, PlainListViewState, State, Statistics, VersionedArc,
};
use crate::inout::{LibOutput, MoniStatistics};
use crate::util::ExpenseId;
use crate::{MoniError, MoniExpensePlainListSnapshot};
use enumset::EnumSet;
use jiff::Timestamp;
use rdxlib::error::RuntimeError;
use rdxlib::subscribers::ComparableResult::{self, Comparable, NothingToCompare};
use rdxlib::subscribers::{OutputSubscriber, Subscriber, SubscriberError, ViewId, ViewTransformer};
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Sub;
use tracing::error;
use uuid::Uuid;

pub fn plain_list_view_subscriber(
    id: ViewId,
    out: LibOutput<MoniExpensePlainListSnapshot>,
) -> Result<impl Subscriber<Client = MoniLibClient>, RuntimeError> {
    OutputSubscriber::new(id, PlainListTransformer::new(), out)
}

pub fn errors_subscriber(
    out: LibOutput<Vec<MoniError>>,
) -> Result<impl Subscriber<Client = MoniLibClient>, RuntimeError> {
    OutputSubscriber::new(Uuid::new_v4().into(), ErrorsViewTransformer::default(), out)
}

#[derive(Default)]
struct ErrorsViewTransformer {
    last_consumed_id: Option<Uuid>,
}

impl ViewTransformer<MoniLibClient> for ErrorsViewTransformer {
    type ComparableValue = Uuid;
    type Slice = Vec<MoniError>;
    type Product = Vec<MoniError>;

    fn interested_in(_: &EnumSet<Dirty>) -> bool {
        true
    }

    fn comparable(state: &State, _token: ViewId) -> ComparableResult<Self::ComparableValue> {
        state
            .running
            .errors
            .last()
            .map_or_else(|| NothingToCompare, |last| Comparable(last.id))
    }

    fn slice(state: &State, _token: ViewId) -> Result<Self::Slice, SubscriberError> {
        Ok(state.running.errors.clone())
    }

    fn derive(&mut self, slice: Self::Slice) -> Option<Self::Product> {
        let last = slice.last()?.id;
        let mut new_errors = vec![];
        if let Some(last_processed_id) = self.last_consumed_id {
            new_errors.extend(slice.into_iter().rev().map_while(|error| {
                if error.id == last_processed_id {
                    None
                } else {
                    Some(error)
                }
            }));
        } else {
            new_errors = slice;
        }

        if new_errors.is_empty() {
            None
        } else {
            self.last_consumed_id = Some(last);
            Some(new_errors)
        }
    }
}

struct PlainListStateSlice {
    expenses: VersionedArc<Vec<Expense>>,
    view_state: PlainListViewState,
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct PlainListComparable {
    movements_version: u64,
    hint: Option<ExpenseId>,
}

struct PlainListTransformer {
    currently_displayed: HashMap<ExpenseId, Expense>,
    prev_ids: Option<HashSet<ExpenseId>>,
    latest_hint: Option<ExpenseId>,
    page_size: usize,
    mid_page_offset: usize,
}

impl PlainListTransformer {
    const PAGE_SIZE: usize = 60;
    const MID_PAGE_OFFSET: usize = 20;

    pub fn new() -> Self {
        PlainListTransformer {
            currently_displayed: HashMap::new(),
            prev_ids: None,
            latest_hint: None,
            page_size: Self::PAGE_SIZE,
            mid_page_offset: Self::MID_PAGE_OFFSET,
        }
    }
}

struct StatisticsTransformer;

impl ViewTransformer<MoniLibClient> for StatisticsTransformer {
    type ComparableValue = Timestamp;
    type Slice = Statistics;
    type Product = MoniStatistics;

    fn interested_in(offered: &EnumSet<Dirty>) -> bool {
        offered.contains(Dirty::Statistics)
    }

    fn comparable(state: &State, _token: ViewId) -> ComparableResult<Self::ComparableValue> {
        if let AppState::Working(working) = &state.app {
            // This subscription depends entirely on this pre-calculated value being present
            let Some(current) = working.statistics_all else {
                return NothingToCompare;
            };
            Comparable(current.requested_at)
        } else {
            NothingToCompare
        }
    }

    fn slice(state: &State, _token: ViewId) -> Result<Self::Slice, SubscriberError> {
        let AppState::Working(model) = &state.app else {
            return Err(SubscriberError::MissingState);
        };

        let Some(statistics) = model.statistics_all else {
            return Err(SubscriberError::MissingState);
        };

        Ok(statistics)
    }

    fn derive(&mut self, slice: Self::Slice) -> Option<Self::Product> {
        Some(MoniStatistics {
            date: slice.requested_at.into(),
            len: slice.items_len,
            sum: slice.results.map(|s| s.sum),
            min: slice.results.map(|s| s.min_expense),
            max: slice.results.map(|s| s.max_expense),
        })
    }
}

pub fn statistics_subscriber(
    out: LibOutput<MoniStatistics>,
) -> Result<impl Subscriber<Client = MoniLibClient>, RuntimeError> {
    OutputSubscriber::new(Uuid::new_v4().into(), StatisticsTransformer, out)
}

impl ViewTransformer<MoniLibClient> for PlainListTransformer {
    type ComparableValue = PlainListComparable;
    type Slice = PlainListStateSlice;
    type Product = MoniExpensePlainListSnapshot;

    fn interested_in(offered: &EnumSet<Dirty>) -> bool {
        use Dirty::{FinancesBeforeThisMonth, FinancesCurrentMonth, Views};
        !offered
            .intersection(FinancesCurrentMonth | FinancesBeforeThisMonth | Views)
            .is_empty()
    }

    fn comparable(state: &State, id: ViewId) -> ComparableResult<Self::ComparableValue> {
        let AppState::Working(model) = &state.app else {
            return NothingToCompare;
        };

        state.running.plain_list.get(&id).map_or_else(
            || NothingToCompare,
            |v| {
                Comparable(PlainListComparable {
                    movements_version: model.movements.version(),
                    hint: v.hint,
                })
            },
        )
    }

    fn slice(state: &State, id: ViewId) -> Result<Self::Slice, SubscriberError> {
        let AppState::Working(model) = &state.app else {
            return Err(SubscriberError::MissingState);
        };

        let view_state = state
            .running
            .plain_list
            .get(&id)
            .ok_or(SubscriberError::MissingState)?;
        Ok(PlainListStateSlice {
            expenses: model.movements.clone(),
            view_state: *view_state,
        })
    }

    fn derive(&mut self, slice: PlainListStateSlice) -> Option<MoniExpensePlainListSnapshot> {
        let ids: Vec<u64> = slice.expenses.iter().rev().map(|e| e.id.into()).collect();
        let current_ids: HashSet<ExpenseId> = slice.expenses.iter().map(|e| e.id).collect();

        let updated = if self.currently_displayed.is_empty()
            && !slice.expenses.is_empty()
            && slice.view_state.hint.is_none()
        {
            // first execution
            let mut updated = Vec::new();
            for expense in slice.expenses.iter().rev().take(self.page_size) {
                self.currently_displayed.insert(expense.id, expense.clone());
                updated.push(expense.clone().into());
            }
            updated
        } else {
            let mut updated = Vec::new();
            let mut page_ids = HashSet::new();

            if let Some(hint) = slice.view_state.hint
                && self.latest_hint != Some(hint)
            {
                // New state comes with a new hint
                self.latest_hint = Some(hint);

                let Some(idx) = slice.expenses.iter().rposition(|e| e.id == hint) else {
                    error!("Hinted at id {hint:?} which doesn't exist");
                    return None;
                };

                let expense_len = slice.expenses.len();
                let end = idx.saturating_add(1).saturating_add(self.mid_page_offset);
                let end = min(expense_len, end);
                let start = end.saturating_sub(self.page_size);

                for expense in &slice.expenses[start..end] {
                    page_ids.insert(expense.id);
                    if self.currently_displayed.get(&expense.id) != Some(expense) {
                        self.currently_displayed.insert(expense.id, expense.clone());
                        updated.push(expense.clone().into());
                    }
                }
            }

            //Detect those items no longer present and remove them
            let displayed_ids: HashSet<ExpenseId> =
                self.currently_displayed.keys().copied().collect();
            let mut to_check = displayed_ids.sub(&page_ids);
            let removed_ids: Vec<_> = to_check
                .extract_if(|id| !current_ids.contains(id))
                .collect();

            // Detect changed items, save them to cache and send them as updated
            for id in to_check {
                let Some(current) = self.currently_displayed.get(&id) else {
                    error!("Expected currently displayed item missing: {id}");
                    continue;
                };
                let Some(recent) = slice.expenses.iter().rfind(|e| e.id == id) else {
                    error!("Expected new item in state but item missing: {id}");
                    continue;
                };
                if current != recent {
                    self.currently_displayed.insert(id, recent.clone());
                    updated.push(recent.clone().into());
                }
            }

            // removing deleted items from cache
            for id in &removed_ids {
                self.currently_displayed.remove(id);
            }

            // check new items, cache them and send them as updated
            if let Some(prev_ids) = &self.prev_ids {
                for expense in slice.expenses.iter() {
                    if !prev_ids.contains(&expense.id)
                        && !self.currently_displayed.contains_key(&expense.id)
                    {
                        self.currently_displayed.insert(expense.id, expense.clone());
                        updated.push(expense.clone().into());
                    }
                }
            }

            updated
        };

        let ids_changed = self.prev_ids.as_ref() != Some(&current_ids);
        self.prev_ids = Some(current_ids);

        if !ids_changed && updated.is_empty() {
            None
        } else {
            Some(MoniExpensePlainListSnapshot { ids, updated })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inout::PlainListItem;
    use crate::runtime::ModelState;
    use crate::testing::{contemporary_ref_date, distant_past_ref_date, ordered_expenses};
    use jiff::ToSpan;
    use rstest::{fixture, rstest};
    use std::cmp::{Ordering, min};

    const PAGE_SIZE: usize = 4;
    const OFFSET: usize = 1;

    impl Default for PlainListTransformer {
        fn default() -> Self {
            PlainListTransformer {
                currently_displayed: HashMap::new(),
                prev_ids: None,
                latest_hint: None,
                page_size: PAGE_SIZE,
                mid_page_offset: OFFSET,
            }
        }
    }

    fn date_ordering(a: &PlainListItem, b: &PlainListItem) -> Ordering {
        a.date.cmp(&b.date)
    }

    fn assert_prev_ids(transformer: &PlainListTransformer, expenses: &[Expense]) {
        assert_eq!(
            transformer.prev_ids,
            Some(expenses.iter().map(|e| e.id).collect())
        );
    }

    #[fixture]
    fn expenses() -> [Expense; 20] {
        ordered_expenses(contemporary_ref_date())
    }

    #[fixture]
    fn ids(expenses: [Expense; 20]) -> Vec<u64> {
        expenses.iter().rev().map(|e| e.id.into()).collect()
    }

    #[fixture]
    fn filled_transformer(expenses: [Expense; 20]) -> PlainListTransformer {
        let mut transformer = PlainListTransformer::default();
        transformer
            .currently_displayed
            .extend(expenses.iter().cloned().map(|e| (e.id, e)));
        transformer.prev_ids = Some(expenses.iter().map(|e| e.id).collect());
        transformer
    }

    #[rstest]
    fn p_list_initial_derive_should_create_initial_view(mut expenses: [Expense; 20], ids: Vec<u64>) {
        let mut transformer = PlainListTransformer::default();
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint: None },
        };
        expenses.reverse();
        let page_items: Vec<PlainListItem> =
            expenses.iter().take(4).cloned().map(Into::into).collect();

        let product = transformer.derive(state).expect("Should be product");

        assert_eq!(product.ids, ids);
        assert_eq!(product.updated, page_items);
        assert_eq!(transformer.latest_hint, None);
        for expense in expenses.iter().take(4) {
            assert_eq!(
                transformer.currently_displayed.get(&expense.id),
                Some(expense)
            )
        }
        assert_eq!(transformer.currently_displayed.len(), 4);
        assert_prev_ids(&transformer, &expenses);
    }

    #[rstest]
    #[case::start(19)]
    #[case::mid(10)]
    #[case::latest_elements(3)]
    #[case::last_two(0)]
    fn p_list_derive_hint_not_overlapping_should_send_complete_page(
        expenses: [Expense; 20],
        ids: Vec<u64>,
        #[case] hint_pos: usize,
    ) {
        let mut transformer = PlainListTransformer::default();
        let latest_hint = Some(expenses[hint_pos].id);
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint: latest_hint },
        };

        let end = min(expenses.len(), hint_pos + 1 + transformer.mid_page_offset);
        let start = end.saturating_sub(transformer.page_size);
        let page = expenses[start..end].to_vec();
        let mut page_items = page
            .iter()
            .cloned()
            .map(Into::into)
            .collect::<Vec<PlainListItem>>();

        let mut product = transformer.derive(state).expect("Should be product");

        assert_eq!(product.ids, ids);
        product.updated.sort_by(date_ordering);
        page_items.sort_by(date_ordering);
        assert_eq!(product.updated, page_items);

        for expense in &page {
            assert_eq!(
                transformer.currently_displayed.get(&expense.id),
                Some(expense)
            )
        }

        assert_eq!(transformer.currently_displayed.len(), page.len());
        assert_eq!(transformer.latest_hint, latest_hint);
        assert_prev_ids(&transformer, &expenses);
    }

    #[rstest]
    #[case::start_overlap1(19, 17, vec![18, 19])]
    #[case::start_overlap2(17, 19, vec![15])]
    #[case::start_overlap3(19, 19, vec![])]
    #[case::mid_overlap1(12, 10, vec![13, 12, 11])]
    #[case::mid_overlap2(10, 12, vec![8])]
    #[case::mid_overlap3(10, 10, vec![11])]
    #[case::end_overlap1(2, 0, vec![3, 2, 1])]
    #[case::end_overlap2(0, 2, vec![])]
    #[case::end_overlap3(0, 0, vec![1])]
    fn p_list_derive_hint_overlapping_should_send_partial_page(
        expenses: [Expense; 20],
        ids: Vec<u64>,
        #[case] hint_pos: usize,
        #[case] already_there_pos: usize,
        #[case] expected_updated_indexes: Vec<usize>,
    ) {
        let mut transformer = PlainListTransformer::default();
        let latest_hint = Some(expenses[hint_pos].id);
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint: latest_hint },
        };

        let end = min(expenses.len(), already_there_pos + 1);
        let start = end.saturating_sub(transformer.page_size);
        let already_loaded = expenses[start..end].iter().cloned().map(|e| (e.id, e));
        transformer.currently_displayed.extend(already_loaded);

        let mut updated: Vec<PlainListItem> = expected_updated_indexes
            .iter()
            .map(|idx| expenses[*idx].clone().into())
            .collect();

        let mut product = transformer.derive(state).expect("Should be product");

        assert_eq!(product.ids, ids);
        product.updated.sort_by(date_ordering);
        updated.sort_by(date_ordering);
        assert_eq!(product.updated, updated);

        for id in updated.iter().map(|item| item.id) {
            assert!(
                transformer
                    .currently_displayed
                    .contains_key(&ExpenseId::from(id))
            );
        }
        assert_prev_ids(&transformer, &expenses);
    }

    #[rstest]
    #[case::any(10, 0, 1)]
    #[case::same_as_hint(10, 10, 10)]
    #[case::same_as_new_hint(3, 3, 0)]
    #[case::same_as_old_hint(0, 3, 3)]
    fn p_list_changed_displayed_element_should_send_independently_of_hint(
        mut expenses: [Expense; 20],
        ids: Vec<u64>,
        mut filled_transformer: PlainListTransformer,
        #[case] changed: usize,
        #[case] recv_hint: usize,
        #[case] prev_hint: usize,
    ) {
        let hint = Some(expenses[recv_hint].id);
        let prev_hint = Some(expenses[prev_hint].id);
        filled_transformer.latest_hint = prev_hint;
        expenses[changed].amount = -1;
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint },
        };

        let product = filled_transformer.derive(state).expect("Should be product");

        assert_eq!(product.ids, ids);
        assert_eq!(product.updated.len(), 1);
        assert_eq!(product.updated[0].id, u64::from(expenses[changed].id));
        assert_eq!(product.updated[0].amount, -1);
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_derive_repeat_hint_no_data_changes_should_be_ignored(
        expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
    ) {
        let hint = Some(expenses[19].id);
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint },
        };
        filled_transformer.latest_hint = hint;

        assert_eq!(filled_transformer.derive(state), None);
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_derived_hint_represents_non_existing_id_should_ignore(
        expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
    ) {
        let hint = Some(ExpenseId::from(999));
        filled_transformer.prev_ids = None;
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint },
        };

        assert_eq!(filled_transformer.derive(state), None);
        assert_eq!(filled_transformer.prev_ids, None);
    }

    #[rstest]
    fn p_list_expenses_changed_order_should_update_element_and_cache(
        mut expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
    ) {
        expenses[10].date = distant_past_ref_date();
        let updated = expenses[10].clone();
        expenses.sort();
        let ids: Vec<u64> = expenses.iter().rev().map(|e| e.id.into()).collect();
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint: None },
        };

        let products = filled_transformer
            .derive(state)
            .expect("Should generate products");
        assert_eq!(products.ids, ids);
        assert_eq!(products.updated, vec![updated.clone().into()]);
        assert_eq!(filled_transformer.currently_displayed[&updated.id], updated);
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_added_expense_after_initial_view_should_only_send_new_item(expenses: [Expense; 20]) {
        let mut transformer = PlainListTransformer::default();
        let initial = PlainListStateSlice {
            expenses: VersionedArc::from(Vec::from(expenses.clone())),
            view_state: PlainListViewState { hint: None },
        };
        transformer.derive(initial).expect("Should be product");
        assert_prev_ids(&transformer, &expenses);

        let added = Expense::new_default_with(
            ExpenseId::from(100),
            &contemporary_ref_date() + 20.days(),
            Some(-2),
        );
        let mut expenses: Vec<_> = expenses.into();
        expenses.push(added.clone());
        let ids: Vec<u64> = expenses.iter().rev().map(|e| e.id.into()).collect();
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(expenses.clone()),
            view_state: PlainListViewState { hint: None },
        };

        let product = transformer.derive(state).expect("Should be product");

        assert_eq!(product.ids, ids);
        assert_eq!(product.updated, vec![added.clone().into()]);
        assert_eq!(transformer.currently_displayed.len(), PAGE_SIZE + 1);
        assert_eq!(transformer.currently_displayed[&added.id], added);
        assert_prev_ids(&transformer, &expenses);
    }

    #[rstest]
    #[case::newest(20)]
    #[case::mid(10)]
    #[case::oldest(0)]
    fn p_list_added_expense_should_send_data_and_update_cache(
        expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
        #[case] insert_at: usize,
    ) {
        let added = Expense::new_default_with(
            ExpenseId::from(100),
            &contemporary_ref_date() + (insert_at as i64).days() - 12.hours(),
            Some(-2),
        );
        let mut expenses: Vec<_> = expenses.into();
        expenses.insert(insert_at, added.clone());
        let ids: Vec<u64> = expenses.iter().rev().map(|e| e.id.into()).collect();
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(expenses.clone()),
            view_state: PlainListViewState { hint: None },
        };

        let product = filled_transformer.derive(state).expect("Should be product");

        assert_eq!(product.ids, ids);
        assert_eq!(product.updated, vec![added.clone().into()]);
        assert_eq!(filled_transformer.currently_displayed[&added.id], added);
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_added_expense_with_unchanged_hint_should_send_data(
        expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
    ) {
        let hint = Some(expenses[10].id);
        filled_transformer.latest_hint = hint;
        let added = Expense::new_default_with(
            ExpenseId::from(100),
            &contemporary_ref_date() + 20.days(),
            Some(-2),
        );
        let mut expenses: Vec<_> = expenses.into();
        expenses.push(added.clone());
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(expenses.clone()),
            view_state: PlainListViewState { hint },
        };

        let product = filled_transformer.derive(state).expect("Should be product");

        assert_eq!(product.updated, vec![added.clone().into()]);
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_added_expense_same_state_should_not_repeat(
        expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
    ) {
        let added = Expense::new_default_with(
            ExpenseId::from(100),
            &contemporary_ref_date() + 20.days(),
            Some(-2),
        );
        let mut expenses: Vec<_> = expenses.into();
        expenses.push(added.clone());
        let state = || PlainListStateSlice {
            expenses: VersionedArc::from(expenses.clone()),
            view_state: PlainListViewState { hint: None },
        };

        filled_transformer
            .derive(state())
            .expect("Should be product");

        assert_eq!(filled_transformer.derive(state()), None);
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_expense_delete_should_update_ids_remove_cache(
        expenses: [Expense; 20],
        mut filled_transformer: PlainListTransformer,
    ) {
        let mut expenses: Vec<_> = expenses.into();
        let removed = expenses.remove(10);
        let ids: Vec<u64> = expenses.iter().rev().map(|e| e.id.into()).collect();
        let state = PlainListStateSlice {
            expenses: VersionedArc::from(expenses.clone()),
            view_state: PlainListViewState { hint: None },
        };

        let products = filled_transformer
            .derive(state)
            .expect("Should generate products");
        assert_eq!(products.ids, ids);
        assert!(products.updated.is_empty());
        assert!(
            !filled_transformer
                .currently_displayed
                .contains_key(&removed.id)
        );
        assert_prev_ids(&filled_transformer, &expenses);
    }

    #[rstest]
    fn p_list_comparable_should_reflect_hint_changes(expenses: [Expense; 20]) {
        let token = ViewId::from(Uuid::now_v7());
        let mut state = State {
            app: AppState::Working(ModelState {
                movements: VersionedArc::from(Vec::from(expenses.clone())),
                ..ModelState::default()
            }),
            ..State::default()
        };

        assert_eq!(
            PlainListTransformer::comparable(&state, token),
            NothingToCompare
        );

        state
            .running
            .plain_list
            .insert(token, PlainListViewState { hint: None });
        let Comparable(before) = PlainListTransformer::comparable(&state, token) else {
            panic!("Should be comparable")
        };
        assert_eq!(
            PlainListTransformer::comparable(&state, token),
            Comparable(before)
        );

        state.running.plain_list.insert(
            token,
            PlainListViewState {
                hint: Some(expenses[10].id),
            },
        );
        let Comparable(with_hint) = PlainListTransformer::comparable(&state, token) else {
            panic!("Should be comparable")
        };
        assert_ne!(before, with_hint);

        let AppState::Working(model) = &mut state.app else {
            unreachable!()
        };
        model.movements.update_with(|_| {});
        let Comparable(with_bump) = PlainListTransformer::comparable(&state, token) else {
            panic!("Should be comparable")
        };
        assert_ne!(with_hint, with_bump);
    }
}
