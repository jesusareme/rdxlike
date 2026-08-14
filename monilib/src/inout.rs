use crate::inout::MoniValidationErrorCause::Range;
use crate::runtime::Expense;
use crate::util::{ClockSource, ExpenseId};
use crate::{ExpenseCategory, LibErrorCause, MoniError, MoniErrorType};
use boltffi::{EventSubscription, data, error};
use jiff::Zoned;
use rdxlib::subscribers::ViewOutput;
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
    path::Path,
    sync::Arc,
    time::SystemTime,
};

#[data]
#[derive(Clone, Debug, PartialEq)]
pub enum MoniValidationErrorCause {
    Date,
    Empty,
    Range,
}

#[error]
#[derive(Clone, Debug, PartialEq)]
pub struct MoniValidationError {
    cause: MoniValidationErrorCause,
    field: String,
}

impl Display for MoniValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Validation error for field: '{}', cause: {:?}",
            self.field, self.cause
        )
    }
}

impl Error for MoniValidationError {}

#[data]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MoniStatistics {
    pub date: SystemTime,
    pub len: usize,
    pub sum: Option<i64>,
    pub min: Option<i64>,
    pub max: Option<i64>,
}

#[data]
#[derive(Clone, Debug, PartialEq)]
pub struct PlainListItem {
    pub id: u64,
    pub date: SystemTime,
    pub amount: i64,
    pub comment: Option<String>,
    pub category: ExpenseCategory,
}

#[data]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoniExpense {
    pub date: Option<SystemTime>,
    pub amount: i64,
    pub comment: Option<String>,
    pub category: ExpenseCategory,
}

#[data]
#[derive(Clone)]
pub struct MoniExpenseUpdate {
    pub id: u64,
    pub expense: MoniExpense,
}

#[data]
#[derive(Clone, Debug, PartialEq)]
pub struct MoniExpensePlainListSnapshot {
    pub ids: Vec<u64>,
    pub updated: Vec<PlainListItem>,
}

fn date_error(cause: MoniValidationErrorCause) -> MoniValidationError {
    MoniValidationError {
        cause,
        field: "date".to_string(),
    }
}

fn validated_date(date: SystemTime, clock: &dyn ClockSource) -> Result<Zoned, MoniValidationError> {
    let date = Zoned::try_from(date).map_err(|_| date_error(MoniValidationErrorCause::Date))?;

    if date > clock.now_civil() {
        return Err(date_error(Range));
    }

    Ok(date)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExpenseAddIntent {
    pub date: Option<Zoned>,
    pub amount: i64,
    pub comment: Option<String>,
    pub category: ExpenseCategory,
}

impl MoniExpenseUpdate {
    pub fn into_updatable_expense(
        self,
        clock: &dyn ClockSource,
    ) -> Result<Expense, MoniValidationError> {
        let MoniExpenseUpdate {
            id,
            expense:
                MoniExpense {
                    date,
                    amount,
                    comment,
                    category,
                },
        } = self;

        let date = match date {
            None => return Err(date_error(MoniValidationErrorCause::Empty)),
            Some(system_date) => validated_date(system_date, clock)?,
        };

        let amount = MoniExpense::validated_amount(amount)?;

        Ok(Expense::new(
            ExpenseId::from(id),
            date,
            amount,
            comment,
            category,
        ))
    }
}

impl MoniExpense {
    pub(crate) fn into_add_intent(
        self,
        clock: &dyn ClockSource,
    ) -> Result<ExpenseAddIntent, MoniValidationError> {
        let validated_date = match self.date {
            Some(system_date) => Some(validated_date(system_date, clock)?),
            None => None,
        };

        let amount = Self::validated_amount(self.amount)?;

        Ok(ExpenseAddIntent {
            date: validated_date,
            amount,
            comment: self.comment,
            category: self.category,
        })
    }
}

impl MoniExpense {
    pub const AMOUNT_LIMIT: i64 = 1_000_000_00; // 1M

    fn validated_amount(amount: i64) -> Result<i64, MoniValidationError> {
        if (-Self::AMOUNT_LIMIT..=Self::AMOUNT_LIMIT).contains(&amount) {
            Ok(amount)
        } else {
            Err(MoniValidationError {
                cause: Range,
                field: "amount".to_string(),
            })
        }
    }
}

#[cfg(test)]
impl Default for MoniExpense {
    fn default() -> Self {
        MoniExpense {
            date: None,
            amount: 1230,
            comment: Some("comment".to_string()),
            category: ExpenseCategory::Essential,
        }
    }
}

pub struct LibOutput<V>
where
    V: Send + 'static,
{
    output: Arc<EventSubscription<V>>,
}

impl<V> ViewOutput<V> for LibOutput<V>
where
    V: Send + 'static,
{
    fn new(capacity: usize) -> Self {
        LibOutput {
            output: Arc::new(EventSubscription::new(capacity)),
        }
    }

    fn send(&self, v: V) -> bool {
        self.output.push_event(v)
    }

    fn is_active(&self) -> bool {
        self.output.is_active()
    }
}

impl<V> From<LibOutput<V>> for Arc<EventSubscription<V>>
where
    V: Send + 'static,
{
    fn from(lib_output: LibOutput<V>) -> Self {
        lib_output.output
    }
}

impl<V> Clone for LibOutput<V>
where
    V: Send + 'static,
{
    fn clone(&self) -> Self {
        LibOutput {
            output: self.output.clone(),
        }
    }
}

pub fn try_state_path(path: impl AsRef<Path>) -> Result<(), MoniError> {
    // TODO: Implement a solid way of checking access.
    match std::fs::exists(path) {
        Ok(true) => Ok(()),
        _ => Err(MoniError::new(MoniErrorType::Lib(LibErrorCause::Path))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{
        StuckClock, contemporary_ref_date, distant_future_ref_date, distant_past_ref_date, ref_id,
    };
    use jiff::ToSpan;
    use rstest::rstest;

    fn add_intent(date: Option<Zoned>, amount: i64) -> ExpenseAddIntent {
        ExpenseAddIntent {
            date,
            amount,
            comment: Some("comment".to_string()),
            category: ExpenseCategory::Essential,
        }
    }

    #[test]
    fn into_add_intent_no_date_stays_empty() {
        let m_expense = MoniExpense::default();
        let clock = StuckClock {
            stuck_at: distant_future_ref_date(),
        };

        let intent = m_expense
            .into_add_intent(&clock)
            .expect("Conversion should work without errors");

        assert_eq!(intent, add_intent(None, 1230))
    }

    #[test]
    fn into_add_intent_date_future() {
        let m_expense = MoniExpense {
            date: Some(distant_future_ref_date().into()),
            ..MoniExpense::default()
        };

        let clock = StuckClock {
            stuck_at: distant_future_ref_date() - 1.nanosecond(),
        };

        assert_eq!(
            m_expense.into_add_intent(&clock).err().unwrap().cause,
            Range
        );
    }

    #[test]
    fn into_add_intent_date_past() {
        let m_expense = MoniExpense {
            date: Some(distant_future_ref_date().into()),
            ..MoniExpense::default()
        };

        let clock = StuckClock {
            stuck_at: distant_future_ref_date() + 1.nanosecond(),
        };

        let intent = m_expense.into_add_intent(&clock).expect("no errors");

        assert_eq!(intent, add_intent(Some(distant_future_ref_date()), 1230))
    }

    #[test]
    fn into_updatable_expense_empty_date_should_err() {
        let no_date_expense = MoniExpense {
            date: None,
            ..MoniExpense::default()
        };
        let expense_update = MoniExpenseUpdate {
            id: ref_id(),
            expense: no_date_expense,
        };
        let clock = StuckClock {
            stuck_at: contemporary_ref_date(),
        };

        let error = expense_update
            .into_updatable_expense(&clock)
            .expect_err("Should be error");

        assert_eq!(error.cause, MoniValidationErrorCause::Empty);
        assert_eq!(error.field, "date");
    }

    #[test]
    fn into_updatable_expense_future_date_should_err() {
        let future_date_expense = MoniExpense {
            date: Some(distant_future_ref_date().into()),
            ..MoniExpense::default()
        };
        let expense_update = MoniExpenseUpdate {
            id: ref_id(),
            expense: future_date_expense,
        };
        let clock = StuckClock {
            stuck_at: contemporary_ref_date(),
        };

        let error = expense_update
            .into_updatable_expense(&clock)
            .expect_err("Should be error");

        assert_eq!(error.cause, Range);
        assert_eq!(error.field, "date");
    }

    #[test]
    fn into_updatable_expense_past_date_should_ok() {
        let past_date_expense = MoniExpense {
            date: Some(distant_past_ref_date().into()),
            ..MoniExpense::default()
        };
        let expense_update = MoniExpenseUpdate {
            id: ref_id(),
            expense: past_date_expense,
        };
        let clock = StuckClock {
            stuck_at: contemporary_ref_date(),
        };

        let expense = expense_update
            .into_updatable_expense(&clock)
            .expect("Should be ok");

        let compared =
            Expense::new_default_with(ExpenseId::from(ref_id()), distant_past_ref_date(), None);

        assert_eq!(expense, compared);
    }

    #[rstest]
    #[case::above_limit(MoniExpense::AMOUNT_LIMIT + 1, false)]
    #[case::below_limit(-MoniExpense::AMOUNT_LIMIT - 1, false)]
    #[case::at_upper_limit(MoniExpense::AMOUNT_LIMIT, true)]
    #[case::at_lower_limit(-MoniExpense::AMOUNT_LIMIT, true)]
    #[case::within_limits(4200, true)]
    fn into_add_intent_validates_amount(#[case] amount: i64, #[case] valid: bool) {
        let m_expense = MoniExpense {
            amount,
            ..MoniExpense::default()
        };
        let clock = StuckClock {
            stuck_at: contemporary_ref_date(),
        };

        let result = m_expense.into_add_intent(&clock);

        if valid {
            assert_eq!(result.expect("Should be ok"), add_intent(None, amount));
        } else {
            let error = result.expect_err("Should be error");
            assert_eq!(error.cause, Range);
            assert_eq!(error.field, "amount");
        }
    }

    #[rstest]
    #[case::above_limit(MoniExpense::AMOUNT_LIMIT + 1, false)]
    #[case::below_limit(-MoniExpense::AMOUNT_LIMIT - 1, false)]
    #[case::at_upper_limit(MoniExpense::AMOUNT_LIMIT, true)]
    #[case::at_lower_limit(-MoniExpense::AMOUNT_LIMIT, true)]
    #[case::within_limits(4200, true)]
    fn into_updatable_expense_validates_amount(#[case] amount: i64, #[case] valid: bool) {
        let m_expense = MoniExpense {
            date: Some(distant_past_ref_date().into()),
            amount,
            ..MoniExpense::default()
        };
        let expense_update = MoniExpenseUpdate {
            id: ref_id(),
            expense: m_expense,
        };
        let clock = StuckClock {
            stuck_at: contemporary_ref_date(),
        };

        let result = expense_update.into_updatable_expense(&clock);

        if valid {
            let compared = Expense::new_default_with(
                ExpenseId::from(ref_id()),
                distant_past_ref_date(),
                Some(amount),
            );
            assert_eq!(result.expect("Should be ok"), compared);
        } else {
            let error = result.expect_err("Should be error");
            assert_eq!(error.cause, Range);
            assert_eq!(error.field, "amount");
        }
    }
}
