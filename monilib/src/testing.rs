use crate::runtime::Expense;
use crate::util::{ClockSource, ExpenseId, IdSource};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};
use std::cell::Cell;
use std::str::FromStr;
use std::time::Instant;
use uuid::Uuid;

pub fn ref_id() -> Uuid {
    Uuid::from_str("01234567-0123-0123-0123-000000000001").unwrap()
}

pub fn contemporary_ref_date() -> Zoned {
    "2026-05-01 00:00[Europe/Madrid]".parse().unwrap()
}

pub fn distant_future_ref_date() -> Zoned {
    "2099-05-01 00:00[Europe/Madrid]".parse().unwrap()
}

pub fn distant_past_ref_date() -> Zoned {
    Zoned::new(Timestamp::UNIX_EPOCH, TimeZone::UTC)
}

pub fn ordered_by_index_map<const S: usize, T: PartialOrd + Clone>(
    original: Vec<T>,
    expected_order: [usize; S],
) -> Vec<T> {
    assert_eq!(original.len(), S);
    let mut nullable: Vec<_> = original.into_iter().map(Some).collect();
    expected_order
        .iter()
        .filter_map(|index| nullable[*index].take())
        .collect()
}

pub fn ordered_expenses<const S: usize>(ref_date: Zoned) -> [Expense; S] {
    std::array::from_fn(|i| {
        let id =
            ExpenseId::from(Uuid::from_str(&format!("01234567-0123-0123-0123-{:012}", i)).unwrap());
        Expense::new_default_with(id, &ref_date + (i as i64).days(), Some(i as i64))
    })
}

pub struct StuckClock {
    pub stuck_at: Zoned,
}

impl ClockSource for StuckClock {
    fn now_civil(&self) -> Zoned {
        self.stuck_at.clone()
    }

    fn now_instant(&self) -> Instant {
        panic!(
            "StuckClock not meant to be used for instant time, use FakeClock for more complete control"
        )
    }
}

pub struct FixedIdSource {
    pub id: ExpenseId,
}

impl IdSource for FixedIdSource {
    fn new_expense_id(&self, _at: Timestamp) -> ExpenseId {
        self.id
    }
}

pub struct FakeClock {
    civil_count: Cell<u32>,
    instant: Cell<Instant>,
    instant_count: Cell<u32>,
}

impl ClockSource for FakeClock {
    fn now_civil(&self) -> Zoned {
        self.civil_count.set(self.civil_count.get() + 1);
        contemporary_ref_date()
    }

    fn now_instant(&self) -> Instant {
        self.instant_count.update(|instant| instant + 1);
        self.instant.get()
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        FakeClock {
            civil_count: Cell::new(0),
            instant: Cell::new(Instant::now()),
            instant_count: Cell::new(0),
        }
    }
}
