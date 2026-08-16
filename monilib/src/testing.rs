use crate::runtime::Expense;
use crate::util::{ClockSource, ExpenseId};
use jiff::tz::TimeZone;
use jiff::{Timestamp, ToSpan, Zoned};
use std::time::Instant;
use uuid::Uuid;

pub fn ref_id() -> u64 {
    42
}

pub fn ref_uuid() -> Uuid {
    Uuid::parse_str("a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8")
        .expect("Example Uuid parsing should not fail")
}

pub fn alternative_ref_uuid() -> Uuid {
    Uuid::parse_str("f1f2f3f4e1e2d1d2c1c2b1b2b3b4b5b6")
        .expect("Example Uuid parsing should not fail")
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
        let id = ExpenseId::from(i as u64);
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
        panic!("StuckClock not meant to be used for instant time")
    }
}

impl Default for StuckClock {
    fn default() -> Self {
        StuckClock {
            stuck_at: contemporary_ref_date(),
        }
    }
}

pub struct StuckInstantClock {
    pub stuck_at: Instant,
}

impl ClockSource for StuckInstantClock {
    fn now_civil(&self) -> Zoned {
        panic!("StuckInstantClock not meant to be used for civil time")
    }

    fn now_instant(&self) -> Instant {
        self.stuck_at
    }
}

impl Default for StuckInstantClock {
    fn default() -> Self {
        StuckInstantClock {
            stuck_at: Instant::now(),
        }
    }
}
