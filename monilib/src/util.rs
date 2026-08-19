use jiff::Zoned;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::time::Instant;
use std::{
    ops::Deref,
    sync::Arc,
};

pub trait ClockSource {
    fn now_civil(&self) -> Zoned;
    fn now_instant(&self) -> Instant;
}

pub struct SystemClockSource;

impl ClockSource for SystemClockSource {
    fn now_civil(&self) -> Zoned {
        Zoned::now()
    }
    fn now_instant(&self) -> Instant {
        Instant::now()
    }
}

pub trait IdSource {
    /// Returns the currently available value and advances the internal counter
    fn get_and_inc(&mut self) -> Self;
}

#[derive(PartialEq, Eq, Hash, Debug, Serialize, Deserialize, Clone, Copy, Default)]
pub struct ExpenseId(u64);

impl IdSource for ExpenseId {
    fn get_and_inc(&mut self) -> Self {
        let next = *self;
        *self = ExpenseId(self.0.checked_add(1).expect("ExpenseId space exhausted"));
        next
    }
}

impl From<u64> for ExpenseId {
    fn from(value: u64) -> Self {
        ExpenseId(value)
    }
}

impl From<ExpenseId> for u64 {
    fn from(value: ExpenseId) -> Self {
        value.0
    }
}

impl Display for ExpenseId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ExpenseId").field(&self.0).finish()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VersionedArc<T> {
    content: Arc<T>,
    version: u64,
}

impl<T> VersionedArc<T>
where
    T: Clone,
{
    pub fn update_with<R>(&mut self, update: impl FnOnce(&mut T) -> R) -> R {
        self.version = self.version.wrapping_add(1);
        update(Arc::make_mut(&mut self.content))
    }
}

impl<T> Deref for VersionedArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl<T> PartialEq for VersionedArc<T>
where
    T: PartialEq,
{
    /// Two `Versioned<T>` values are partially equal if the contained `T` values are partially equal.
    /// So, two versioned values are considered equal irrespective of the version they are on.
    fn eq(&self, other: &Self) -> bool {
        self.content.eq(&other.content)
    }
}

impl<T> VersionedArc<T> {
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl<T> From<T> for VersionedArc<T> {
    fn from(value: T) -> Self {
        VersionedArc {
            content: Arc::new(value),
            version: 0,
        }
    }
}

#[cfg(test)]
mod test_versioned {
    use super::VersionedArc;
    use std::ops::Deref;

    #[test]
    fn versioned_inc_version_mut_ref() {
        let mut versioned = VersionedArc::from(42);
        versioned.update_with(|value| *value += 1);
        assert_eq!(versioned.version, 1);
        versioned.update_with(|value| *value -= 2);
        versioned.update_with(|_value| {});
        assert_eq!(versioned.version, 3);
        assert_eq!(versioned.version(), 3);
        assert_eq!(*versioned, 41);
    }

    #[test]
    fn versioned_no_inc_version_ref() {
        let versioned = VersionedArc::from(42);
        let value1 = *versioned;
        let value2 = versioned.deref();
        assert_eq!(versioned.version, 0);
        assert_eq!(versioned.version(), 0);
        assert_eq!((value1, *value2), (42, 42));
    }
}
