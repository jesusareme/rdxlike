use crate::action::WorkingAction;
use crate::runtime::cmd::{DebounceCmd, TimeSubscriptionCmd};
use crate::util::{ClockSource};
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;
use rdxlib::util::MessageSend;

pub struct Timers {
    tx: Sender<TimersMessage>,
}

#[derive(Debug, PartialEq)]
enum TimerType {
    Repeat(Option<NonZeroU64>),
    Debounce,
}

pub struct TimerTask {
    pub id: TimerId,
    outcome: fn(u64) -> WorkingAction,
    timer_type: TimerType,
    get_duration: fn(u64) -> Duration,
}

pub enum TimersMessage {
    Bump(TimerTask),
    Cancel(TimerId),
}

#[derive(Debug, PartialEq)]
struct TimerState {
    count: u64,
    deadline: Instant,
}

#[derive(Debug)]
struct TimerBehavior {
    outcome: fn(u64) -> WorkingAction,
    get_next_duration: fn(u64) -> Duration,
    timer_type: TimerType,
}

#[derive(Debug)]
struct Timer {
    state: TimerState,
    behavior: TimerBehavior,
}

impl Timer {
    fn start(task: TimerTask, now: Instant) -> Self {
        Timer {
            state: TimerState {
                count: 0,
                deadline: now + (task.get_duration)(0),
            },
            behavior: TimerBehavior {
                outcome: task.outcome,
                get_next_duration: task.get_duration,
                timer_type: task.timer_type,
            },
        }
    }

    fn deadline(&self) -> Instant {
        self.state.deadline
    }

    pub fn bump(&mut self, now: Instant) {
        match self.behavior.timer_type {
            TimerType::Repeat(_) => self.state.count = 0,
            TimerType::Debounce => self.state.count += 1,
        };
        let next_duration = (self.behavior.get_next_duration)(self.state.count);
        self.state.deadline = now + next_duration;
    }

    pub fn repeatable(mut self) -> Option<Self> {
        if let TimerType::Repeat(max) = self.behavior.timer_type {
            let next_count = self.state.count.saturating_add(1);
            if let Some(max) = max && next_count >= u64::from(max) {
                return None;
            }
            self.state.count = next_count;
            self.state.deadline += (self.behavior.get_next_duration)(next_count);
            return Some(self);
        }
        None
    }

    pub fn action(&self) -> WorkingAction {
        (self.behavior.outcome)(self.state.count)
    }
}

impl Timers {
    pub fn new(action_tx: &impl MessageSend, clock: &Arc<dyn ClockSource + Send + Sync>) -> Self {
        let action_tx = action_tx.clone();
        let (tx, rx) = mpsc::channel();
        let clock_clone = Arc::clone(clock);
        _ = thread::spawn(move || {
            debug!("Starting Timers service.");

            let mut tasks: HashMap<TimerId, Timer> = HashMap::new();
            let mut next: Option<Instant> = None;

            loop {
                let message = match next {
                    Some(deadline) => {
                        let when = deadline.saturating_duration_since(clock_clone.now_instant());
                        match rx.recv_timeout(when) {
                            Ok(message) => Some(message),
                            Err(RecvTimeoutError::Timeout) => None,
                            Err(RecvTimeoutError::Disconnected) => break,
                        }
                    }
                    None => match rx.recv() {
                        Ok(message) => Some(message),
                        Err(_error) => break,
                    },
                };

                let now = clock_clone.now_instant();
                let actions = Self::advance(&mut tasks, message, now);
                for action in actions {
                    action_tx.send_message(action).unwrap();
                }
                next = Self::next_deadline(&tasks);
            }
            debug!("Ending Timers service.");
        });

        Timers { tx }
    }

    fn advance(
        tasks: &mut HashMap<TimerId, Timer>,
        message: Option<TimersMessage>,
        now: Instant,
    ) -> Vec<WorkingAction> {
        if let Some(message) = message {
            Self::process_message(message, tasks, now);
        }
        Self::fire_due(tasks, now)
    }

    fn next_deadline(tasks: &HashMap<TimerId, Timer>) -> Option<Instant> {
        tasks.values().map(Timer::deadline).min()
    }

    fn process_message(message: TimersMessage, tasks: &mut HashMap<TimerId, Timer>, now: Instant) {
        match message {
            TimersMessage::Bump(timer) => {
                if let Some(task) = tasks.get_mut(&timer.id) {
                    task.bump(now);
                } else {
                    tasks.insert(timer.id, Timer::start(timer, now));
                }
            }
            TimersMessage::Cancel(id) => {
                tasks.remove(&id);
            }
        }
    }

    fn fire_due(tasks: &mut HashMap<TimerId, Timer>, now: Instant) -> Vec<WorkingAction> {
        let finished: Vec<(TimerId, Timer)> = tasks
            .extract_if(|_, timer| timer.deadline() <= now)
            .collect();

        let actions = finished.iter().map(|(_, timer)| timer.action()).collect();

        finished
            .into_iter()
            .filter_map(|(id, timer)| Some((id, timer.repeatable()?)))
            .for_each(|(id, timer)| {
                tasks.insert(id, timer);
            });

        actions
    }

    pub fn submit(&self, task: TimerTask) {
        self.tx.send(TimersMessage::Bump(task)).unwrap();
    }

    pub fn remove(&self, task: TimerTask) {
        self.tx.send(TimersMessage::Cancel(task.id)).unwrap();
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Clone)]
pub enum TimerId {
    Watchdog,
    DebounceSave,
}

impl From<TimeSubscriptionCmd> for TimerTask {
    fn from(value: TimeSubscriptionCmd) -> Self {
        match value {
            TimeSubscriptionCmd::Watchdog => TimerTask {
                id: TimerId::Watchdog,
                outcome: |_| WorkingAction::WatchdogWatching,
                timer_type: TimerType::Repeat(None),
                get_duration: |_| Duration::from_secs(1),
            },
        }
    }
}

impl From<DebounceCmd> for TimerTask {
    fn from(value: DebounceCmd) -> Self {
        match value {
            DebounceCmd::DelayedSave(_) => TimerTask {
                id: TimerId::DebounceSave,
                outcome: |_| WorkingAction::Save,
                timer_type: TimerType::Debounce,
                get_duration: |_| Duration::from_secs(5),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeClock;
    use rstest::{fixture, rstest};

    impl Default for TimerBehavior {
        fn default() -> Self {
            TimerBehavior {
                outcome: |_| WorkingAction::Save,
                get_next_duration: |_| Duration::from_nanos(1),
                timer_type: TimerType::Debounce,
            }
        }
    }

    #[fixture]
    fn clock() -> FakeClock {
        FakeClock::default()
    }

    fn debounce_task() -> TimerTask {
        TimerTask {
            id: TimerId::DebounceSave,
            outcome: |_| WorkingAction::Save,
            timer_type: TimerType::Debounce,
            get_duration: |_| Duration::from_nanos(1),
        }
    }

    fn repeat_task() -> TimerTask {
        TimerTask {
            id: TimerId::Watchdog,
            outcome: |_| WorkingAction::Save,
            timer_type: TimerType::Repeat(Some(NonZeroU64::new(2).unwrap())),
            get_duration: |_| Duration::from_nanos(1),
        }
    }

    fn debounce_timer(deadline: Instant) -> Timer {
        Timer {
            state: TimerState { count: 0, deadline },
            behavior: TimerBehavior::default(),
        }
    }

    fn repeat_timer(deadline: Instant) -> Timer {
        Timer {
            state: TimerState { count: 0, deadline },
            behavior: TimerBehavior {
                timer_type: TimerType::Repeat(Some(NonZeroU64::new(2).unwrap())),
                ..TimerBehavior::default()
            },
        }
    }

    #[rstest]
    #[case::debounce(debounce_task(), TimerType::Debounce)]
    #[case::repeat(repeat_task(), TimerType::Repeat(Some(NonZeroU64::new(2).unwrap())))]
    fn timer_initial_state(
        clock: FakeClock,
        #[case] task: TimerTask,
        #[case] expected_type: TimerType,
    ) {
        let expected_final_state = TimerState {
            count: 0,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let timer = Timer::start(task, clock.now_instant());

        assert_eq!(timer.state, expected_final_state);
        assert_eq!(timer.behavior.timer_type, expected_type);
    }

    #[rstest]
    fn timer_debounce_type_bump_state_changed(clock: FakeClock) {
        let mut timer = debounce_timer(clock.now_instant());
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        timer.bump(clock.now_instant());

        assert_eq!(timer.state, expected_final_state);
    }

    #[rstest]
    fn timer_repeat_type_bump_state_changed(clock: FakeClock) {
        let mut timer = repeat_timer(clock.now_instant() + Duration::from_nanos(1));
        timer.state = TimerState {
            count: 42,
            deadline: clock.now_instant() + Duration::from_nanos(43),
        };

        let expected_final_state = TimerState {
            count: 0,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        timer.bump(clock.now_instant());

        assert_eq!(timer.state, expected_final_state);
    }

    #[rstest]
    fn timer_debounce_not_repeatable(clock: FakeClock) {
        let timer = debounce_timer(clock.now_instant());
        assert!(timer.repeatable().is_none());
    }

    #[rstest]
    fn timer_repeat_not_drained_repetitions_gets_repeatable(clock: FakeClock) {
        let timer = repeat_timer(clock.now_instant());
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let repeatable = timer.repeatable().expect("Should get repeatable");

        assert_eq!(repeatable.state, expected_final_state);
    }

    #[rstest]
    fn timer_repeat_get_next_duration_correctly_called(clock: FakeClock) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.behavior.get_next_duration = |_| Duration::from_secs(42);
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_secs(42),
        };

        let repeatable = timer.repeatable().expect("Should get repeatable");

        assert_eq!(repeatable.state, expected_final_state);
    }

    #[rstest]
    fn timer_repeat_action_outcome_correctly_called_according_to_state(clock: FakeClock) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.behavior.outcome = |repeat| {
            if repeat % 2 == 0 {
                WorkingAction::Save
            } else {
                WorkingAction::Watchdog
            }
        };

        assert_eq!(timer.action(), WorkingAction::Save);

        timer.state.count = 1;
        assert_eq!(timer.action(), WorkingAction::Watchdog);
    }

    #[rstest]
    #[case::from_zero(0, 1)]
    #[case::from_zero(1, 2)]
    #[case(u64::MAX - 1, u64::MAX)]
    #[case::saturates_at_max(u64::MAX, u64::MAX)]
    fn timer_repeat_infinite_is_always_repeatable(
        clock: FakeClock,
        #[case] start_count: u64,
        #[case] expected_count: u64,
    ) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.behavior.timer_type = TimerType::Repeat(None);
        timer.state.count = start_count;

        let next = timer
            .repeatable()
            .expect("infinite repeat is always repeatable");

        assert_eq!(next.state.count, expected_count);
    }

    #[rstest]
    fn timer_repeat_drained_repetitions_non_repeatable(clock: FakeClock) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.state.count = 1; // we are on second repetition

        assert!(timer.repeatable().is_none());
    }

    #[rstest]
    fn next_deadline_empty_gets_none() {
        let tasks: HashMap<TimerId, Timer> = HashMap::new();
        assert!(Timers::next_deadline(&tasks).is_none())
    }

    #[rstest]
    fn next_deadline_one_gets_one(clock: FakeClock) {
        let mut tasks = HashMap::new();
        tasks.insert(TimerId::Watchdog, repeat_timer(clock.now_instant()));

        assert_eq!(
            Timers::next_deadline(&tasks),
            Some(repeat_timer(clock.now_instant()).state.deadline)
        )
    }

    #[rstest]
    fn next_deadline_in_past_gets_one(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let past_deadline = clock.now_instant() - Duration::from_nanos(1);
        let timer = repeat_timer(past_deadline);
        tasks.insert(TimerId::Watchdog, timer);

        assert_eq!(Timers::next_deadline(&tasks), Some(past_deadline))
    }

    #[rstest]
    fn next_deadline_multiple_timers_gets_next(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let timer1 = repeat_timer(clock.now_instant() + Duration::from_secs(9));
        tasks.insert(TimerId::Watchdog, timer1);
        let timer2 = repeat_timer(clock.now_instant() + Duration::from_secs(7));
        tasks.insert(TimerId::DebounceSave, timer2);

        assert_eq!(
            Timers::next_deadline(&tasks),
            Some(clock.now_instant() + Duration::from_secs(7))
        );
    }

    #[rstest]
    fn next_deadline_multiple_timers_same_deadline_gets_next(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let deadline = clock.now_instant() + Duration::from_secs(9);
        let timer1 = repeat_timer(deadline);
        tasks.insert(TimerId::Watchdog, timer1);
        let timer2 = repeat_timer(deadline);
        tasks.insert(TimerId::DebounceSave, timer2);

        assert_eq!(Timers::next_deadline(&tasks), Some(deadline));
    }

    #[rstest]
    #[case::debounce(debounce_task(), TimerId::DebounceSave)]
    #[case::repeat(repeat_task(), TimerId::Watchdog)]
    fn timers_initial_bump_creates_task_and_schedule(
        clock: FakeClock,
        #[case] task: TimerTask,
        #[case] expected_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let expected_final_state = TimerState {
            count: 0,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let actions = Timers::advance(
            &mut tasks,
            Some(TimersMessage::Bump(task)),
            clock.now_instant(),
        );

        assert!(actions.is_empty());
        assert_eq!(tasks.get(&expected_id).unwrap().state, expected_final_state);
    }

    #[rstest]
    fn timers_debounce_task_mode_deadline_passed_creates_action_and_removed(clock: FakeClock) {
        let mut tasks = HashMap::new();
        tasks.insert(TimerId::DebounceSave, debounce_timer(clock.now_instant()));

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert_eq!(actions, vec![WorkingAction::Save]);
        assert!(tasks.is_empty());
    }

    #[rstest]
    fn timers_debounce_task_mode_deadline_not_passed_no_action_state_not_updated(clock: FakeClock) {
        let mut tasks = HashMap::new();
        tasks.insert(
            TimerId::DebounceSave,
            debounce_timer(clock.now_instant() + Duration::from_nanos(1)),
        );
        let final_state = TimerState {
            count: 0,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert!(actions.is_empty());
        assert_eq!(
            tasks.get(&TimerId::DebounceSave).unwrap().state,
            final_state
        );
    }

    #[rstest]
    fn timers_debounce_task_mode_deadline_jumped_no_action_state_updated(clock: FakeClock) {
        let mut tasks = HashMap::new();
        tasks.insert(TimerId::DebounceSave, debounce_timer(clock.now_instant()));
        let final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let actions = Timers::advance(
            &mut tasks,
            Some(TimersMessage::Bump(debounce_task())),
            clock.now_instant(),
        );
        assert!(actions.is_empty());
        assert_eq!(
            tasks.get(&TimerId::DebounceSave).unwrap().state,
            final_state
        );
    }

    #[rstest]
    fn timers_debounce_mode_cancel_gets_cancelled_no_action(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let timer = debounce_timer(clock.now_instant());
        tasks.insert(TimerId::DebounceSave, timer);

        assert!(
            Timers::advance(
                &mut tasks,
                Some(TimersMessage::Cancel(TimerId::DebounceSave)),
                clock.now_instant()
            )
            .is_empty()
        );
        assert!(tasks.is_empty())
    }

    #[rstest]
    fn timers_repeat_mode_successive_bump_resets_state(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let mut timer = repeat_timer(clock.now_instant());
        timer.state.count = 1;
        timer.state.deadline = clock.now_instant();
        tasks.insert(TimerId::Watchdog, timer);
        let expected_final_state = TimerState {
            count: 0,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };
        assert!(
            Timers::advance(
                &mut tasks,
                Some(TimersMessage::Bump(repeat_task())),
                clock.now_instant()
            )
            .is_empty()
        );
        assert_eq!(
            tasks.get(&TimerId::Watchdog).unwrap().state,
            expected_final_state
        );
    }

    #[rstest]
    fn timers_repeat_mode_deadline_passed_gets_action_re_scheduled(clock: FakeClock) {
        let mut tasks = HashMap::new();
        tasks.insert(TimerId::Watchdog, repeat_timer(clock.now_instant()));
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert_eq!(actions, vec![WorkingAction::Save]);
        assert_eq!(
            tasks.get(&TimerId::Watchdog).unwrap().state,
            expected_final_state
        );
    }

    #[rstest]
    fn timers_repeat_mode_last_repeat_gets_action_no_rescheduling(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let mut timer = repeat_timer(clock.now_instant());
        timer.state.count = 1;
        tasks.insert(TimerId::Watchdog, timer);

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert_eq!(actions, vec![WorkingAction::Save]);
        assert!(tasks.is_empty());
    }

    #[rstest]
    fn timers_repeat_mode_cancel_gets_cancelled(clock: FakeClock) {
        let mut tasks = HashMap::new();
        let timer = repeat_timer(clock.now_instant());
        tasks.insert(TimerId::Watchdog, timer);

        assert!(
            Timers::advance(
                &mut tasks,
                Some(TimersMessage::Cancel(TimerId::Watchdog)),
                clock.now_instant()
            )
            .is_empty()
        );
        assert!(tasks.is_empty())
    }
}
