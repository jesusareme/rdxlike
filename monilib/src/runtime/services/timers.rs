use crate::MoniError;
use crate::action::WorkingAction;
use crate::runtime::MoniMessage;
use crate::runtime::cmd::{DebounceAction, DebounceCmd, TimeSubscriptionCmd};
use crate::util::ClockSource;
use rdxlib::util::MessageSend;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroU64;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use tracing::debug;
use uuid::Uuid;

pub(crate) struct Timers {
    tx: Sender<TimersMessage>,
}

#[derive(Debug, PartialEq)]
enum TimerType {
    Repeat(Option<NonZeroU64>),
    Debounce,
}

type TimerOutcome = dyn Fn(u64) -> WorkingAction + Send;
type TimerDuration = dyn Fn(u64) -> Duration + Send;

pub(crate) struct TimerTask {
    pub id: TimerId,
    outcome: Box<TimerOutcome>,
    timer_type: TimerType,
    get_duration: Box<TimerDuration>,
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

struct TimerBehavior {
    outcome: Box<TimerOutcome>,
    get_next_duration: Box<TimerDuration>,
    timer_type: TimerType,
}

impl Debug for TimerBehavior {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimerBehavior")
            .field("timer_type", &self.timer_type)
            .finish()
    }
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
            if let Some(max) = max
                && next_count >= u64::from(max)
            {
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
    pub fn new(
        action_tx: &impl MessageSend<Message = MoniMessage>,
        clock: &Arc<dyn ClockSource + Send + Sync>,
    ) -> Result<Self, MoniError> {
        let action_tx = action_tx.clone();
        let (tx, rx) = mpsc::channel();
        let clock_clone = Arc::clone(clock);
        let builder = thread::Builder::new().name("Timers.thread".to_string());
        builder.spawn(move || {
            debug!("Starting Timers service.");

            let mut tasks: HashMap<TimerId, Timer> = HashMap::new();
            let mut next: Option<Instant> = None;

            'thread_loop: loop {
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
                    if !action_tx.send_message(action).is_ok() {
                        debug!("Sender dropped, nothing more to do here...");
                        break 'thread_loop;
                    }
                }
                next = Self::next_deadline(&tasks);
            }
            debug!("Ending Timers service.");
        })?;

        Ok(Timers { tx })
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

    pub fn send(&self, message: TimersMessage) -> Result<(), MoniError> {
        self.tx.send(message)?;
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub enum TimerId {
    DebounceSave,
    RepeatedAction(Uuid),
}

impl From<TimeSubscriptionCmd> for TimersMessage {
    fn from(value: TimeSubscriptionCmd) -> Self {
        match value {
            TimeSubscriptionCmd::EveryXInterval(id, interval, action) => {
                TimersMessage::Bump(TimerTask {
                    id: TimerId::RepeatedAction(id),
                    outcome: Box::new(move |_| action.clone()),
                    timer_type: TimerType::Repeat(None),
                    get_duration: Box::new(move |_| interval),
                })
            }
            TimeSubscriptionCmd::CancelEveryXInterval(id) => {
                TimersMessage::Cancel(TimerId::RepeatedAction(id))
            }
        }
    }
}

impl From<DebounceCmd> for TimersMessage {
    fn from(value: DebounceCmd) -> Self {
        match value {
            DebounceCmd::DelayedSave(DebounceAction::Bump) => TimersMessage::Bump(TimerTask {
                id: TimerId::DebounceSave,
                outcome: Box::new(|_| WorkingAction::Save),
                timer_type: TimerType::Debounce,
                get_duration: Box::new(|_| Duration::from_secs(5)),
            }),
            DebounceCmd::DelayedSave(DebounceAction::Cancel) => {
                TimersMessage::Cancel(TimerId::DebounceSave)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{StuckInstantClock, alternative_ref_uuid, ref_uuid};
    use rstest::{fixture, rstest};

    impl Default for TimerBehavior {
        fn default() -> Self {
            TimerBehavior {
                outcome: Box::new(|_| WorkingAction::Save),
                get_next_duration: Box::new(|_| Duration::from_nanos(1)),
                timer_type: TimerType::Debounce,
            }
        }
    }

    #[fixture]
    fn clock() -> StuckInstantClock {
        StuckInstantClock::default()
    }

    fn debounce_task() -> TimerTask {
        TimerTask {
            id: TimerId::DebounceSave,
            outcome: Box::new(|_| WorkingAction::Save),
            timer_type: TimerType::Debounce,
            get_duration: Box::new(|_| Duration::from_nanos(1)),
        }
    }

    fn repeat_task() -> TimerTask {
        TimerTask {
            id: TimerId::RepeatedAction(ref_uuid()),
            outcome: Box::new(|_| WorkingAction::Save),
            timer_type: TimerType::Repeat(Some(NonZeroU64::new(2).unwrap())),
            get_duration: Box::new(|_| Duration::from_nanos(1)),
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

    #[fixture]
    fn repeated_task_id() -> TimerId {
        TimerId::RepeatedAction(ref_uuid())
    }

    #[rstest]
    #[case::debounce(debounce_task(), TimerType::Debounce)]
    #[case::repeat(repeat_task(), TimerType::Repeat(Some(NonZeroU64::new(2).unwrap())))]
    fn timer_started_from_task_should_have_zero_count_and_first_deadline(
        clock: StuckInstantClock,
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
    fn timer_debounce_bumped_should_increment_count_and_reschedule(clock: StuckInstantClock) {
        let mut timer = debounce_timer(clock.now_instant());
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        timer.bump(clock.now_instant());

        assert_eq!(timer.state, expected_final_state);
    }

    #[rstest]
    fn timer_repeat_bumped_should_reset_count_and_reschedule(clock: StuckInstantClock) {
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
    fn timer_debounce_should_not_be_repeatable(clock: StuckInstantClock) {
        let timer = debounce_timer(clock.now_instant());
        assert!(timer.repeatable().is_none());
    }

    #[rstest]
    fn timer_repeat_with_remaining_repetitions_should_be_repeatable(clock: StuckInstantClock) {
        let timer = repeat_timer(clock.now_instant());
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let repeatable = timer.repeatable().expect("Should get repeatable");

        assert_eq!(repeatable.state, expected_final_state);
    }

    #[rstest]
    fn timer_repeat_repeated_should_reschedule_with_next_duration(clock: StuckInstantClock) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.behavior.get_next_duration = Box::new(|_| Duration::from_secs(42));
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_secs(42),
        };

        let repeatable = timer.repeatable().expect("Should get repeatable");

        assert_eq!(repeatable.state, expected_final_state);
    }

    #[rstest]
    fn timer_repeat_action_should_be_derived_from_repetition_count(clock: StuckInstantClock) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.behavior.outcome = Box::new(|repeat| {
            if repeat % 2 == 0 {
                WorkingAction::Save
            } else {
                WorkingAction::SuccessfulSave
            }
        });

        assert_eq!(timer.action(), WorkingAction::Save);

        timer.state.count = 1;
        assert_eq!(timer.action(), WorkingAction::SuccessfulSave);
    }

    #[rstest]
    #[case::from_zero(0, 1)]
    #[case::from_one(1, 2)]
    #[case::near_max(u64::MAX - 1, u64::MAX)]
    #[case::saturates_at_max(u64::MAX, u64::MAX)]
    fn timer_repeat_infinite_should_always_be_repeatable(
        clock: StuckInstantClock,
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
    fn timer_repeat_drained_repetitions_should_not_be_repeatable(clock: StuckInstantClock) {
        let mut timer = repeat_timer(clock.now_instant());
        timer.state.count = 1; // we are on second repetition

        assert!(timer.repeatable().is_none());
    }

    #[rstest]
    fn next_deadline_without_tasks_should_get_none() {
        let tasks: HashMap<TimerId, Timer> = HashMap::new();
        assert!(Timers::next_deadline(&tasks).is_none())
    }

    #[rstest]
    fn next_deadline_single_task_should_get_its_deadline(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        tasks.insert(repeated_task_id, repeat_timer(clock.now_instant()));

        assert_eq!(
            Timers::next_deadline(&tasks),
            Some(repeat_timer(clock.now_instant()).state.deadline)
        )
    }

    #[rstest]
    fn next_deadline_already_passed_should_still_get_it(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let past_deadline = clock.now_instant() - Duration::from_nanos(1);
        let timer = repeat_timer(past_deadline);
        tasks.insert(repeated_task_id, timer);

        assert_eq!(Timers::next_deadline(&tasks), Some(past_deadline))
    }

    #[rstest]
    fn next_deadline_multiple_tasks_should_get_earliest(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let timer1 = repeat_timer(clock.now_instant() + Duration::from_secs(9));
        tasks.insert(repeated_task_id, timer1);
        let timer2 = repeat_timer(clock.now_instant() + Duration::from_secs(7));
        tasks.insert(TimerId::DebounceSave, timer2);

        assert_eq!(
            Timers::next_deadline(&tasks),
            Some(clock.now_instant() + Duration::from_secs(7))
        );
    }

    #[rstest]
    fn next_deadline_multiple_tasks_sharing_deadline_should_get_that_deadline(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let deadline = clock.now_instant() + Duration::from_secs(9);
        let timer1 = repeat_timer(deadline);
        tasks.insert(repeated_task_id, timer1);
        let timer2 = repeat_timer(deadline);
        tasks.insert(TimerId::DebounceSave, timer2);

        assert_eq!(Timers::next_deadline(&tasks), Some(deadline));
    }

    #[rstest]
    #[case::debounce(debounce_task(), TimerId::DebounceSave)]
    #[case::repeat(repeat_task(), repeated_task_id())]
    fn timers_first_bump_should_create_task_and_schedule_it(
        clock: StuckInstantClock,
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
    fn timers_debounce_deadline_passed_should_fire_action_and_remove_task(
        clock: StuckInstantClock,
    ) {
        let mut tasks = HashMap::new();
        tasks.insert(TimerId::DebounceSave, debounce_timer(clock.now_instant()));

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert_eq!(actions, vec![WorkingAction::Save]);
        assert!(tasks.is_empty());
    }

    #[rstest]
    fn timers_debounce_deadline_not_passed_should_not_fire_nor_change_state(
        clock: StuckInstantClock,
    ) {
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
    fn timers_debounce_bumped_before_firing_should_reschedule_without_firing(
        clock: StuckInstantClock,
    ) {
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
    fn timers_debounce_cancelled_should_remove_task_without_firing(clock: StuckInstantClock) {
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
    fn timers_repeat_bumped_before_firing_should_reset_state_without_firing(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let mut timer = repeat_timer(clock.now_instant());
        timer.state.count = 1;
        timer.state.deadline = clock.now_instant();
        tasks.insert(repeated_task_id, timer);
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
            tasks.get(&repeated_task_id).unwrap().state,
            expected_final_state
        );
    }

    #[rstest]
    fn timers_repeat_deadline_passed_should_fire_action_and_reschedule(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        tasks.insert(repeated_task_id, repeat_timer(clock.now_instant()));
        let expected_final_state = TimerState {
            count: 1,
            deadline: clock.now_instant() + Duration::from_nanos(1),
        };

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert_eq!(actions, vec![WorkingAction::Save]);
        assert_eq!(
            tasks.get(&repeated_task_id).unwrap().state,
            expected_final_state
        );
    }

    #[rstest]
    fn timers_repeat_last_repetition_should_fire_action_and_remove_task(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let mut timer = repeat_timer(clock.now_instant());
        timer.state.count = 1;
        tasks.insert(repeated_task_id, timer);

        let actions = Timers::advance(&mut tasks, None, clock.now_instant());
        assert_eq!(actions, vec![WorkingAction::Save]);
        assert!(tasks.is_empty());
    }

    #[rstest]
    fn timers_repeat_cancelled_should_remove_task_without_firing(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let timer = repeat_timer(clock.now_instant());
        tasks.insert(repeated_task_id, timer);

        assert!(
            Timers::advance(
                &mut tasks,
                Some(TimersMessage::Cancel(repeated_task_id)),
                clock.now_instant()
            )
            .is_empty()
        );
        assert!(tasks.is_empty())
    }

    #[rstest]
    fn timers_cancel_unknown_task_should_leave_registered_ones_the_same(
        clock: StuckInstantClock,
        repeated_task_id: TimerId,
    ) {
        let mut tasks = HashMap::new();
        let deadline = clock.now_instant() + Duration::from_secs(9);
        tasks.insert(repeated_task_id, repeat_timer(deadline));

        let actions = Timers::advance(
            &mut tasks,
            Some(TimersMessage::Cancel(TimerId::RepeatedAction(
                alternative_ref_uuid(),
            ))),
            clock.now_instant(),
        );

        assert!(actions.is_empty());
        assert_eq!(
            tasks.get(&repeated_task_id).unwrap().state.deadline,
            deadline
        );
    }

    #[rstest]
    fn every_x_interval_cmd_should_convert_into_bump_of_repeated_task() {
        let message = TimersMessage::from(TimeSubscriptionCmd::EveryXInterval(
            ref_uuid(),
            Duration::from_secs(30),
            WorkingAction::Save,
        ));

        let TimersMessage::Bump(task) = message else {
            panic!("EveryXInterval should convert into a Bump");
        };

        assert_eq!(task.id, TimerId::RepeatedAction(ref_uuid()));
        assert_eq!(task.timer_type, TimerType::Repeat(None));
        assert_eq!((task.get_duration)(0), Duration::from_secs(30));
        assert_eq!((task.get_duration)(41), Duration::from_secs(30));
    }

    #[rstest]
    fn every_x_interval_cmd_should_yield_same_action_on_every_repetition() {
        let message = TimersMessage::from(TimeSubscriptionCmd::EveryXInterval(
            ref_uuid(),
            Duration::from_secs(30),
            WorkingAction::SuccessfulSave,
        ));

        let TimersMessage::Bump(task) = message else {
            panic!("EveryXInterval should convert into a Bump");
        };

        assert_eq!((task.outcome)(0), WorkingAction::SuccessfulSave);
        assert_eq!((task.outcome)(1), WorkingAction::SuccessfulSave);
        assert_eq!((task.outcome)(u64::MAX), WorkingAction::SuccessfulSave);
    }

    #[rstest]
    fn cancel_every_x_interval_cmd_should_convert_into_cancel_of_repeated_task() {
        let message = TimersMessage::from(TimeSubscriptionCmd::CancelEveryXInterval(ref_uuid()));

        let TimersMessage::Cancel(id) = message else {
            panic!("CancelEveryXInterval should convert into a Cancel");
        };

        assert_eq!(id, TimerId::RepeatedAction(ref_uuid()));
    }

    #[rstest]
    fn delayed_save_bump_cmd_should_convert_into_bump_of_debounce_task() {
        let message = TimersMessage::from(DebounceCmd::DelayedSave(DebounceAction::Bump));

        let TimersMessage::Bump(task) = message else {
            panic!("DelayedSave(Bump) should convert into a Bump");
        };

        assert_eq!(task.id, TimerId::DebounceSave);
        assert_eq!(task.timer_type, TimerType::Debounce);
        assert_eq!((task.outcome)(0), WorkingAction::Save);
        assert_eq!((task.get_duration)(0), Duration::from_secs(5));
    }

    #[rstest]
    fn delayed_save_cancel_cmd_should_convert_into_cancel_of_debounce_task() {
        let message = TimersMessage::from(DebounceCmd::DelayedSave(DebounceAction::Cancel));

        let TimersMessage::Cancel(id) = message else {
            panic!("DelayedSave(Cancel) should convert into a Cancel");
        };

        assert_eq!(id, TimerId::DebounceSave);
    }

    #[rstest]
    fn converted_cmds_should_be_processed_through_advance(clock: StuckInstantClock) {
        let mut tasks = HashMap::new();

        Timers::advance(
            &mut tasks,
            Some(
                TimeSubscriptionCmd::EveryXInterval(
                    ref_uuid(),
                    Duration::from_secs(30),
                    WorkingAction::Save,
                )
                .into(),
            ),
            clock.now_instant(),
        );
        assert!(tasks.contains_key(&TimerId::RepeatedAction(ref_uuid())));

        Timers::advance(
            &mut tasks,
            Some(TimeSubscriptionCmd::CancelEveryXInterval(ref_uuid()).into()),
            clock.now_instant(),
        );
        assert!(tasks.is_empty());
    }
}
