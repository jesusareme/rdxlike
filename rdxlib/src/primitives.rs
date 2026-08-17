//! Concurrency building blocks the runtime is built on.
//!
//! These have been building exercises to familiarize myself with the inner working details of
//! low level constructs such as those here present: thread pools, one slot channels,
//! and multiple consumers channels
//!
//! Poisoning errors have been ignored through these constructs as logic executed while
//! holding a lock should never panic save for a std library bug. There are two exceptions to this,
//! [`OneSlotReceiver::recv()`] and [`OneSlotReceiver::try_recv()`], which can panic if the contained
//! value drop panics on `take()` call. We would lose latest value on that call but OneSlot
//! invariants are not affected.

use crate::error::InitError;
use std::panic::UnwindSafe;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::{Receiver, SendError, TryRecvError};
use std::sync::{Condvar, MutexGuard, PoisonError};
use std::{
    panic,
    sync::{
        Arc, Mutex,
        mpsc::{Sender, channel},
    },
    thread,
};
use tracing::{debug, error, warn};

#[derive(PartialEq)]
struct Slot<T> {
    latest: Option<T>,
    senders: usize,
    receivers: usize,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Slot {
            latest: None,
            senders: 1,
            receivers: 1,
        }
    }
}

impl<T> Slot<T> {
    fn is_content_available(&self) -> bool {
        self.latest.is_some() || self.senders == 0
    }
}

struct OneSlotCore<T> {
    mutex: Mutex<Slot<T>>,
    cvar: Condvar,
}

fn lock_ignoring_poisoning<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<T> OneSlotCore<T> {
    fn get_guard(&self) -> MutexGuard<'_, Slot<T>> {
        lock_ignoring_poisoning(&self.mutex)
    }
}

/// Sending end of a [`one_slot_channel`].
/// `OneSlotSender` implements `Clone` so can have multiple producers.
pub struct OneSlotSender<T> {
    core: Arc<OneSlotCore<T>>,
}

impl<T> Clone for OneSlotSender<T> {
    fn clone(&self) -> Self {
        let mut guard = self.core.get_guard();
        guard.senders += 1;
        OneSlotSender {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T: Send> OneSlotSender<T> {
    /// Stores a value, overwriting whatever was waiting there unread. Never blocks.
    ///
    /// # Errors
    /// Will return `Err` if all receiving end objects are dropped.
    pub fn send(&self, new_value: T) -> Result<(), SendError<T>> {
        let mut guard = self.core.get_guard();
        if guard.receivers == 0 {
            Err(SendError(new_value))
        } else {
            guard.latest = Some(new_value);
            drop(guard);
            self.core.cvar.notify_all();
            Ok(())
        }
    }
}

impl<T> Drop for OneSlotSender<T> {
    fn drop(&mut self) {
        let mut guard = self.core.get_guard();
        guard.senders -= 1;
        drop(guard);
        self.core.cvar.notify_all();
    }
}

/// Receiving end of a [`one_slot_channel`], also usable as an [`Iterator`].
/// `OneSlotReceiver` implements `Clone` so we can have multiple consumers.
pub struct OneSlotReceiver<T> {
    core: Arc<OneSlotCore<T>>,
}

impl<T> Clone for OneSlotReceiver<T> {
    fn clone(&self) -> Self {
        let mut guard = self.core.get_guard();
        guard.receivers += 1;
        OneSlotReceiver {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> Drop for OneSlotReceiver<T> {
    fn drop(&mut self) {
        let mut guard = self.core.get_guard();
        guard.receivers -= 1;
    }
}

impl<T> OneSlotReceiver<T> {
    /// Blocks until a value is available and returns it.
    ///
    /// # Errors
    /// Will return `Err` if no sender left and no value available.
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            let guard = self.core.get_guard();
            let mut guard = self
                .core
                .cvar
                .wait_while(guard, |s| !s.is_content_available())
                .unwrap_or_else(PoisonError::into_inner);

            if let Some(available) = guard.latest.take() {
                return Ok(available)
            }
            if guard.senders == 0 {
                return Err(RecvError)
            }
        }
    }

    /// Takes the stored value if there is one, without blocking.
    ///
    /// # Errors
    /// Will return `TryRecvError::Empty` if no value is available, or
    /// `TryRecvError::Disconnected` if no sender left and no value available.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut guard = self.core.get_guard();
        guard.latest.take().map_or_else(
            || {
                if guard.senders == 0 {
                    Err(TryRecvError::Disconnected)
                } else {
                    Err(TryRecvError::Empty)
                }
            },
            |v| Ok(v),
        )
    }
}

impl<T> Iterator for OneSlotReceiver<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

/// Creates a channel that remembers only the most recent value sent. Each send value can be received
/// by only one of the many potentially available receivers.
///
/// Meant for state updates where only the newest value is worth checking. For instance,
/// generating products from latest state on a potentially lagging thread, where only latest
/// state at every moment makes sense to be processed.
#[must_use]
pub fn one_slot_channel<T: Send>() -> (OneSlotSender<T>, OneSlotReceiver<T>) {
    let core = OneSlotCore {
        mutex: Mutex::new(Slot::default()),
        cvar: Condvar::new(),
    };
    let core = Arc::new(core);
    (
        OneSlotSender {
            core: Arc::clone(&core),
        },
        OneSlotReceiver { core },
    )
}

#[cfg(test)]
mod tests_one_slot_channel {
    use super::one_slot_channel;
    use crate::primitives::tests_one_slot_channel::TimedOutTestState::{Finished, Ready};
    use std::sync::mpsc::{RecvError, TryRecvError};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_secs(1);

    enum TimedOutTestState<R> {
        Preparing,
        Ready,
        Finished(R),
    }
    impl<R> TimedOutTestState<R> {
        pub fn is_finished(&self) -> bool {
            matches!(self, Finished(_))
        }

        pub fn is_ready(&self) -> bool {
            matches!(self, Ready)
        }
    }

    fn execute_with_timeout<J, P, R>(timeout: Duration, job: J, prepare: P) -> R
    where
        J: FnOnce() -> R + Send + 'static,
        P: FnOnce() + Send + 'static,
        R: Clone + Send + 'static,
    {
        let sync = Arc::new((Mutex::new(TimedOutTestState::Preparing), Condvar::new()));
        let sync_remote = sync.clone();

        thread::spawn(move || {
            let mut guard = sync_remote
                .1
                .wait_while(sync_remote.0.lock().unwrap(), |s| !s.is_ready())
                .expect("we should not err under normal test conditions");
            let result = job();
            *guard = Finished(result);
            sync_remote.1.notify_all();
        });

        prepare();
        *sync.0.lock().unwrap() = Ready;
        sync.1.notify_all();
        match sync
            .1
            .wait_timeout_while(sync.0.lock().unwrap(), timeout, |s| !s.is_finished())
        {
            Ok((guard, _result)) => {
                if let Finished(ref value) = *guard {
                    value.clone()
                } else {
                    panic!("Test time-out");
                }
            }
            Err(_) => panic!("Test job panicked"),
        }
    }

    #[test]
    fn sent_item_should_be_received() {
        let (sender, receiver) = one_slot_channel();
        let value = 1;

        let result = execute_with_timeout(
            TIMEOUT,
            move || receiver.recv(),
            move || {
                sender.send(value).expect("Send should not fail");
            },
        );

        assert_eq!(result.expect("Value should be received"), value);
    }

    #[test]
    fn sent_items_should_receive_latest_one_only() {
        let (sender, receiver) = one_slot_channel();
        let value1 = 1;
        let value2 = 2;

        sender.send(value1).expect("Send should not fail");
        sender.send(value2).expect("Send should not fail");

        let result = execute_with_timeout(TIMEOUT, move || receiver.recv(), || {});

        assert_eq!(result.expect("Latest value should be received"), value2);
    }

    #[test]
    fn iter_should_retrieve_results_and_end_when_no_sender_exists() {
        let (sender, mut receiver) = one_slot_channel();
        let value1 = 1;
        let value2 = 2;
        let value3 = 3;

        let sender2 = sender.clone();

        let mut results = vec![];

        sender.send(value1).expect("Send should not fail here");
        results.push(receiver.next().unwrap());
        sender.send(value2).expect("Send should not fail here");
        results.push(receiver.next().unwrap());

        drop(sender);

        sender2.send(value3).expect("Send should not fail here");
        results.push(receiver.next().unwrap());

        drop(sender2);

        assert_eq!(receiver.next(), None);

        assert_eq!(results, vec![value1, value2, value3]);
    }

    #[test]
    fn dropped_sender_should_receive_pending_value_before_sender_dropped() {
        let (sender, receiver) = one_slot_channel();
        let value = 1;

        sender.send(value).expect("Send should not fail");
        drop(sender);

        let (result1, result2) =
            execute_with_timeout(TIMEOUT, move || (receiver.recv(), receiver.recv()), || {});

        assert_eq!(result1, Ok(value));
        assert_eq!(result2, Err(RecvError));
    }

    #[test]
    fn sent_item_should_receive_try_then_empty() {
        let (sender, receiver) = one_slot_channel();
        let value = 1;

        sender.send(value).expect("Send should not fail");
        assert_eq!(receiver.try_recv().expect("Receive should not fail"), value);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn not_sent_item_should_err_empty() {
        let (_sender, receiver) = one_slot_channel::<i32>();

        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn dropped_sender_should_err_disconnected() {
        let (_, receiver) = one_slot_channel::<i32>();

        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn sent_items_should_receive_last_one_then_empty() {
        let (sender, receiver) = one_slot_channel();
        let value1 = 1;
        let value2 = 2;

        sender.send(value1).expect("Send should not fail");
        sender.send(value2).expect("Send should not fail");

        assert_eq!(receiver.try_recv(), Ok(value2));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn cloned_senders_should_disconnect_only_after_the_last_one_drops() {
        let (sender, receiver) = one_slot_channel::<i32>();
        let sender2 = sender.clone();
        let sender3 = sender2.clone();
        let value = 1;

        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        sender3.send(value).expect("Send should not fail");
        drop(sender3);

        assert_eq!(receiver.try_recv(), Ok(value));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        let result = execute_with_timeout(
            TIMEOUT,
            move || receiver.recv(),
            move || {
                drop(sender2);
            },
        );

        assert_eq!(result, Err(RecvError));
    }

    #[test]
    fn sent_item_should_be_received_one_receiver() {
        let (sender, receiver) = one_slot_channel::<i32>();
        let receiver2 = receiver.clone();
        let value1 = 1;
        let value2 = 2;

        let _sender_keep_alive = sender.clone();

        let result = execute_with_timeout(
            TIMEOUT,
            move || [receiver.try_recv(), receiver2.try_recv()],
            move || {
                sender.send(value1).expect("Send should not fail");
                sender.send(value2).expect("Send should not fail");
            },
        );

        assert_eq!(result[0], Ok(value2));
        assert_eq!(result[1], Err(TryRecvError::Empty));
    }
}

/// Creates a multiple producers / multiple consumers channel.
///
/// Each value goes to exactly one of the receivers.
#[must_use]
pub fn shared_channel<T: Send>() -> (Sender<T>, SharedReceiver<T>) {
    let (sender, receiver) = channel();
    (sender, SharedReceiver(Arc::new(Mutex::new(receiver))))
}

/// A cloneable receiver whose clones all pull from the same queue.
pub struct SharedReceiver<T: Send>(Arc<Mutex<Receiver<T>>>);

impl<T: Send> SharedReceiver<T> {
    /// Blocks until this receiver gets a value, or the channel disconnects.
    ///
    /// # Errors
    /// Will return `Err` if sender is disconnected and no message will ever be received here again.
    pub fn recv(&self) -> Result<T, RecvError> {
        lock_ignoring_poisoning(&self.0).recv()
    }
}

impl<T: Send> Iterator for SharedReceiver<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

pub(crate) type BoxedThreadPoolJob = Box<dyn FnOnce() + Send + UnwindSafe>;

impl<T: Send> Clone for SharedReceiver<T> {
    fn clone(&self) -> Self {
        SharedReceiver(self.0.clone())
    }
}

/// Represents a construct able to process jobs off the calling thread.
pub trait JobsDispatcher {
    /// Takes ownership of a job and schedules it to run.
    ///
    /// Must not block the caller under any circumstance.
    fn work_on(&self, job: BoxedThreadPoolJob);
}

/// A fixed set of worker threads pulling jobs from one shared queue.
///
/// Basic implementation of `JobsDispatcher`.
///
/// Workers catch panics so a failing job takes down neither its thread nor the pool.
/// Dropping the pool closes the queue and blocks until the workers have drained the jobs
/// still queued.
pub struct ThreadPool {
    sender: Option<Sender<BoxedThreadPoolJob>>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadPool {
    /// Starts a pool of `capacity` worker threads.
    ///
    /// # Errors
    /// Will return `Err` if `capacity` is 0, or we have an OS level error while spawning threads.
    pub fn new(capacity: usize) -> Result<Self, InitError> {
        if capacity == 0 {
            return Err(InitError::InvalidCapacity);
        }
        let mut handles = Vec::with_capacity(capacity);
        let (tx, rx) = shared_channel::<BoxedThreadPoolJob>();

        for i in 0..capacity {
            let job_rx_thread = rx.clone();
            let builder = thread::Builder::new().name(format!("thread_{i}"));
            let spawned = builder.spawn(move || {
                loop {
                    match job_rx_thread.recv() {
                        Ok(job) => {
                            if panic::catch_unwind(job).is_err() {
                                error!("Job in thread {} panicked", i);
                            }
                        }
                        Err(error) => {
                            error!(
                                "job channel in thread_{} broke, exiting thread: {:?}",
                                i, error
                            );
                            break;
                        }
                    }
                }
            });

            match spawned {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    error!("Unable to spawn thread_{i}, shutting down partial pool: {error:?}");
                    drop_workers(Some(tx), handles);
                    return Err(InitError::ThreadSpawn(error));
                }
            }
        }

        Ok(ThreadPool {
            sender: Some(tx),
            handles,
        })
    }
}

fn drop_workers(sender: Option<Sender<BoxedThreadPoolJob>>, handles: Vec<thread::JoinHandle<()>>) {
    drop(sender);

    for (i, handle) in handles.into_iter().enumerate() {
        debug!("Waiting for thread {i} to end...");
        if handle.join().is_err() {
            warn!("Thread {i} panicked.");
        }
    }
}

impl JobsDispatcher for ThreadPool {
    fn work_on(&self, job: BoxedThreadPoolJob) {
        if let Some(sender) = &self.sender
            && let Err(error) = sender.send(job)
        {
            error!("actions channel tx for threadpool broke: {:?}", error);
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop_workers(self.sender.take(), self.handles.drain(..).collect());
    }
}
