use crate::error::InitError;
use std::sync::Condvar;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::{Receiver, SendError, TryRecvError};
use std::{
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
    ended: bool,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Slot {
            latest: None,
            ended: false,
        }
    }
}

impl<T> Slot<T> {
    fn is_content_available(&self) -> bool {
        matches!(self.latest, Some(_)) || self.ended
    }
}

struct OneSlotCore<T> {
    mutex: Mutex<Slot<T>>,
    cvar: Condvar,
}

pub struct OneSlotSender<T> {
    core: Arc<OneSlotCore<T>>,
}

impl<T: Send> OneSlotSender<T> {
    pub fn send(&self, new_value: T) -> Result<(), SendError<T>> {
        match self.core.mutex.lock() {
            Ok(mut guard) => {
                guard.latest = Some(new_value);
                drop(guard);
                self.core.cvar.notify_all();
                Ok(())
            }
            Err(_) => Err(SendError(new_value)),
        }
    }
}

impl<T> Drop for OneSlotSender<T> {
    fn drop(&mut self) {
        _ = self.core.mutex.lock().and_then(|mut guard| {
            guard.ended = true;
            drop(guard);
            self.core.cvar.notify_all();
            Ok(())
        });
    }
}

pub struct OneSlotReceiver<T> {
    core: Arc<OneSlotCore<T>>,
}

impl<T> Clone for OneSlotReceiver<T> {
    fn clone(&self) -> Self {
        OneSlotReceiver {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> OneSlotReceiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        let guard = self.core.mutex.lock().map_err(|_| RecvError)?;
        let mut guard = self
            .core
            .cvar
            .wait_while(guard, |s| !s.is_content_available())
            .map_err(|_| RecvError)?;

        guard
            .latest
            .take()
            .map_or_else(|| Err(RecvError), |v| Ok(v))
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut guard = self.core.mutex.lock().map_err(|_| RecvError)?;
        guard.latest.take().map_or_else(
            || {
                if guard.ended {
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
    use std::sync::mpsc;
    use std::sync::mpsc::TryRecvError;
    use std::thread;
    use std::time::Duration;

    const TIMEOUT: Duration = Duration::from_secs(1);
    const WAIT: Duration = Duration::from_millis(100);

    fn execute_with_timeout<J, P, R>(timeout: Duration, job: J, prepare: P) -> R
    where
        J: FnOnce() -> R + Send + 'static,
        P: FnOnce() + Send + 'static,
        R: Send + 'static,
    {
        let (result_sender, result_receiver) = mpsc::channel::<R>();
        thread::spawn(move || {
            thread::spawn(move || _ = result_sender.send(job()));

            prepare();
        });
        result_receiver
            .recv_timeout(timeout)
            .expect("Result is never received")
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
    fn sent_items_should_awake_on_receive_and_iter_ends() {
        let (sender, receiver) = one_slot_channel();
        let value1 = 1;
        let value2 = 2;

        let results: Vec<i32> = execute_with_timeout(
            TIMEOUT,
            move || receiver.collect(),
            move || {
                thread::sleep(WAIT);
                sender.send(value1).expect("Send should not fail");
                thread::sleep(WAIT);
                sender.send(value2).expect("Send should not fail");
                thread::sleep(WAIT);
                drop(sender);
            },
        );

        assert_eq!(results, vec![value1, value2]);
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
        assert!(
            result2.is_err(),
            "Second receive intent should return error"
        );
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
    fn sent_item_should_be_received_one_receiver() {
        let (sender, receiver) = one_slot_channel::<i32>();
        let receiver2 = receiver.clone();
        let value1 = 1;
        let value2 = 2;

        let result = execute_with_timeout(
            TIMEOUT,
            move || {
                [receiver.try_recv(), receiver2.try_recv()]
            },
            move || {
                sender.send(value1).expect("Send should not fail");
                sender.send(value2).expect("Send should not fail");
                thread::sleep(WAIT);
            },
        );

        assert_eq!(result[0], Ok(value2));
        assert_eq!(result[1], Err(TryRecvError::Empty));
    }
}

pub fn shared_channel<T: Send>() -> (Sender<T>, SharedReceiver<T>) {
    let (sender, receiver) = channel();
    (sender, SharedReceiver(Arc::new(Mutex::new(receiver))))
}

pub struct SharedReceiver<T: Send>(Arc<Mutex<Receiver<T>>>);

impl<T: Send> SharedReceiver<T> {
    pub fn recv(&self) -> Result<T, RecvError> {
        self.0.lock().unwrap().recv()
    }
}

impl<T: Send> Iterator for SharedReceiver<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = self.0.lock().unwrap();
        guard.recv().ok()
    }
}

impl<T: Send> Clone for SharedReceiver<T> {
    fn clone(&self) -> Self {
        SharedReceiver(self.0.clone())
    }
}

pub trait JobsDispatcher {
    fn work_on(&self, job: Box<dyn FnOnce() + Send + 'static>);
}

pub struct ThreadPool {
    sender: Option<Sender<Box<dyn FnOnce() + Send>>>,
    handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(capacity: usize) -> Result<Self, InitError> {
        if capacity == 0 {
            return Err(InitError::InvalidCapacity);
        }
        let mut handles = Vec::with_capacity(capacity);
        let (tx, rx) = shared_channel::<Box<dyn FnOnce() + Send>>();

        for i in 0..capacity {
            let job_rx_thread = rx.clone();
            let builder = thread::Builder::new().name(format!("thread_{i}"));
            let spawned = builder.spawn(move || {
                loop {
                    let received = job_rx_thread.recv();
                    match received {
                        Ok(job) => {
                            job();
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

fn drop_workers(
    sender: Option<Sender<Box<dyn FnOnce() + Send>>>,
    handles: Vec<thread::JoinHandle<()>>,
) {
    drop(sender);

    for (i, handle) in handles.into_iter().enumerate() {
        debug!("Waiting for thread {i} to end...");
        if handle.join().is_err() {
            warn!("Thread {i} panicked.");
        }
    }
}

impl JobsDispatcher for ThreadPool {
    fn work_on(&self, job: Box<dyn FnOnce() + Send + 'static>) {
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
