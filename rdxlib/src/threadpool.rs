use crate::error::InitError;
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{Sender, channel},
    },
    thread,
};
use tracing::{debug, error, warn};

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
        let (tx, rx) = channel::<Box<dyn FnOnce() + Send>>();
        let rx = Arc::new(Mutex::new(rx));

        for i in 0..capacity {
            let job_rx_thread = rx.clone();
            let builder = thread::Builder::new().name(format!("thread_{i}"));
            let spawned = builder.spawn(move || {
                loop {
                    let received = job_rx_thread.lock().unwrap().recv();
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
        if let Some(sender) = &self.sender &&
            let Err(error) = sender.send(job) {
            error!("actions channel tx for threadpool broke: {:?}", error);
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop_workers(self.sender.take(), self.handles.drain(..).collect());
    }
}
