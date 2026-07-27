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
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "TheadPool needs to have at least one thread");
        let mut handles = Vec::with_capacity(capacity);
        let (tx, rx) = channel::<Box<dyn FnOnce() + Send>>();
        let rx = Arc::new(Mutex::new(rx));

        for i in 0..capacity {
            let job_rx_thread = rx.clone();
            let builder = thread::Builder::new().name(format!("thread_{i}"));
            let handle = builder
                .spawn(move || {
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
                })
                .unwrap();
            handles.push(handle);
        }
        ThreadPool {
            sender: Some(tx),
            handles,
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
        drop(self.sender.take());

        for (i, handle) in self.handles.drain(..).enumerate() {
            debug!("Waiting for thread {i} to end...");
            if handle.join().is_err() {
                warn!("Thread {i} panicked.");
            }
        }
    }
}
