//! Deterministic tests for synchronous publication/termination serialization.
use crate::tx::cold_reconcile::lifecycle::PubTermInterlock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn publication_runs_callback_and_termination_is_sticky() {
    let lock = PubTermInterlock::new();
    let value = AtomicUsize::new(0);
    assert_eq!(lock.publish(|| value.fetch_add(1, Ordering::SeqCst)), Ok(0));
    assert_eq!(value.load(Ordering::SeqCst), 1);
    assert!(lock.terminate());
    assert!(!lock.terminate());
    assert!(lock.is_terminated());
    assert!(lock
        .publish(|| value.fetch_add(1, Ordering::SeqCst))
        .is_err());
    assert_eq!(value.load(Ordering::SeqCst), 1);
}

#[test]
fn termination_waits_until_inflight_publication_callback_finishes() {
    assert!(run_inflight_termination(true).publication_completed_at_return());
}

#[test]
fn nonwaiting_negative_control_returns_before_publication_finishes() {
    let protected = run_inflight_termination(true);
    let unprotected = run_inflight_termination(false);
    assert!(protected.publication_completed_at_return());
    assert!(!unprotected.publication_completed_at_return());
}

struct TerminationSample {
    publication_at_return: usize,
    terminated: bool,
    repeated_termination: bool,
    later_publication_rejected: bool,
}

impl TerminationSample {
    fn publication_completed_at_return(&self) -> bool {
        self.publication_at_return == 42
    }
}

fn run_inflight_termination(protected: bool) -> TerminationSample {
    let lock = Arc::new(PubTermInterlock::new());
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let published = Arc::new(AtomicUsize::new(0));
    let (worker_lock, worker_entered, worker_release, worker_value) = (
        Arc::clone(&lock),
        Arc::clone(&entered),
        Arc::clone(&release),
        Arc::clone(&published),
    );
    let worker = thread::spawn(move || {
        worker_lock.publish(|| {
            worker_entered.wait();
            worker_release.wait();
            worker_value.store(42, Ordering::SeqCst);
        })
    });
    entered.wait();
    let term_lock = Arc::clone(&lock);
    let term_value = Arc::clone(&published);
    let (sample_tx, sample_rx) = std::sync::mpsc::channel();
    let terminator = thread::spawn(move || {
        let terminated = if protected {
            term_lock.terminate_after_observed_contention_for_test(|| sample_tx.send(()).unwrap())
        } else {
            let terminated = term_lock.nonwaiting_termination_for_test();
            let at_return = term_value.load(Ordering::SeqCst);
            sample_tx.send(()).unwrap();
            let repeated_termination = term_lock.terminate();
            let later_publication_rejected = term_lock.publish(|| ()).is_err();
            return TerminationSample {
                publication_at_return: at_return,
                terminated,
                repeated_termination,
                later_publication_rejected,
            };
        };
        let at_return = term_value.load(Ordering::SeqCst);
        let repeated_termination = term_lock.terminate();
        let later_publication_rejected = term_lock.publish(|| ()).is_err();
        TerminationSample {
            publication_at_return: at_return,
            terminated,
            repeated_termination,
            later_publication_rejected,
        }
    });
    sample_rx.recv().unwrap();
    release.wait();
    worker.join().unwrap().unwrap();
    let sample = terminator.join().unwrap();
    assert!(sample.terminated);
    assert!(!sample.repeated_termination);
    assert!(sample.later_publication_rejected);
    assert!(lock.is_terminated());
    assert_eq!(published.load(Ordering::SeqCst), 42);
    sample
}

#[test]
fn termination_first_prevents_publication_callback() {
    let lock = PubTermInterlock::new();
    assert!(lock.terminate());
    let called = AtomicUsize::new(0);
    assert!(lock
        .publish(|| called.fetch_add(1, Ordering::SeqCst))
        .is_err());
    assert_eq!(called.load(Ordering::SeqCst), 0);
}
