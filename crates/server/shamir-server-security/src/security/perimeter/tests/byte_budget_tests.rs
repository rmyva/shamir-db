use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use super::ByteBudget;

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[test]
fn contended_acquires_are_counted_until_dropped_or_admitted() {
    let budget = ByteBudget::new(Some(10));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(full) = pin!(budget.acquire(10)).poll(&mut cx) else {
        panic!("acquire must be ready when the budget is empty");
    };
    assert_eq!(budget.waiters(), 0);

    let mut remaining = pin!(budget.acquire(4));
    assert!(remaining.as_mut().poll(&mut cx).is_pending());
    assert_eq!(budget.waiters(), 1);
    {
        let mut dropped = pin!(budget.acquire(4));
        assert!(dropped.as_mut().poll(&mut cx).is_pending());
        assert_eq!(budget.waiters(), 2);
    }
    assert_eq!(budget.waiters(), 1);
    assert_eq!(budget.used(), 10);

    let woke = Arc::new(WakeFlag::default());
    let waker = Waker::from(Arc::clone(&woke));
    let mut cx = Context::from_waker(&waker);
    assert!(remaining.as_mut().poll(&mut cx).is_pending());
    assert_eq!(budget.waiters(), 1);
    assert!(!woke.0.load(Ordering::Relaxed));
    drop(full);
    assert!(
        woke.0.load(Ordering::Relaxed),
        "release must wake the waiter"
    );

    let Poll::Ready(guard) = remaining.as_mut().poll(&mut cx) else {
        panic!("woken acquire must be ready after release");
    };
    assert_eq!(budget.waiters(), 0);
    assert_eq!(budget.used(), 4);
    drop(guard);
    assert_eq!(budget.used(), 0);
}

#[test]
fn unbounded_acquires_are_ready_without_accounting() {
    let budget = ByteBudget::unbounded();
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(first) = pin!(budget.acquire(usize::MAX)).poll(&mut cx) else {
        panic!("unbounded acquire must be ready");
    };
    let Poll::Ready(second) = pin!(budget.acquire(usize::MAX)).poll(&mut cx) else {
        panic!("unbounded acquire must be ready while another guard is held");
    };
    assert_eq!(budget.cap(), None);
    assert_eq!(budget.waiters(), 0);
    assert_eq!(budget.used(), 0);
    drop((first, second));
    assert_eq!(budget.used(), 0);
}
