use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use crate::shell_tests::access_activity_holders::teardown_deadline::teardown;
use crate::shell_tests::runtime::TestRuntime;

/// A synchronously blocked spawned task freezes `block_on` before its job receiver
/// can observe all senders dropped. The bounded stop notification must therefore
/// time out; an unbounded receive mutant hangs this test past the runner's kill.
#[test]
fn shutdown_until_reports_worker_stuck_before_deadline() {
    let mut runtime = TestRuntime::new();
    let (sender, receiver) = mpsc::channel::<()>();
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    runtime.run_timeout(Duration::from_secs(5), async move {
        tokio::spawn(async move {
            let _ = entered_tx.send(());
            let _ = receiver.recv();
        });
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("blocking task must enter within 5 s");

    let deadline = Instant::now() + Duration::from_secs(2);
    let result = runtime.shutdown_until(deadline);
    assert_eq!(result, Err("runtime worker did not stop before deadline"));
    assert!(Instant::now() < deadline + Duration::from_secs(5));

    drop(sender);
}

/// The healthy stop path is the positive control for the stuck-worker oracle.
#[test]
fn shutdown_until_stops_healthy_worker() {
    let mut runtime = TestRuntime::new();
    assert_eq!(
        runtime.shutdown_until(Instant::now() + Duration::from_secs(10)),
        Ok(())
    );
}

/// A held mutex must produce the typed deadline error rather than report success.
/// Removing the lock-inside-timeout fix now panics at the runtime bound, failing
/// the exact `Err("holder mutex teardown deadline")` assertion.
#[test]
fn teardown_reports_holder_deadline_and_succeeds_when_released() {
    let mut runtime = TestRuntime::new();
    let holder = Arc::new(tokio::sync::Mutex::new(()));
    let (locked_tx, locked_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    runtime.run_timeout(Duration::from_secs(5), {
        let holder = Arc::clone(&holder);
        async move {
            tokio::spawn(async move {
                let _guard = holder.lock().await;
                let _ = locked_tx.send(());
                let _ = release_rx.await;
            });
        }
    });
    locked_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("holder task must acquire the mutex within 5 s");

    assert_eq!(
        teardown(
            &mut runtime,
            Arc::clone(&holder),
            Instant::now() + Duration::from_secs(2),
        ),
        Err("holder mutex teardown deadline"),
        "contended teardown must return the typed deadline error"
    );

    drop(release_tx);
    assert_eq!(
        teardown(
            &mut runtime,
            holder,
            Instant::now() + Duration::from_secs(30),
        ),
        Ok(())
    );
}
