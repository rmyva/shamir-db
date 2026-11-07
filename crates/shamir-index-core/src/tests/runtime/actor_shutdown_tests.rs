use super::IndexActor;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn shutdown_reports_applier_panic_and_preserves_payload() {
    let actor = IndexActor::<(), ()>::spawn((), |_, _| async {
        panic!("actor shutdown regression panic");
    });
    actor.submit(()).await.unwrap();
    let reported = Arc::new(Mutex::new(None));
    let sink = reported.clone();
    actor
        .shutdown_with_report(move |error| {
            *sink.lock().unwrap() = Some(error);
        })
        .await;
    let error = reported
        .lock()
        .unwrap()
        .take()
        .expect("join failure reported");
    assert!(error.is_panic());
    let payload = error.into_panic();
    assert_eq!(
        payload.downcast_ref::<&str>(),
        Some(&"actor shutdown regression panic")
    );
}

#[tokio::test]
async fn shutdown_does_not_report_normal_drain() {
    let actor = IndexActor::<(), ()>::spawn((), |_, _| async {});
    actor.submit(()).await.unwrap();
    actor
        .shutdown_with_report(|error| panic!("unexpected join failure: {error}"))
        .await;
}
