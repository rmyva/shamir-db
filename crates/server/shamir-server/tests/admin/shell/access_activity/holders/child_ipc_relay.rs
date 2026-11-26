//! Test-only transparent IPC relay that records complete payload frames.
use std::sync::{Arc, Mutex};

use shamir_transport_ipc::IpcListener;
use shamir_transport_tcp::framing::{read_frame, write_frame, MAX_FRAME_SIZE_DEFAULT};
use tokio::io::{split, AsyncWriteExt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Direction {
    ChildToServer,
    ServerToChild,
}

pub(super) type RecordedFrame = (Direction, Vec<u8>);

/// Accept an already-bound child-facing endpoint, connect to the fixture,
/// and record frames unchanged until either pump ends. Whichever pump ends
/// first (normally child EOF) aborts its sibling, dropping every held half of
/// both connections by construction. On Unix, dropping the halves closes the
/// sockets so the peer sees EOF; on Windows, dropping the NamedPipeClient and
/// NamedPipeServer handles closes the pipe handles, so the peer observes
/// disconnect rather than a graceful EOF.
pub(super) async fn pump_relay(
    mut listener: IpcListener,
    server_endpoint: String,
) -> Vec<RecordedFrame> {
    let child = listener
        .accept()
        .await
        .expect("accept child IPC connection");
    let server = shamir_transport_ipc::connect(&server_endpoint)
        .await
        .expect("connect relay to fixture IPC server");
    let (child_read, child_write) = split(child);
    let (server_read, server_write) = split(server);
    let frames = Arc::new(Mutex::new(Vec::new()));

    // A `JoinSet` ends BOTH pumps by construction: whichever finishes first
    // (either direction) lets the coordinator shut the set down, and dropping
    // this future (an outer timeout) aborts both pumps instead of detaching them.
    let mut pumps = tokio::task::JoinSet::new();

    let child_frames = Arc::clone(&frames);
    pumps.spawn(async move {
        let (mut reader, mut writer) = (child_read, server_write);
        while let Ok(payload) = read_frame(&mut reader, MAX_FRAME_SIZE_DEFAULT).await {
            child_frames
                .lock()
                .expect("relay frame lock")
                .push((Direction::ChildToServer, payload.clone()));
            if write_frame(&mut writer, &payload).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    let server_frames = Arc::clone(&frames);
    pumps.spawn(async move {
        let (mut reader, mut writer) = (server_read, child_write);
        while let Ok(payload) = read_frame(&mut reader, MAX_FRAME_SIZE_DEFAULT).await {
            server_frames
                .lock()
                .expect("relay frame lock")
                .push((Direction::ServerToChild, payload.clone()));
            if write_frame(&mut writer, &payload).await.is_err() {
                break;
            }
        }
        let _ = writer.shutdown().await;
    });

    // The first pump to end (normally child EOF; a closed server side ends the
    // other direction) decides: abort and join the sibling so every half of
    // both connections is dropped before the recording is taken.
    let _ = pumps.join_next().await;
    pumps.shutdown().await;
    Arc::try_unwrap(frames)
        .expect("relay frame tasks released recording")
        .into_inner()
        .expect("relay recording lock")
}
