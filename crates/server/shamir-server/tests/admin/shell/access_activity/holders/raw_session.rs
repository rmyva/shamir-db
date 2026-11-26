use crate::shell_tests::access_activity_raw_scram::{login as scram_login, roundtrip};
use crate::shell_tests::harness::{ShellFixture, DB_NAME};
use shamir_query_types::batch::BatchResponse;
use shamir_server::db_handler::{DbRequest, DbResponse};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use zeroize::Zeroizing;

pub(in crate::shell_tests) const STEP_BOUND: Duration = Duration::from_secs(120);
pub(in crate::shell_tests) const FRAME_BOUND: Duration = Duration::from_secs(10);
pub(in crate::shell_tests) type TlsRead = ReadHalf<TlsStream<TcpStream>>;
pub(in crate::shell_tests) type TlsWrite = WriteHalf<TlsStream<TcpStream>>;
pub(in crate::shell_tests) fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}
#[derive(Clone)]
pub(in crate::shell_tests) struct RawSession {
    pub(in crate::shell_tests) state: Arc<tokio::sync::Mutex<RawState>>,
    pub(in crate::shell_tests) deadline: Instant,
}
pub(in crate::shell_tests) struct RawState {
    pub(in crate::shell_tests) sid: [u8; 32],
    pub(in crate::shell_tests) next_rid: u32,
    pub(in crate::shell_tests) read: TlsRead,
    pub(in crate::shell_tests) write: TlsWrite,
}
impl RawSession {
    pub(in crate::shell_tests) fn ask(
        &self,
        fixture: &ShellFixture,
        req: DbRequest,
    ) -> (Self, DbResponse) {
        let state = self.state.clone();
        let cap = remaining(self.deadline).min(STEP_BOUND).min(FRAME_BOUND);
        let response = fixture.rt.run_timeout(cap, async move {
            tokio::time::timeout(cap, async move {
                let mut state = state.lock().await;
                exchange(&mut state, &req).await
            })
            .await
            .expect("raw request deadline")
        });
        (self.clone(), response)
    }
}
pub(in crate::shell_tests) fn login(
    fixture: &ShellFixture,
    username: &str,
    password: &[u8],
    deadline: Instant,
) -> RawSession {
    let addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(password.to_vec());
    let username = username.to_owned();
    // Construct the timer inside the runtime context.
    let (sid, read, write, next_rid) =
        fixture
            .rt
            .run_timeout(remaining(deadline).min(STEP_BOUND), async move {
                tokio::time::timeout(
                    remaining(deadline).min(STEP_BOUND),
                    scram_login(addr, &username, &password, pin),
                )
                .await
                .expect("raw SCRAM login deadline")
            });
    RawSession {
        state: Arc::new(tokio::sync::Mutex::new(RawState {
            sid,
            next_rid,
            read,
            write,
        })),
        deadline,
    }
}
pub(in crate::shell_tests) async fn exchange(
    session: &mut RawState,
    req: &DbRequest,
) -> DbResponse {
    roundtrip(
        req,
        session.sid,
        &mut session.next_rid,
        &mut session.write,
        &mut session.read,
    )
    .await
}
pub(in crate::shell_tests) fn batch_reply(response: DbResponse, context: &str) -> BatchResponse {
    match response {
        DbResponse::Batch { response } | DbResponse::TxBatch { response } => response,
        DbResponse::Error { code, .. } => panic!("{context}: server replied `{code}`"),
        _ => panic!("{context}: unexpected reply shape"),
    }
}
pub(in crate::shell_tests) fn rollback_tx(
    fixture: &ShellFixture,
    session: RawSession,
    tx_handle: u64,
) -> RawSession {
    let request = DbRequest::TxRollback {
        db: DB_NAME.into(),
        tx_handle,
    };
    let (session, reply) = session.ask(fixture, request);
    match reply {
        DbResponse::TxRolledBack {
            tx_handle: rolled_back,
        } => assert_eq!(
            rolled_back, tx_handle,
            "session A rolls back the handle it opened"
        ),
        DbResponse::Error { code, .. } => {
            panic!("session A rolls back its transaction: server replied `{code}`")
        }
        _ => panic!("session A rolls back its transaction: unexpected reply shape"),
    }
    session
}
pub(in crate::shell_tests) fn close_session(fixture: &ShellFixture, session: RawSession) {
    let cap = remaining(session.deadline).min(FRAME_BOUND);
    fixture.rt.run_timeout(cap, async move {
        tokio::time::timeout(cap, async move {
            let mut state = session.state.lock().await;
            state.write.shutdown().await
        })
        .await
        .expect("session A close deadline")
        .expect("session A close");
    });
}
