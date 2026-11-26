//! Signed shell-child IPC capture regression.
//!
//! Mutant killed: emitting the child's signing key/tag to its output or
//! profile artifacts (for example, a signing-key debug print).
use std::time::{Duration, Instant};

use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine as _;
use serde::Deserialize;
use shamir_query_builder::ddl;
use shamir_query_types::hmac as canon;
use shamir_transport_ipc::IpcListener;
use tempfile::TempDir;
use zeroize::Zeroizing;

use super::child_ipc_relay::{pump_relay, Direction};
use crate::common::ipc_test_addr;
use crate::shell_tests::harness::{
    boot, overwrite_profile, profile_text, run_shell, seed_catalogue, ShellFixture, DB_NAME,
    PLAIN_TABLE, REPO_NAME,
};
use crate::shell_tests::interactive::{assert_human_field, InteractiveChild};
use crate::shell_tests::secure_fixture::scan_operator_home;
use crate::shell_tests::state_data_common::data;

const TOTAL_BOUND: Duration = Duration::from_secs(180);

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn pinned_ipc_profile(fixture: &ShellFixture, address: &str) -> String {
    profile_text("ipc", address, None, Some(DB_NAME))
        .replace("pin: <PIN>\n", &format!("pin: {}\n", fixture.pin_hex()))
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[derive(Deserialize)]
struct AuthOkWire {
    #[serde(with = "serde_bytes")]
    server_signature: Vec<u8>,
    #[serde(with = "serde_bytes")]
    server_pub_key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    identity_sig: Vec<u8>,
    #[serde(with = "serde_bytes")]
    session_id: Vec<u8>,
    expires_at_ns: u64,
    #[serde(default, with = "serde_bytes")]
    resumption_ticket: Vec<u8>,
    #[serde(default)]
    resumption_expires_at_ns: u64,
    #[serde(default)]
    _server_query_version: u8,
    #[serde(default)]
    _rotation_in_progress: Option<shamir_connect::common::wire::RotationInProgressWire>,
    #[serde(default)]
    _kdf_upgrade_required: Option<bool>,
}

/// Real IPC server + one shell child; the relay's positive tag witness ensures
/// the captured session belongs to this child, not a separate admin client.
#[test]
fn signed_shell_ipc_chmod_keeps_its_own_session_material_out_of_child_artifacts() {
    let start = Instant::now();
    let deadline = start + TOTAL_BOUND;
    let fixture =
        boot("signed_shell_ipc_chmod_keeps_its_own_session_material_out_of_child_artifacts");
    let relay_temp = TempDir::new().expect("relay endpoint tempdir");
    let relay_endpoint = ipc_test_addr(&relay_temp);
    #[cfg(target_os = "macos")]
    assert!(
        relay_endpoint.len() < 104,
        "relay IPC socket path exceeds the 104-byte sun_path floor: {} bytes",
        relay_endpoint.len(),
    );
    overwrite_profile(&fixture, &pinned_ipc_profile(&fixture, &relay_endpoint));
    seed_catalogue(&fixture);
    let command = InteractiveChild::child_command(&fixture);
    let secret = Zeroizing::new(fixture.canary.as_str().to_owned());
    let home = fixture.profile_home.path().to_path_buf();
    let endpoint = relay_endpoint.clone();
    let server_endpoint = fixture.ipc_addr.clone();

    let (run, key_hex, key_b64, tag, sid_hex) =
        fixture.rt.run_timeout(remaining(deadline), async move {
            let listener = IpcListener::bind(&endpoint)
                .await
                .expect("bind child IPC relay before spawning shell");
            let mut child = InteractiveChild::spawn(command, secret, home).await;
            // A `JoinSet` aborts the relay when this job is dropped by the
            // outer deadline instead of detaching it.
            let mut relay = tokio::task::JoinSet::new();
            relay.spawn(pump_relay(listener, server_endpoint));
            let response = child
                .command_until(
                    &format!("chmod {DB_NAME}/{REPO_NAME}/{PLAIN_TABLE} 0751 --yes"),
                    "chmod",
                    "mode",
                )
                .await;
            assert_human_field(&response, "mode", "0751");
            let run = child.finish().await;
            let frames = relay
                .join_next()
                .await
                .expect("IPC relay task present")
                .expect("IPC relay task joins");

            assert!(
                frames
                    .iter()
                    .any(|(direction, _)| *direction == Direction::ChildToServer),
                "relay saw no child-to-server frames"
            );
            assert!(
                frames
                    .iter()
                    .any(|(direction, _)| *direction == Direction::ServerToChild),
                "relay saw no server-to-child frames"
            );

            let sid = frames
                .iter()
                .filter(|(direction, _)| *direction == Direction::ServerToChild)
                .find_map(|(_, frame)| {
                    rmp_serde::from_slice::<AuthOkWire>(frame).ok().map(|wire| {
                        let _trailing_auth_fields = (
                            wire.server_signature,
                            wire.server_pub_key,
                            wire.identity_sig,
                            wire.expires_at_ns,
                            wire.resumption_ticket,
                            wire.resumption_expires_at_ns,
                            wire._server_query_version,
                            wire._rotation_in_progress,
                            wire._kdf_upgrade_required,
                        );
                        wire.session_id
                    })
                })
                .expect("server auth-ok carries child session id");
            let sid: [u8; 32] = sid.try_into().expect("auth-ok session id is 32 bytes");
            let key = canon::derive_session_hmac_key(&sid);
            let resource = ddl::res::table(DB_NAME, REPO_NAME, PLAIN_TABLE);
            let unsigned = ddl::chmod(resource, 0o751)
                .hmac("0".repeat(64))
                .build()
                .expect("typed unsigned chmod with placeholder");
            let canonical =
                canon::canonical_batch_op(DB_NAME, &unsigned).expect("canonical chmod operation");
            let tag = canon::compute_tag_hex(&key, &canonical);
            let key_hex = hex::encode(key);
            let key_b64 = [STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD]
                .map(|engine| engine.encode(key));
            let sid_hex = hex::encode(sid);
            assert!(
                frames.iter().any(|(direction, frame)| {
                    *direction == Direction::ChildToServer && contains_bytes(frame, tag.as_bytes())
                }),
                "positive control: captured child request contains its HMAC tag"
            );
            (run, key_hex, key_b64, tag, sid_hex)
        });

    let key_variants = std::iter::once(key_hex.as_str()).chain(key_b64.iter().map(String::as_str));
    for key_variant in key_variants {
        assert!(
            !contains_bytes(&run.stdout, key_variant.as_bytes()),
            "session key leaked to child stdout"
        );
        assert!(
            !contains_bytes(&run.stderr, key_variant.as_bytes()),
            "session key leaked to child stderr"
        );
    }
    assert!(
        !contains_bytes(&run.stdout, tag.as_bytes()),
        "signature tag leaked to child stdout"
    );
    assert!(
        !contains_bytes(&run.stderr, tag.as_bytes()),
        "signature tag leaked to child stderr"
    );
    // The signed child's own profile home is scanned BEFORE the profile is
    // rewritten for the readback: the rewrite truncates the same leaf and
    // would erase any key or tag the child left in it.
    let mut home_needles = vec![key_hex.as_str(), tag.as_str(), sid_hex.as_str()];
    home_needles.extend(key_b64.iter().map(String::as_str));
    scan_operator_home(&fixture, &home_needles);
    overwrite_profile(&fixture, &pinned_ipc_profile(&fixture, &fixture.ipc_addr));
    let path = format!("{DB_NAME}/{REPO_NAME}/{PLAIN_TABLE}");
    let readback = run_shell(&fixture, &["rights", "show", &path]);
    assert_eq!(data(&readback, "rights")["mode"], "0751");
}
