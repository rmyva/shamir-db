//! Secure-listener extension, typed ACL/token setup, and shared child runner.

use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;

use shamir_connect::common::kdf_params::KdfParams;
use shamir_db::access::{principal64, Actor, ResourceMeta, ResourcePath};
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl;
use shamir_query_builder::token_admin::CreateApiToken;
use shamir_query_types::admin::ResourceRef;
use shamir_query_types::hmac as canon;
use shamir_server::config::HttpListenerSettings;
use shamir_server::config::ListenerConfig;
use shamir_server::config::ListenerKind;
use shamir_server::config::ProfileKind;
use shamir_server::user_directory::{derive_scram_record, FjallUserDirectory};
use shamir_sync::format_stamp::{check_data_dir, EpochPolicy, IgnoreSet, StampMode};
use tokio::io::AsyncWriteExt;
use zeroize::Zeroizing;

use super::boot_with;
use super::bound;
use super::connect_tls_owned;
use super::contains;
use super::overwrite_profile;
use super::profile_env;
use super::profile_home_path;
use super::server_command;
use super::unique_canary;
use super::ShellFixture;
use super::ShellRun;
use super::TestRuntime;
use super::ADMIN;
use super::BOOT_BOUND;
use super::PROFILE_NAME;
use super::ROUTING_DB;
use super::SHELL_BOUND;

pub(in crate::shell_tests) struct SecureFixture {
    pub(in crate::shell_tests) shell: ShellFixture,
    pub(in crate::shell_tests) wss_addr: SocketAddr,
    pub(in crate::shell_tests) http_addr: SocketAddr,
    /// A real native WSS listener serving a path the SDK never dials
    /// (`/decoy/v1`): pointing a native-WSS profile at it provokes an
    /// upgrade rejection BEFORE SCRAM (review item 3b).
    pub(in crate::shell_tests) decoy_wss_addr: SocketAddr,
    pub(in crate::shell_tests) ca_path: PathBuf,
}

/// Boot WSS SCRAM and HTTPS REST/token WSS alongside the PR1 listeners.
/// READ-ONLY path of the fixture's held data directory (e.g. verifying
/// the on-disk format stamp against what `status` reports). A descendant
/// helper of `harness`: it reaches the private `_data_dir` field from
/// this child module so `harness.rs` itself stays at its line cap.
pub(in crate::shell_tests) fn data_dir(fixture: &ShellFixture) -> &std::path::Path {
    fixture._data_dir.path()
}

pub(in crate::shell_tests) fn boot_secure(name: &str) -> SecureFixture {
    boot_secure_with(name, false, |_| None)
}

pub(in crate::shell_tests) fn boot_secure_root_list(name: &str) -> SecureFixture {
    boot_secure_with(name, true, |_| None)
}

/// Boot with the two named principals minted OFFLINE into this fixture's
/// private user directory, `owner` owning `ResourcePath::Root` (mode 0755).
/// Returns the fixture plus each principal's per-run zeroizing secret.
///
/// The wire cannot seed any of this: `ResourceRef` has no `Root` variant, so
/// `Manage(Root)` (ownership-only) is unreachable except through the offline
/// seam — and the owner's `principal64` is minted by the directory, hence
/// the principals have to be created before the Root ACL is written.
pub(in crate::shell_tests) fn boot_secure_root_owner(
    name: &str,
    owner: &'static str,
    other: &'static str,
) -> (SecureFixture, Zeroizing<String>, Zeroizing<String>) {
    let owner_secret = unique_canary();
    let other_secret = unique_canary();
    let owner_seed = owner_secret.clone();
    let other_seed = other_secret.clone();
    let secure = boot_secure_with(name, true, move |data_dir| {
        Some(seed_probe_principals(
            data_dir,
            (owner, owner_seed.as_str()),
            (other, other_seed.as_str()),
        ))
    });
    (secure, owner_secret, other_secret)
}

fn boot_secure_with(
    name: &str,
    root_list: bool,
    seed_root_owner: impl FnOnce(&Path) -> Option<u64> + Send + 'static,
) -> SecureFixture {
    let (shell, bound_addrs, cert_path) = boot_with(
        name,
        root_list,
        Default::default(),
        |config| {
            config.listeners.extend([
                ListenerConfig {
                    kind: ListenerKind::Ws,
                    addr: "127.0.0.1:0".into(),
                    profile: Some(ProfileKind::TlsExporter),
                    path: Some("/shamir/v1".into()),
                    kdf_override: None,
                    browser_origin_allowlist: vec![],
                    http: Default::default(),
                },
                ListenerConfig {
                    kind: ListenerKind::Http,
                    addr: "127.0.0.1:0".into(),
                    profile: None,
                    path: None,
                    kdf_override: None,
                    browser_origin_allowlist: vec![],
                    http: HttpListenerSettings {
                        tls: Some(true),
                        ..Default::default()
                    },
                },
                ListenerConfig {
                    kind: ListenerKind::Ws,
                    addr: "127.0.0.1:0".into(),
                    profile: Some(ProfileKind::TlsExporter),
                    path: Some("/decoy/v1".into()),
                    kdf_override: None,
                    browser_origin_allowlist: vec![],
                    http: Default::default(),
                },
            ]);
        },
        seed_root_owner,
    );
    let [.., Some(wss_addr), Some(http_addr), Some(decoy_wss_addr)] = bound_addrs.as_slice() else {
        panic!("secure fixture listeners did not bind");
    };
    let ca_path = profile_home_path(&shell).join("fixture-ca.pem");
    // This copies only the public certificate. The child gets no path into
    // the server data directory, and verifies the certificate normally.
    std::fs::copy(cert_path, &ca_path).expect("copy fixture public certificate");
    shell.guard.note("wss_addr", wss_addr.to_string());
    shell.guard.note("http_addr", http_addr.to_string());
    shell
        .guard
        .note("decoy_wss_addr", decoy_wss_addr.to_string());
    SecureFixture {
        shell,
        wss_addr: *wss_addr,
        http_addr: *http_addr,
        decoy_wss_addr: *decoy_wss_addr,
        ca_path,
    }
}

/// Stamp the fixture directory while it is still EMPTY: the stamp has to be
/// its first file, so this runs before any offline store is created. Split
/// from [`seed_root_acl`] because the pre-launch hook mints the Root owner
/// BETWEEN the two.
pub(super) fn stamp_data_dir(data_dir: &Path) {
    check_data_dir(
        data_dir,
        StampMode::Writer,
        &IgnoreSet::none(),
        &EpochPolicy::compiled(),
    )
    .expect("stamp empty Root List fixture directory");
}

/// Root cannot be addressed by wire ACL operations. Seed it before launch
/// in this fixture's private directory, using the launcher's system path.
/// `owner` replaces the shipped `Actor::System` owner with a
/// directory-minted principal64 (the shipped owner stays for `None`); `mode`
/// remains 0755 so non-owner `Execute` traversal still works.
pub(super) fn seed_root_acl(data_dir: &Path, owner: Option<u64>) {
    let system_path = data_dir.join("shamir_db_meta.redb");
    // Join the offline runtime before starting the launcher.
    TestRuntime::new().run(bound(BOOT_BOUND, async move {
        let db = ShamirDb::init(SystemStoreConfig::Fjall(system_path))
            .await
            .expect("open Root List fixture system store");
        db.set_resource_meta(
            &ResourcePath::Root,
            &ResourceMeta {
                owner: owner.map_or(Actor::System, Actor::User),
                group: None,
                mode: 0o755,
            },
        )
        .await
        .expect("seed Root List ACL");
        db.flush_all_waiting().await.expect("flush Root List ACL");
    }));
}

/// KDF cost of the offline-seeded probe principals: the spec floor, so the
/// two Argon2id derivations stay cheap while remaining real SCRAM records.
const PROBE_KDF: KdfParams = KdfParams {
    memory_kb: 19_456,
    time: 2,
    parallelism: 1,
    argon2_version: 0x13,
};

/// Mint `owner` and `other` OFFLINE into the fixture's private user
/// directory and return `principal64` of the first (the Root owner) — Root
/// stores `principal64(user_id)` as its owner, and the directory mints
/// `user_id`, so the principal must exist before the Root ACL is written.
/// Both records start with no roles and `superuser = false` (neither the
/// wire nor this seam grants either).
fn seed_probe_principals(data_dir: &Path, owner: (&str, &str), other: (&str, &str)) -> u64 {
    let users_path = data_dir.join("users");
    let owner = (owner.0.to_string(), owner.1.to_string());
    let other = (other.0.to_string(), other.1.to_string());
    TestRuntime::new().run(bound(BOOT_BOUND, async move {
        let directory = FjallUserDirectory::open(&users_path).expect("open fixture user directory");
        let mut owner_id = None;
        for (name, password) in [owner, other] {
            let record = derive_scram_record(password, PROBE_KDF)
                .await
                .expect("derive probe SCRAM record");
            let user_id = directory
                .insert_with_scope(name, record, None)
                .expect("seed probe principal");
            if owner_id.is_none() {
                owner_id = Some(user_id);
            }
        }
        // The launcher re-opens this store: release it before returning.
        drop(directory);
        principal64(owner_id.expect("the owner principal is seeded first"))
    }))
}

#[derive(serde::Serialize)]
struct SecureProfile<'a> {
    transport: &'a str,
    address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<&'static str>,
    sni: &'static str,
    database: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ca_path: Option<&'a Path>,
}

/// Write a pinned SCRAM profile (`None`) or a token profile with a trusted PEM.
pub(in crate::shell_tests) fn write_secure_profile(
    fixture: &ShellFixture,
    transport: &str,
    addr: SocketAddr,
    database: &str,
    ca_path: Option<&Path>,
) -> PathBuf {
    let profile = SecureProfile {
        transport,
        address: addr.to_string(),
        username: ca_path.is_none().then_some(ADMIN),
        sni: "localhost",
        database,
        pin: ca_path.is_none().then(|| fixture.pin_hex()),
        ca_path,
    };
    let text = ktav::to_string(&profile).expect("serialize secure fixture profile");
    assert!(ktav::parse_strict(&text).is_ok(), "strict secure profile");
    overwrite_profile(fixture, &text)
}

/// Issue a real fresh token over the pinned admin SCRAM connection. The
/// caller chooses the owner, databases, mode and channels on `request`.
pub(in crate::shell_tests) fn provision_token(
    fixture: &ShellFixture,
    request: CreateApiToken,
) -> Zeroizing<String> {
    fixture.guard.mark("seed: API token");
    let addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    fixture.rt.run(bound(BOOT_BOUND, async move {
        let client = connect_tls_owned(addr, pin, password)
            .await
            .expect("verified admin connection for token issuance");
        // Do not format a response or issuance error that could carry a secret.
        let created = client
            .create_api_token(request)
            .await
            .unwrap_or_else(|_| panic!("fixture token issuance failed"));
        let secret = Zeroizing::new(created.token.reveal().to_owned());
        client.close().await;
        secret
    }))
}

/// Revoke an issued token by its public ID over pinned admin SCRAM.
pub(in crate::shell_tests) fn revoke_fixture_token(fixture: &ShellFixture, token_id: &str) {
    fixture.guard.mark("seed: revoke API token");
    let addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    let token_id = token_id.to_owned();
    fixture.rt.run(bound(BOOT_BOUND, async move {
        let client = connect_tls_owned(addr, pin, password)
            .await
            .expect("verified admin connection for token revocation");
        client
            .revoke_api_token(&token_id)
            .await
            .expect("revoke fixture token");
        client.close().await;
    }));
}

/// Set a resource's mode through an authenticated, signed typed operation.
/// The routing database must already exist; Root is not a wire resource.
pub(in crate::shell_tests) fn set_acl_mode(
    fixture: &ShellFixture,
    resource: ResourceRef,
    mode: u16,
) {
    set_acl_mode_timeout(fixture, resource, mode, BOOT_BOUND)
}

pub(in crate::shell_tests) fn set_acl_mode_timeout(
    fixture: &ShellFixture,
    resource: ResourceRef,
    mode: u16,
    timeout: std::time::Duration,
) {
    fixture.guard.mark("seed: token catalogue ACL");
    let addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    fixture.rt.run(bound(timeout, async move {
        let client = connect_tls_owned(addr, pin, password)
            .await
            .expect("verified admin connection for ACL setup");
        let unsigned = ddl::chmod(resource.clone(), mode)
            .hmac("0".repeat(64))
            .build()
            .expect("typed chmod with placeholder tag");
        let key = canon::derive_session_hmac_key(&client.session_id());
        let bytes =
            canon::canonical_batch_op(ROUTING_DB, &unsigned).expect("chmod canonical bytes");
        let op = ddl::chmod(resource, mode)
            .hmac(canon::compute_tag_hex(&key, &bytes))
            .build()
            .expect("signed typed chmod");
        let mut batch = Batch::new();
        batch.id("shell_fixture_acl");
        batch.chmod("mode", op);
        let result = client
            .execute(ROUTING_DB, batch.build())
            .await
            .expect("apply ACL");
        let result = result.results.get("mode").expect("chmod result alias");
        assert_eq!(result.records.len(), 1, "chmod returned one record");
        assert_eq!(
            result.records[0].get_value_i64("mode"),
            Some(i64::from(mode))
        );
        client.close().await;
    }));
}

/// The meaningful suffix of a secret: the token's bearer tail after the
/// final dot, else the last 8 bytes (a leaked prefix alone still shows the
/// secret shape; the tail is the high-entropy part an echo would carry).
pub(in crate::shell_tests) fn secret_suffix(secret: &str) -> &str {
    if let Some((_, tail)) = secret.rsplit_once('.') {
        if !tail.is_empty() {
            return tail;
        }
    }
    let mut cut = secret.len().saturating_sub(8);
    while !secret.is_char_boundary(cut) {
        cut += 1;
    }
    &secret[cut..]
}

/// Recursive post-run operator-home invariant (review item 6b): NO entry
/// name may look like history, and EVERY file the shell created under the
/// operator home is byte-scanned for every needle, whatever its name.
pub(in crate::shell_tests) fn scan_operator_home(fixture: &ShellFixture, needles: &[&str]) {
    let mut dirs = vec![profile_home_path(fixture)];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).expect("scan operator home") {
            let entry = entry.expect("operator home entry");
            let kind = entry.file_type().expect("operator home entry type");
            assert!(
                !entry
                    .file_name()
                    .to_string_lossy()
                    .to_ascii_lowercase()
                    .contains("history"),
                "unexpected shell history"
            );
            if kind.is_dir() {
                dirs.push(entry.path());
            } else {
                assert!(kind.is_file(), "unexpected non-file in operator home");
                let bytes = std::fs::read(entry.path()).expect("read operator file");
                for needle in needles {
                    assert!(
                        !contains(&bytes, needle),
                        "secret leaked in operator home file"
                    );
                }
            }
        }
    }
}

/// Run the token shell and scan both streams and the operator home for
/// the token (including its secret suffix) and for unexpected history.
pub(in crate::shell_tests) fn run_token_shell(
    fixture: &ShellFixture,
    token: &Zeroizing<String>,
    args: &[&str],
) -> ShellRun {
    assert!(!token.is_empty(), "fixture token must not be empty");
    let run = run_with_secret(fixture, token.clone(), true, args);
    let suffix = secret_suffix(token);
    assert!(
        !run.contains_canary(token) && !run.contains_canary(suffix),
        "token leaked in shell output"
    );
    scan_operator_home(fixture, &[token.as_str(), suffix]);
    run
}

/// Run the real binary with a zeroizing, stdin-only secret. Arguments with
/// an explicit `--` are used as supplied; otherwise they are the command.
pub(in crate::shell_tests) fn run_with_secret(
    fixture: &ShellFixture,
    secret: Zeroizing<String>,
    token_auth: bool,
    args: &[&str],
) -> ShellRun {
    run_secret_process(fixture, Some(secret), token_auth, None, &[], args)
}

/// Generalized child shell run (review items 5 and 6c): optional stdin
/// secret, optional child-only working directory and extra environment
/// (never applied to this process), otherwise as [`run_with_secret`].
pub(in crate::shell_tests) fn run_secret_process(
    fixture: &ShellFixture,
    secret: Option<Zeroizing<String>>,
    token_auth: bool,
    current_dir: Option<&Path>,
    extra_env: &[(&'static str, String)],
    args: &[&str],
) -> ShellRun {
    run_secret_process_timeout(
        fixture,
        secret,
        token_auth,
        current_dir,
        extra_env,
        args,
        SHELL_BOUND,
    )
}

/// Narrow timeout override for shell tests that share one absolute deadline.
pub(in crate::shell_tests) fn run_secret_process_timeout(
    fixture: &ShellFixture,
    secret: Option<Zeroizing<String>>,
    token_auth: bool,
    current_dir: Option<&Path>,
    extra_env: &[(&'static str, String)],
    args: &[&str],
    timeout: std::time::Duration,
) -> ShellRun {
    run_secret_process_timeout_scanned(
        fixture,
        secret,
        token_auth,
        current_dir,
        args,
        timeout,
        (extra_env, &[]),
    )
}

/// Timeout-bounded shell process with caller-provided additional scan needles.
pub(in crate::shell_tests) fn run_secret_process_timeout_scanned(
    fixture: &ShellFixture,
    secret: Option<Zeroizing<String>>,
    token_auth: bool,
    current_dir: Option<&Path>,
    args: &[&str],
    timeout: std::time::Duration,
    checks: (&[(&'static str, String)], &[&str]),
) -> ShellRun {
    let (extra_env, scan_needles) = checks;
    fixture.guard.mark("shell: spawn");
    let (env_key, env_value) = profile_env(fixture);
    let mut argv: Vec<String> = vec!["shell".into(), "--profile".into(), PROFILE_NAME.into()];
    if secret.is_some() {
        argv.push("--secret-stdin".into());
    }
    if token_auth {
        argv.extend(["--auth".into(), "token".into()]);
    }
    if args.contains(&"--") {
        // Caller-supplied shape: use exactly as given, no inserted or
        // duplicated delimiter.
        argv.extend(args.iter().map(ToString::to_string));
    } else if !args.is_empty() {
        argv.push("--".into());
        argv.extend(args.iter().map(ToString::to_string));
    }
    // An EMPTY `args` with a secret is a bare interactive probe: appending
    // `--` there would flip the parser into command-consuming mode with
    // nothing after it.
    // Env-source secrets must not leak into the assembled argv either.
    assert!(
        !argv.iter().any(|arg| {
            secret.as_deref().is_some_and(|value| arg.contains(value))
                || extra_env.iter().any(|(_, value)| arg.contains(value))
                || arg.contains(fixture.canary.as_str())
                || scan_needles.iter().any(|needle| arg.contains(needle))
        }),
        "fixture secret leaked into the assembled argv",
    );
    // The child's whole home is the isolated profile home: no state can
    // land in the real user home even under a regression.
    let home_dir = profile_home_path(fixture);
    let current_dir = current_dir.map(Path::to_path_buf);
    let extra_env = extra_env.to_vec();

    let outcome = fixture.rt.run_timeout(timeout, async move {
        let mut command = server_command();
        command
            .args(&argv)
            .env(env_key, env_value)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.env("HOME", &home_dir);
        #[cfg(windows)]
        command.env("USERPROFILE", &home_dir);
        command.current_dir(&home_dir);
        if let Some(dir) = &current_dir {
            command.current_dir(dir);
        }
        for (key, value) in &extra_env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawn shell child");
        // stdin-only; close the pipe for EOF. An early-exit refusal closes
        // the pipe first, so only `BrokenPipe` is tolerated downstream.
        if let Some(mut stdin) = child.stdin.take() {
            match &secret {
                Some(secret) => {
                    write_secret_stdin(&mut stdin, secret.as_bytes()).await;
                }
                None => drop(stdin),
            }
        }
        let output = child
            .wait_with_output()
            .await
            .expect("shell child waits with output");
        (
            output.status.code().unwrap_or(-1),
            output.stdout,
            output.stderr,
        )
    });
    fixture.guard.note("shell exit", outcome.0.to_string());
    ShellRun {
        exit_code: outcome.0,
        stdout: outcome.1,
        stderr: outcome.2,
    }
}

/// One child command for THIS test binary running ONLY the named test
/// (`--exact ... --nocapture`), so the spawn never recurses across the
/// whole suite: every secret-bearing and home variable stripped, the
/// fixture's isolated profile home applied as home and working directory,
/// both output pipes captured (the child's stdout legitimately carries
/// libtest framing), stdin piped for the caller to stage. `gate` selects
/// the child entry (set to "1" by the caller).
pub(in crate::shell_tests) fn scripted_child_command(
    fixture: &ShellFixture,
    gate: &'static str,
    exact_test: &str,
) -> tokio::process::Command {
    let mut command =
        tokio::process::Command::new(std::env::current_exe().expect("test binary path"));
    command
        .args(["--exact", exact_test, "--nocapture"])
        .env(gate, "1");
    for var in [
        "SHAMIR_SHELL_PASSWORD",
        "SHAMIR_SHELL_TOKEN",
        "XDG_CONFIG_HOME",
        "HOME",
        "LOCALAPPDATA",
        "USERPROFILE",
    ] {
        command.env_remove(var);
    }
    let (env_key, env_value) = profile_env(fixture);
    let home_dir = profile_home_path(fixture);
    command.env(env_key, env_value);
    #[cfg(unix)]
    command.env("HOME", &home_dir);
    #[cfg(windows)]
    command.env("USERPROFILE", &home_dir);
    command
        .current_dir(&home_dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command
}

/// Write one full stdin payload (secret line + script lines) to the child,
/// then flush+shutdown. `ErrorKind::BrokenPipe` is accepted (early-exit
/// refusal); every other error is a fixture bug and panics. The payload is
/// `Zeroizing`-scrubbed.
async fn write_secret_stdin(stdin: &mut tokio::process::ChildStdin, secret: &[u8]) {
    // Preallocated (secret + LF), zeroized so the assembled line is
    // scrubbed on drop.
    let mut line = Zeroizing::new(Vec::with_capacity(secret.len() + 1));
    line.extend_from_slice(secret);
    line.push(b'\n');
    if let Err(error) = stdin.write_all(line.as_slice()).await {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            panic!("write secret to shell stdin: {error}");
        }
        return; // child exited before reading the secret — fine for refusals
    }
    if let Err(error) = stdin.flush().await {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            panic!("flush secret to shell stdin: {error}");
        }
        return;
    }
    if let Err(error) = stdin.shutdown().await {
        if error.kind() != std::io::ErrorKind::BrokenPipe {
            panic!("close shell stdin: {error}");
        }
    }
}
