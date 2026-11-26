//! Shared base fixture and helpers for the `shell` integration tests:
//! one [`TestRuntime`], one real in-process [`ServerLauncher`] server (both
//! TLS-TCP and IPC listeners, persistent store in a held TempDir), a
//! per-test-unique zeroizing canary secret and an operator-shaped profile
//! home. The shell under test is the REAL production binary, driven as an
//! operator would; the secret travels stdin-only. Assertions never embed
//! raw child output or argv — canary scans are [`ShellRun::contains_canary`].
//! Handshakes, not sleeps: every await is hang-bounded and
//! [`FlakeGuard::with_watchdog`] dumps the timeline.

use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use zeroize::Zeroizing;

use base64::Engine as _;
use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt as _;
#[cfg(windows)]
use std::os::windows::io::FromRawHandle as _;
#[cfg(windows)]
use windows_sys::Win32::Foundation as win_foundation;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem as win_fs;

use shamir_client::Client;
use shamir_client::ConnectOptions;
use shamir_private_dir::create_private_dir;
use shamir_query_builder::batch::Batch;
use shamir_query_builder::ddl;
use shamir_query_builder::BatchRequest;
use shamir_query_types::hmac as canon;
use shamir_server::config::Config;
use shamir_server::config::ListenerConfig;
use shamir_server::config::ListenerKind;
use shamir_server::config::ProfileKind;
use shamir_server::config::SecurityConfig;
use shamir_server::db_handler::NodeMode;
use shamir_server::server::BootError;
use shamir_server::server::BootstrapMode;
use shamir_server::server::ServerHandle;
use shamir_server::server::ServerLauncher;
use shamir_test_diag::FlakeGuard;

use crate::common;

// The fixture stayed in `secure/` when this file moved to `core/`; a
// `#[path]` in a non-`mod.rs` file resolves against THIS file's directory.
#[path = "../secure/secure_fixture.rs"]
pub(super) mod secure_fixture;

// ── Named constants ──────────────────────────────────────────────────

/// The binary under test, resolved by cargo for this integration target.
pub const SERVER_BIN: &str = env!("CARGO_BIN_EXE_shamir-server");

/// The single profile name every shell invocation in this suite uses.
pub const PROFILE_NAME: &str = "fixture";

/// Bootstrap admin username (`BootstrapMode::Password` default).
pub const ADMIN: &str = "admin";

/// Catalogue fixture names — distinct aliases so assertions are exact sets.
pub const DB_NAME: &str = "shelldb";
pub const REPO_NAME: &str = "mainrepo";
pub const PLAIN_TABLE: &str = "plainkv";
pub const INDEXED_TABLE: &str = "indexedkv";
pub const INDEX_NAME: &str = "by_score";

/// Table with a NONEMPTY schema so `table describe` / `table schema`
/// return real rule rows.
pub const SCHEMA_TABLE: &str = "schemakv";

/// Known function seeded from raw WASM (echo module, slice-2 ABI) so
/// `function list` returns a real entry without the wasm32 toolchain.
pub const FN_NAME: &str = "shellecho";

/// Index whose NAME carries TAB + ESC + ANSI SGR + BEL — the reflection
/// fixture. Identifiers are server-restricted to `[A-Za-z0-9_-]`, but
/// index names are data, so this is the carrier making ACTUAL returned
/// catalogue data carry raw control bytes the shell must escape.
pub const INDEX_ODD_NAME: &str = "odd\tindex\x1b[31m\x07";

/// Non-superuser account for the real-server denial test: NO roles, so
/// every `authorize_access`-gated catalogue op denies it over the wire.
pub const SHELL_USER: &str = "shelllimited";

/// Routing database the seeding batches ride on.
const ROUTING_DB: &str = "default";

/// Hard hang bounds — hang DETECTORS, not latency assertions.
const BOOT_BOUND: Duration = Duration::from_secs(120);
const SHELL_BOUND: Duration = Duration::from_secs(120);
/// Watchdog fires before any of the above, so the dump lands while alive.
const WATCHDOG_AFTER: Duration = Duration::from_secs(150);

/// Spec-floor Argon2 concurrency: enough parallel sessions for the SDK
/// probes plus the shell subprocess without admission stalls.
const FIXTURE_ARGON2_CONCURRENT_MAX: u32 = 64;

// ── TestRuntime ──────────────────────────────────────────────────────

pub use super::runtime::TestRuntime;

// ── Canary secret ────────────────────────────────────────────────────

/// A fresh, unique, zeroizing canary secret: the bootstrap admin password
/// AND the leak-scan needle (uniqueness from pid + wall-clock ns tag).
pub fn unique_canary() -> Zeroizing<String> {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tag = format!(
        "canary-{:x}-{:x}-{:x}@",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
        n,
    );
    Zeroizing::new(tag)
}

// ── ShellFixture ─────────────────────────────────────────────────────

/// The per-test fixture: one runtime, one real server (persistent store
/// in a held TempDir, graceful shutdown on drop), both transports bound,
/// a captured pin, a unique canary and an operator-shaped profile home.
pub struct ShellFixture {
    /// Diagnostic timeline; kept in the field list so the failure-only
    /// report covers the whole fixture lifetime.
    pub guard: FlakeGuard,
    /// The test's runtime — drive futures on it via [`TestRuntime::run`].
    pub rt: TestRuntime,
    /// Server data directory (persistent Fjall store), held for the whole
    /// fixture lifetime; kept private: alive-ness is the field's only job.
    _data_dir: TempDir,
    server: Option<ServerHandle>,
    /// TLS-TCP address the server actually bound.
    pub tls_addr: std::net::SocketAddr,
    /// Raw IPC endpoint the server actually bound (what a `transport: ipc`
    /// profile takes).
    pub ipc_addr: String,
    /// `SHA256(server_ed25519_pub_key)` captured out of band by a prior
    /// successful SDK connection to THIS fixture's server.
    pub pin: [u8; 32],
    /// Per-test unique canary: the admin password. Zeroized on drop.
    pub canary: Zeroizing<String>,
    /// Profile-home root; the profile directory lives under this root and
    /// every shell subprocess gets the matching env var pointed here.
    pub profile_home: TempDir,
}

impl ShellFixture {
    /// The 64-hex-digit pin string a profile carries.
    pub fn pin_hex(&self) -> String {
        hex::encode(self.pin)
    }

    /// Shut down the server under an absolute deadline. The job's own timeout is
    /// recomputed from the deadline when the job runs and an overrun PANICS the
    /// job (it is never discarded as a success); the caller-side bound carries a
    /// slack so that panic is the one reported. Callers on Drop paths must catch
    /// the resulting caller panic.
    pub fn shutdown_with_deadline(&mut self, deadline: Instant) {
        if let Some(server) = self.server.take() {
            let cap = deadline.saturating_duration_since(Instant::now());
            self.rt
                .run_timeout(cap + super::runtime::DEADLINE_SLACK, async move {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if tokio::time::timeout(remaining, server.shutdown())
                        .await
                        .is_err()
                    {
                        panic!("fixture server shutdown exceeded the common deadline");
                    }
                });
        }
    }
}

impl Drop for ShellFixture {
    fn drop(&mut self) {
        // Graceful server shutdown first (commit-loss window closed),
        // THEN the TempDirs drop; Drop must never panic.
        if let Some(server) = self.server.take() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.rt.run(bound(SHUTDOWN_BOUND, async move {
                    let _ = server.shutdown().await;
                }));
            }));
        }
    }
}

/// Boot the full fixture; the returned fixture owns everything and shuts
/// down gracefully on drop.
pub fn boot(name: &str) -> ShellFixture {
    boot_with(name, false, Default::default(), |_| {}, |_| None).0
}

/// Boot with a data directory and export root as sibling directories inside
/// the fixture TempDir. ExportRoot creates the private `export-root` leaf.
pub fn boot_export(name: &str) -> ShellFixture {
    boot_with(
        name,
        false,
        Default::default(),
        |config| {
            let fixture_dir = config.data_dir.clone();
            let data_dir = fixture_dir.join("data");
            std::fs::create_dir_all(&data_dir).expect("create nested fixture data dir");
            config.data_dir = data_dir;
            config.security.export_root = Some(fixture_dir.join("export-root"));
        },
        |_| None,
    )
    .0
}

/// Boot with an enabled scheduled backup. The data directory and the
/// existing private `scheduled-backups` destination are sibling directories
/// inside the fixture TempDir. A one-hour interval avoids a scheduler run
/// during this projection-only probe.
pub fn boot_scheduled_backup(name: &str) -> ShellFixture {
    boot_with(
        name,
        false,
        Default::default(),
        |config| {
            let fixture_dir = config.data_dir.clone();
            let data_dir = fixture_dir.join("data");
            std::fs::create_dir_all(&data_dir).expect("create nested fixture data dir");
            let destination = fixture_dir.join("scheduled-backups");
            create_private_dir(&destination).expect("create private scheduled-backup destination");
            config.data_dir = data_dir;
            config.backup = Some(shamir_server::config::BackupScheduleConfig {
                destination,
                interval: "1h".into(),
                window: None,
                run_on_start: false,
                retention: Default::default(),
            });
        },
        |_| None,
    )
    .0
}

pub fn boot_follower(name: &str) -> ShellFixture {
    boot_with(name, false, NodeMode::ReadOnly, |_| {}, |_| None).0
}

/// Extend the PR1 listener configuration and return the actual bind slots
/// (in configuration order) and the generated public certificate path.
///
/// `seed_root_owner` runs on THIS thread after the empty-directory stamp
/// and before the offline Root ACL write. It may mint principals in the
/// user directory and return the principal64 that must own
/// `ResourcePath::Root`: Root is not addressable by any wire ACL op, so a
/// non-bootstrap owner has to be in place before the launcher takes the
/// store lock.
fn boot_with(
    name: &str,
    root_list: bool,
    node_mode: NodeMode,
    extend: impl FnOnce(&mut Config) + Send + 'static,
    seed_root_owner: impl FnOnce(&Path) -> Option<u64> + Send + 'static,
) -> (ShellFixture, Vec<Option<SocketAddr>>, PathBuf) {
    let guard = FlakeGuard::with_watchdog(name, WATCHDOG_AFTER);
    let data_dir = TempDir::new().expect("fixture tempdir");
    if root_list {
        guard.mark("seed: stamp empty data dir");
        secure_fixture::stamp_data_dir(data_dir.path());
    }
    let root_owner = seed_root_owner(data_dir.path());
    assert!(
        root_owner.is_none() || root_list,
        "a pre-launch Root owner needs the offline Root ACL seed"
    );
    if root_list {
        guard.mark("seed: offline Root List ACL");
        secure_fixture::seed_root_acl(data_dir.path(), root_owner);
    }
    let rt = TestRuntime::new();
    let canary = unique_canary();

    // The `rt.run` futures are `Send + 'static`: clone the password out
    // of the canary BEFORE the `async move`.
    let boot_password = Zeroizing::new(canary.as_bytes().to_vec());

    guard.mark("boot: launch server");
    let (server, data_dir, tls_addr, ipc_addr, cert_path) = rt.run(bound(BOOT_BOUND, async move {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut config = fixture_config(&data_dir);
        extend(&mut config);
        let cert_path = config.tls.cert_path.clone();
        let lock_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        let server = loop {
            let launched = ServerLauncher {
                config: config.clone(),
                bootstrap: BootstrapMode::Password {
                    username: ADMIN.into(),
                    password: boot_password.clone(),
                },
                node_mode,
            }
            .launch()
            .await;
            match launched {
                Ok(server) => break server,
                // The offline Fjall engine may release its lock asynchronously.
                Err(BootError::ShamirDbInit(ref text))
                    if root_list
                        && text.contains("FjallError: Locked")
                        && tokio::time::Instant::now() < lock_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => panic!("fixture server boot: {error}"),
            }
        };
        let tls_addr = server
            .first_tls_exporter_addr()
            .expect("TLS listener bound");
        let ipc_addr = server
            .first_ipc_path()
            .expect("IPC listener bound")
            .to_owned();
        (server, data_dir, tls_addr, ipc_addr, cert_path)
    }));
    guard.note("tls_addr", tls_addr.to_string());
    guard.note("ipc_addr", ipc_addr.clone());

    #[cfg(target_os = "macos")]
    {
        // macOS $TMPDIR sits behind /var → /private/var with long names;
        // catch the 104-byte `sun_path` floor at boot, not at bind.
        assert!(
            ipc_addr.len() < 104,
            "macOS IPC socket path exceeds the 104-byte sun_path floor: {} bytes",
            ipc_addr.len(),
        );
    }

    // A separately zeroized copy for the pin capture, taken from the
    // retained canary (the boot copy moved into the launch job above).
    let pin_password = Zeroizing::new(canary.as_bytes().to_vec());

    // Out-of-band pin capture: TOFU connect, then a verified reconnect.
    guard.mark("boot: pin capture + verified reconnect");
    let pin = rt.run(bound(BOOT_BOUND, async move {
        let pin = match Client::connect(tofu_options(tls_addr, &pin_password)).await {
            Ok(client) => {
                let pin = client.server_pub_key_pin();
                client.close().await;
                pin
            }
            Err(error) => panic!("fixture TOFU connect failed: {error}"),
        };
        let mut pinned = tofu_options(tls_addr, &pin_password);
        pinned.accept_new_host = false;
        pinned.trusted_pin = Some(pin);
        let verified = Client::connect(pinned)
            .await
            .expect("fixture verified-pin reconnect");
        verified.ping().await.expect("fixture verified-pin ping");
        verified.close().await;
        pin
    }));
    guard.note("pin_hex", hex::encode(pin));

    // macOS: canonicalize so the env var carries the real /private/var
    // path, not the /var symlink.
    let profile_home = TempDir::new().expect("profile home tempdir");
    let bound_addrs = server.bound_addrs.clone();
    let fixture = ShellFixture {
        guard,
        rt,
        _data_dir: data_dir,
        server: Some(server),
        tls_addr,
        ipc_addr,
        pin,
        canary,
        profile_home,
    };
    (fixture, bound_addrs, cert_path)
}

/// Fixture config: persistent store, fast spec-floor KDF, Argon2 64,
/// generous auth-init rate, and BOTH PR1 transports (TLS-TCP and IPC
/// `plain`) bound by one launcher.
fn fixture_config(data_dir: &TempDir) -> Config {
    let mut config = common::make_test_config(data_dir, "127.0.0.1:0");
    config.argon2_concurrent_max = FIXTURE_ARGON2_CONCURRENT_MAX;
    config.security = SecurityConfig {
        auth_init_rate_per_second: 64,
        ..Default::default()
    };
    config.listeners.push(ListenerConfig {
        kind: ListenerKind::Unix,
        addr: common::ipc_test_addr(data_dir),
        profile: Some(ProfileKind::Plain),
        path: None,
        kdf_override: None,
        browser_origin_allowlist: vec![],
        http: Default::default(),
    });
    config
}

fn tofu_options(addr: std::net::SocketAddr, password: &Zeroizing<Vec<u8>>) -> ConnectOptions {
    ConnectOptions {
        addr,
        server_name: "localhost".to_string(),
        username: ADMIN.to_string(),
        password: password.clone(),
        accept_new_host: true,
        trusted_pin: None,
        connect_timeout: Some(BOOT_BOUND),
        handshake_timeout: Some(BOOT_BOUND),
        request_timeout: Some(SHELL_BOUND),
    }
}

/// Everything owned, so callers can await it inside a `Send + 'static`
/// [`TestRuntime::run`] job.
pub(super) async fn connect_tls_owned(
    addr: std::net::SocketAddr,
    pin: [u8; 32],
    password: Zeroizing<Vec<u8>>,
) -> Result<Client, shamir_client::ClientError> {
    let mut opts = tofu_options(addr, &password);
    opts.accept_new_host = false;
    opts.trusted_pin = Some(pin);
    Client::connect(opts).await
}

/// Absolute-bound wrapper: a hang detector around every awaited fixture
/// step (an elapse means a genuine hang).
async fn bound<T>(limit: Duration, future: impl std::future::Future<Output = T>) -> T {
    match tokio::time::timeout(limit, future).await {
        Ok(value) => value,
        Err(_) => panic!("fixture step exceeded its {limit:?} hang bound"),
    }
}

// ── Profile writing ──────────────────────────────────────────────────

/// The platform-default profile directory root under `profile_home`:
/// Unix `$XDG_CONFIG_HOME/shamir-shell/profiles`, Windows
/// `%LOCALAPPDATA%/ShamirDB/shell/profiles`.
fn profiles_dir(fixture: &ShellFixture) -> PathBuf {
    if cfg!(unix) {
        profile_home_path(fixture)
            .join("shamir-shell")
            .join("profiles")
    } else {
        profile_home_path(fixture)
            .join("ShamirDB")
            .join("shell")
            .join("profiles")
    }
}

/// The profile-home path as the shell subprocess must see it. macOS:
/// canonicalize out of the /var symlink; Windows: verbatim (`\\?\`
/// prefix from canonicalize is unwanted).
fn profile_home_path(fixture: &ShellFixture) -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        fixture
            .profile_home
            .path()
            .canonicalize()
            .unwrap_or_else(|_| fixture.profile_home.path().to_path_buf())
    }
    #[cfg(not(target_os = "macos"))]
    {
        fixture.profile_home.path().to_path_buf()
    }
}

/// The env mapping pointing the shell at [`profiles_dir`].
pub fn profile_env(fixture: &ShellFixture) -> (&'static str, String) {
    if cfg!(unix) {
        (
            "XDG_CONFIG_HOME",
            profile_home_path(fixture).display().to_string(),
        )
    } else {
        (
            "LOCALAPPDATA",
            profile_home_path(fixture).display().to_string(),
        )
    }
}

/// The strict-Ktav rendering of a profile `address`, via `ktav::to_string`
/// — the SAME parser the shell reads profiles with, never hand-rolled.
#[derive(serde::Serialize)]
struct ProfileAddress<'a> {
    address: &'a str,
}

fn ktav_address(address: &str) -> String {
    let rendered =
        ktav::to_string(&ProfileAddress { address }).expect("profile address always serializes");
    let value = rendered
        .strip_prefix("address: ")
        .expect("ktav renders a one-field struct as one pair");
    value.trim_end().to_owned()
}

/// Canonical PR1 profile text (strict Ktav, allowlisted fields only, no
/// secret field). Ktav round-trip is asserted here, so refusal tests fail
/// at the fixture, never inside the shell under test.
pub fn profile_text(
    transport: &str,
    address: &str,
    sni: Option<&str>,
    database: Option<&str>,
) -> String {
    let mut text = String::new();
    text.push_str(&format!("address: {}\n", ktav_address(address)));
    text.push_str(&format!("transport: {transport}\n"));
    text.push_str("username: admin\n");
    // The pin is written by `write_profile` (it knows the fixture's
    // pin); this builder takes the hex directly so refusal tests can
    // also build deliberately wrong pins.
    text.push_str("pin: <PIN>\n");
    if let Some(sni) = sni {
        text.push_str(&format!("sni: {sni}\n"));
    }
    if let Some(database) = database {
        text.push_str(&format!("database: {database}\n"));
    }
    assert!(
        ktav::parse_strict(&text).is_ok(),
        "generated fixture profile must be strict-Ktav valid (address escaping bug)",
    );
    text
}

/// Write the profile leaf (see [`write_private_file`]).
pub fn write_profile(fixture: &ShellFixture) -> PathBuf {
    write_profile_with_pin(fixture, &fixture.pin_hex())
}

/// Like [`write_profile`] but with a caller-supplied pin hex — for
/// mismatch/no-pin refusal tests. An empty pin string omits the field.
pub fn write_profile_with_pin(fixture: &ShellFixture, pin_hex: &str) -> PathBuf {
    let dir = profiles_dir(fixture);
    std::fs::create_dir_all(dir.parent().expect("profiles parent")).expect("create config root");
    create_private_dir(&dir).expect("create private profiles dir");
    verify_private_dir(&dir);
    let path = dir.join(format!("{PROFILE_NAME}.ktav"));
    let pin_line = if pin_hex.is_empty() {
        String::new()
    } else {
        format!("pin: {pin_hex}\n")
    };
    let text = profile_text(
        "tls-tcp",
        &format!("127.0.0.1:{}", fixture.tls_addr.port()),
        Some("localhost"),
        Some(DB_NAME),
    )
    .replacen("pin: <PIN>\n", &pin_line, 1);
    write_private_file(&path, &text);
    path
}

/// Overwrite (or create) a profile file with raw text — the escape hatch
/// refusal tests use for DELIBERATELY malformed profiles. Same ownership
/// guarantees.
pub fn overwrite_profile(fixture: &ShellFixture, text: &str) -> PathBuf {
    let dir = profiles_dir(fixture);
    std::fs::create_dir_all(dir.parent().expect("profiles parent")).expect("create config root");
    if !dir.exists() {
        create_private_dir(&dir).expect("create private profiles dir");
    }
    verify_private_dir(&dir);
    let path = dir.join(format!("{PROFILE_NAME}.ktav"));
    write_private_file(&path, text);
    path
}

/// Production-contract re-check of a private directory the fixture just
/// created (`shamir_private_dir::verify_private_dir`); fails at the
/// fixture step, not mid-shell.
fn verify_private_dir(dir: &Path) {
    shamir_private_dir::verify_private_dir(dir).expect("profiles dir meets private-dir contract");
}

/// Owner-only file write: Unix `0600` at creation; Windows an atomic
/// `CreateFileW` leaf carrying its own owner-only descriptor (the
/// parent's protected DACL does not inherit onto children).
fn write_private_file(path: &Path, text: &str) {
    #[cfg(unix)]
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600) // applied at CREATION only; an existing leaf keeps its 0600
            .open(path)
            .expect("create profile file 0600");
        file.write_all(text.as_bytes()).expect("write profile file");
    }
    #[cfg(windows)]
    {
        let sec_desc = shamir_private_dir::OwnerOnlySecurityDescriptor::for_current_user()
            .expect("owner-only security descriptor for profile leaf");
        let wide: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // SAFETY: `wide` is a valid null-terminated UTF-16 path and
        // `sec_desc.as_security_attributes()` a valid `SECURITY_ATTRIBUTES`
        // (descriptor kept alive by `sec_desc`). On success the returned
        // HANDLE is exclusively owned by the `std::fs::File` constructed
        // right after; on `INVALID_HANDLE_VALUE` nothing is created.
        let handle = unsafe {
            win_fs::CreateFileW(
                wide.as_ptr(),
                win_foundation::GENERIC_WRITE,
                0, // no sharing: exclusive leaf, mirrors Unix 0600 intent
                sec_desc.as_security_attributes(),
                win_fs::CREATE_NEW, // atomic create-or-fail: no clobber
                win_fs::FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(), // no template handle
            )
        };
        if handle == win_foundation::INVALID_HANDLE_VALUE {
            let create_error = std::io::Error::last_os_error();
            if create_error.raw_os_error() != Some(win_foundation::ERROR_FILE_EXISTS as i32) {
                panic!("atomically create owner-only profile leaf: {create_error}");
            }
            // Leaf exists from an earlier fixture write (owner-only DACL);
            // reopen for truncate, keeping the original DACL.
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(path)
                .expect("reopen owner-only profile leaf");
            file.write_all(text.as_bytes()).expect("write profile file");
            return;
        }
        // SAFETY: `handle` is a valid `CreateFileW` result, checked nonnull,
        // exclusively owned by the constructed `std::fs::File`.
        let mut file = unsafe {
            assert!(
                !handle.is_null(),
                "CreateFileW returned a null handle (not INVALID_HANDLE_VALUE)"
            );
            std::fs::File::from_raw_handle(handle)
        };
        file.write_all(text.as_bytes()).expect("write profile file");
    }
}

// ── Shell subprocess ─────────────────────────────────────────────────

/// One shell subprocess run: exit status plus raw captured streams. The
/// bytes are NEVER formatted into an assertion message by this module —
/// the output tests scan them via [`ShellRun::contains_canary`].
pub struct ShellRun {
    /// 0 on success.
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl ShellRun {
    /// `true` when the canary byte string appears in either captured
    /// stream. (Case-sensitive exact bytes — the canary is unique.)
    pub fn contains_canary(&self, canary: &str) -> bool {
        contains(&self.stdout, canary) || contains(&self.stderr, canary)
    }
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    needle.len() <= haystack.len()
        && haystack
            .windows(needle.len())
            .any(|w| w == needle.as_bytes())
}

/// A child `Command` for [`SERVER_BIN`] with EVERY variable the production
/// shell actually reads (audited `shell/` reads: `XDG_CONFIG_HOME`+`HOME`,
/// `LOCALAPPDATA`, `SHAMIR_SHELL_PASSWORD`, `SHAMIR_SHELL_TOKEN`; plus
/// `USERPROFILE` so a Windows
/// caller's real home cannot survive a regression) stripped BEFORE the
/// per-fixture overrides the callers apply.
pub(super) fn server_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new(SERVER_BIN);
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
    command
}

/// Run a real shell subprocess with the bootstrap password on stdin.
pub fn run_shell(fixture: &ShellFixture, args: &[&str]) -> ShellRun {
    secure_fixture::run_with_secret(fixture, fixture.canary.clone(), false, args)
}

/// Assert one run exited with `code`; the message carries only the exit code.
pub fn assert_exit(run: &ShellRun, code: i32) {
    assert_eq!(run.exit_code, code, "unexpected shell exit");
}

/// [`run_shell`] plus a success assertion whose message carries ONLY the
/// exit code and stream byte counts — no raw argv and no raw output
/// (canary or peer text must not reflect through a failure).
pub fn run_shell_ok(fixture: &ShellFixture, args: &[&str]) -> ShellRun {
    let run = run_shell(fixture, args);
    assert!(
        run.exit_code == 0,
        "shell command exited {}; stdout {} bytes, stderr {} bytes",
        run.exit_code,
        run.stdout.len(),
        run.stderr.len(),
    );
    run
}

/// [`run_shell`] plus a failure assertion with the same output-free,
/// argv-free message discipline.
pub fn run_shell_refused(fixture: &ShellFixture, args: &[&str]) -> ShellRun {
    let run = run_shell(fixture, args);
    assert!(
        run.exit_code != 0,
        "shell command unexpectedly succeeded; stdout {} bytes",
        run.stdout.len(),
    );
    run
}

// ── Catalogue seeding ────────────────────────────────────────────────
/// One bounded seed round-trip: execute `batch` on `db`, then require the
/// `alias` result echoing `expected` at `field`.
async fn seed_expect(
    client: &Client,
    db: &str,
    alias: &str,
    batch: BatchRequest,
    field: &str,
    expected: &str,
) {
    let response = client
        .execute(db, batch)
        .await
        .expect("seed batch executes");
    let result = response
        .results
        .get(alias)
        .expect("seed response carries the registered alias");
    assert_eq!(
        (result.records.len(), result.records[0].get_value_str(field)),
        (1, Some(expected)),
        "seed op must return exactly one record echoing the created item",
    );
}

/// Seed the catalogue over the REAL wire with the existing builders only:
/// one database, one repository with ALL three tables (the schema-bearing
/// one starts PLAIN — its rules land via `set_table_schema` below), a plain
/// index, a control/ESC-byte index and one known function — every execute
/// validated via [`seed_expect`]; indexes carry an EXPLICIT
/// `.repo(REPO_NAME)` (create_index defaults to repo `"main"`).
pub fn seed_catalogue(fixture: &ShellFixture) {
    seed_catalogue_timeout(fixture, BOOT_BOUND)
}

/// Seed the catalogue with a caller-provided hang bound.
pub fn seed_catalogue_timeout(fixture: &ShellFixture, timeout: Duration) {
    fixture.guard.mark("seed: catalogue");
    let tls_addr = fixture.tls_addr;
    let pin = fixture.pin;
    let password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    fixture.rt.run(bound(timeout, async move {
        let client = connect_tls_owned(tls_addr, pin, password)
            .await
            .expect("seed: fixture verified TLS connect");
        // Reflection self-check: without control bytes the escape tests assert nothing.
        assert!(
            INDEX_ODD_NAME.chars().any(char::is_control),
            "reflection index name lost its control chars",
        );
        // `seed_expect` bound to `DB_NAME` — keeps every call one line.
        async fn seed_db(c: &Client, alias: &str, batch: BatchRequest, field: &str, want: &str) {
            seed_expect(c, DB_NAME, alias, batch, field, want).await;
        }

        let mut b = Batch::new();
        b.id("mk_shell_db");
        b.create_db("mkdb", ddl::create_db(DB_NAME));
        seed_expect(&client, ROUTING_DB, "mkdb", b.build(), "db", DB_NAME).await;

        let mut b = Batch::new();
        b.id("mk_shell_repo");
        b.create_repo(
            "mkrepo",
            ddl::create_repo(REPO_NAME)
                .engine(ddl::RepoEngine::Fjall)
                .tables([PLAIN_TABLE, INDEXED_TABLE, SCHEMA_TABLE]),
        );
        seed_db(&client, "mkrepo", b.build(), "created_repo", REPO_NAME).await;

        // The schema table starts PLAIN (registered with the repo above) —
        // an ordinary `CreateTable` NEVER applies rules, so they reach the
        // catalogue only through the typed `set_table_schema` op. Assert its
        // FULL actual echo (persist + activate; `admin_schema.rs:583-588`).
        let mut b = Batch::new();
        b.id("mk_shell_schema_apply");
        b.set_table_schema(
            "mkset",
            ddl::set_table_schema(SCHEMA_TABLE).repo(REPO_NAME).rules([
                ddl::field(["score"]).int().required().build(),
                ddl::field(["label"]).string().build(),
            ]),
        );
        let response = client
            .execute(DB_NAME, b.build())
            .await
            .expect("seed batch executes");
        let result = response
            .results
            .get("mkset")
            .expect("seed response carries the alias");
        assert_eq!(
            result.records.len(),
            1,
            "set_table_schema returns one record"
        );
        let record = &result.records[0];
        assert_eq!(record.get_value_str("set_table_schema"), Some(SCHEMA_TABLE));
        assert_eq!(record.get_value_str("repo"), Some(REPO_NAME));
        assert_eq!(record.get_value_bool("ok"), Some(true));
        assert_eq!(record.get_value_i64("schema_version"), Some(1));

        let mut b = Batch::new();
        b.id("mk_shell_index");
        b.create_index(
            "mkindex",
            ddl::create_index(INDEX_NAME, INDEXED_TABLE)
                .repo(REPO_NAME)
                .field("score"),
        );
        seed_db(&client, "mkindex", b.build(), "created_index", INDEX_NAME).await;

        // Distinct field, so it never collides with `by_score` — escape it.
        let mut b = Batch::new();
        b.id("mk_shell_odd_index");
        b.create_index(
            "mkoddindex",
            ddl::create_index(INDEX_ODD_NAME, INDEXED_TABLE)
                .repo(REPO_NAME)
                .field("label"),
        );
        let odd = INDEX_ODD_NAME;
        seed_db(&client, "mkoddindex", b.build(), "created_index", odd).await;

        let mut b = Batch::new();
        b.id("mk_shell_function");
        b.create_function(
            "mkfn",
            ddl::create_function(FN_NAME)
                .wasm(echo_wasm_b64())
                .build()
                .expect("echo function builder"),
        );
        seed_db(&client, "mkfn", b.build(), "created_function", FN_NAME).await;

        client.close().await;
    }));
}

/// Identity-echo WASM (slice-2 ABI) — same no-toolchain shape as
/// `create_function_gating.rs`.
const FN_ECHO_WAT: &str = r#"
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "shamir_alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $len)))
    (local.get $ptr))
  (func (export "shamir_call") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))
"#;

fn echo_wasm_b64() -> String {
    let wasm = wat::parse_str(FN_ECHO_WAT).expect("echo WAT parses");
    base64::engine::general_purpose::STANDARD.encode(&wasm)
}

/// Create the non-superuser shell account over the REAL wire and return
/// its password — a SECOND per-run unique zeroizing canary. NO roles, so
/// every `authorize_access`-gated catalogue op denies the account. Both the
/// canonical and the transmitted op come from the typed builder only: the
/// canonical form EXCLUDES `hmac` (and the password), so a placeholder tag
/// yields canonical bytes equal to the real op's; the wire op carries the
/// real session-keyed tag.
pub fn seed_shell_user(fixture: &ShellFixture) -> Zeroizing<String> {
    seed_shell_user_named_timeout(fixture, SHELL_USER, BOOT_BOUND)
}

/// Seed the standard shell user with a caller-provided hang bound.
pub fn seed_shell_user_timeout(fixture: &ShellFixture, timeout: Duration) -> Zeroizing<String> {
    seed_shell_user_named_timeout(fixture, SHELL_USER, timeout)
}

/// Seed a named no-role account with a caller-provided hang bound.
pub fn seed_shell_user_named_timeout(
    fixture: &ShellFixture,
    username: &str,
    timeout: Duration,
) -> Zeroizing<String> {
    fixture.guard.mark("seed: shell user");
    let tls_addr = fixture.tls_addr;
    let pin = fixture.pin;
    let username = username.to_owned();
    let admin_password = Zeroizing::new(fixture.canary.as_bytes().to_vec());
    let user_password = Zeroizing::new(format!("{}u", unique_canary().as_str()));
    fixture.rt.run(bound(timeout, async move {
        let client = connect_tls_owned(tls_addr, pin, admin_password)
            .await
            .expect("seed: fixture verified TLS connect (shell user)");

        let placeholder = "0".repeat(64);
        let unsigned = ddl::create_user(username.as_str(), user_password.as_str())
            .hmac(&placeholder)
            .build()
            .expect("typed create-user builder (placeholder tag)");
        let key = canon::derive_session_hmac_key(&client.session_id());
        let canonical = canon::canonical_batch_op(ROUTING_DB, &unsigned)
            .expect("canonical form of the create-user op");
        let tag = canon::compute_tag_hex(&key, &canonical);
        let op = ddl::create_user(username.as_str(), user_password.as_str())
            .hmac(&tag)
            .build()
            .expect("typed create-user builder");
        let mut b = Batch::new();
        b.id("mk_shell_user");
        b.create_user("mkuser", op);
        seed_expect(
            &client,
            ROUTING_DB,
            "mkuser",
            b.build(),
            "created_user",
            username.as_str(),
        )
        .await;

        client.close().await;
        user_password
    }))
}

// ── Shutdown bound ───────────────────────────────────────────────────

/// Graceful-shutdown hang bound (≥ [`BOOT_BOUND`]).
const SHUTDOWN_BOUND: Duration = Duration::from_secs(150);
