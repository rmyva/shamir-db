use shamir_connect::server::token::{TokenChannels, TokenMode};
use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig};
use shamir_query_types::wire::{DbRequest, DbResponse};
use shamir_server::bootstrap::{ensure_superuser, BootstrapPolicy};
use shamir_server::config::{Config, ListenerConfig, ListenerKind};
use shamir_server::server::{BootstrapMode, ServerHandle, ServerLauncher};
use shamir_server::tokens::NewToken;
use shamir_server::user_directory::FjallUserDirectory;
use shamir_test_diag::FlakeGuard;
use shamir_types::access::ResourcePath;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Duration;
use zeroize::Zeroizing;

pub(crate) const ADMIN_PASSWORD: &[u8] = b"http-rest-admin-password";

pub struct Harness {
    pub handle: ServerHandle,
    pub temp: tempfile::TempDir,
    pub owner: [u8; 16],
    pub(super) owner_roles: Vec<String>,
}

impl Harness {
    pub async fn launch() -> Self {
        Self::launch_with(
            "127.0.0.1:0",
            shamir_server::config::HttpListenerSettings {
                tls: Some(false),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn launch_with(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
    ) -> Self {
        Self::launch_inner(addr, settings, false).await
    }

    pub async fn launch_with_hook<F>(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
        hook: F,
    ) -> Self
    where
        F: FnOnce(&mut Config),
    {
        Self::launch_inner_with_hook(addr, settings, false, hook).await
    }

    /// Starts HTTP REST and native TLS-exporter TCP listeners on the same database.
    pub async fn launch_with_native_seed() -> Self {
        Self::launch_with_native_seed_settings(
            "127.0.0.1:0",
            shamir_server::config::HttpListenerSettings {
                tls: Some(false),
                ..Default::default()
            },
        )
        .await
    }

    pub(crate) async fn launch_with_native_seed_hook<F>(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
        hook: F,
    ) -> Self
    where
        F: FnOnce(&mut Config),
    {
        Self::launch_inner_with_hook(addr, settings, true, hook).await
    }

    /// Starts HTTP REST and native TLS-exporter TCP listeners with the specified HTTP settings.
    pub(super) async fn launch_with_native_seed_settings(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
    ) -> Self {
        Self::launch_inner(addr, settings, true).await
    }

    async fn launch_inner(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
        native: bool,
    ) -> Self {
        Self::launch_inner_with_hook(addr, settings, native, |_| {}).await
    }

    async fn launch_inner_with_hook<F>(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
        native: bool,
        hook: F,
    ) -> Self
    where
        F: FnOnce(&mut Config),
    {
        Self::launch_with_hooks(addr, settings, native, hook, |_| {}).await
    }

    /// Starts the shared HTTP harness with test-specific config and user seeding.
    pub async fn launch_with_hooks<C, S>(
        addr: &str,
        settings: shamir_server::config::HttpListenerSettings,
        native: bool,
        configure: C,
        seed: S,
    ) -> Self
    where
        C: FnOnce(&mut Config),
        S: FnOnce(&std::path::Path),
    {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_dir = temp.path().to_path_buf();
        let (owner, owner_roles) = seed_owner(&data_dir).await;
        seed(&data_dir);
        let mut config = config_for_with(&data_dir, addr, settings);
        configure(&mut config);
        if native {
            config.listeners.push(ListenerConfig {
                kind: ListenerKind::Tcp,
                addr: "127.0.0.1:0".into(),
                profile: Some(shamir_server::config::ProfileKind::TlsExporter),
                path: None,
                kdf_override: None,
                browser_origin_allowlist: vec![],
                http: Default::default(),
            });
        }
        let handle = launch_through_fjall_release(config).await;
        Self {
            handle,
            temp,
            owner,
            owner_roles,
        }
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        let addr = self.handle.bound_addrs[0].expect("HTTP bound address");
        if addr.ip().is_unspecified() {
            std::net::SocketAddr::new("127.0.0.1".parse().expect("loopback address"), addr.port())
        } else {
            addr
        }
    }

    pub fn native_addr(&self) -> std::net::SocketAddr {
        let addr = self.handle.bound_addrs[1].expect("native TCP bound address");
        if addr.ip().is_unspecified() {
            std::net::SocketAddr::new("127.0.0.1".parse().expect("loopback address"), addr.port())
        } else {
            addr
        }
    }

    pub async fn token_with_ttl(&self, label: &str, mode: TokenMode, ttl_secs: u64) -> String {
        self.token_with_options(label, mode, ttl_secs, TokenChannels::Both, None)
            .await
            .token
            .to_string()
    }

    pub async fn token_with_channels(
        &self,
        label: &str,
        mode: TokenMode,
        channels: TokenChannels,
    ) -> String {
        self.token_with_options(label, mode, 3600, channels, None)
            .await
            .token
            .to_string()
    }

    pub async fn token_with_options(
        &self,
        label: &str,
        mode: TokenMode,
        ttl_secs: u64,
        channels: TokenChannels,
        databases: Option<Vec<String>>,
    ) -> shamir_server::tokens::CreatedToken {
        self.handle
            .token_service
            .create(NewToken {
                owner_user_id: self.owner,
                owner_roles: self.owner_roles.clone(),
                label: label.into(),
                ttl_secs: Some(ttl_secs),
                mode,
                channels,
                databases,
            })
            .await
            .expect("create token")
    }

    pub async fn token(&self, label: &str, mode: TokenMode) -> String {
        self.token_with_options(label, mode, 3600, TokenChannels::Both, None)
            .await
            .token
            .to_string()
    }
}

/// `seed_owner` drops its stores just before the launch; fjall releases the
/// directory locks from background handles, so the first attempt can observe
/// `Locked`. That is transient: retry until the release lands.
async fn launch_through_fjall_release(config: Config) -> ServerHandle {
    let guard = FlakeGuard::new("http_rest server launch");
    for _ in 0..200 {
        let launched = ServerLauncher {
            config: config.clone(),
            node_mode: Default::default(),
            bootstrap: BootstrapMode::Password {
                username: "admin".into(),
                password: Zeroizing::new(ADMIN_PASSWORD.to_vec()),
            },
        }
        .launch()
        .await;
        match launched {
            Ok(handle) => {
                guard.mark("server launched");
                return handle;
            }
            Err(e) if format!("{e:?}").contains("Locked") => {
                guard.mark("launch retry: fjall lock");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                guard.note("launch error", format!("{e:?}"));
                panic!("launch HTTP listener: {e:?}");
            }
        }
    }
    panic!("fjall lock was not released within the retry window");
}

/// The database's `0o757` mode grants other principals read, write, and execute,
/// allowing interactive transaction commit (which authorizes Write on the database).
/// The store's `0o711` mode grants traversal. Both resources are System-owned at
/// `0o700` by default, which hides even tables the token owner created.
pub async fn seed_owner(data_dir: &std::path::Path) -> ([u8; 16], Vec<String>) {
    let guard = FlakeGuard::new("http_rest seed_owner");
    let directory = FjallUserDirectory::open(data_dir.join("users")).expect("directory");
    let kdf = shamir_server::config::KdfConfig {
        memory_kb: 19_456,
        time: 2,
        parallelism: 1,
        argon2_version: 0x13,
    };
    ensure_superuser(
        &directory,
        data_dir,
        "admin",
        BootstrapPolicy::Password(ADMIN_PASSWORD),
        &shamir_connect::common::kdf_params::KdfParams {
            memory_kb: kdf.memory_kb,
            time: kdf.time,
            parallelism: kdf.parallelism,
            argon2_version: kdf.argon2_version,
        },
    )
    .expect("seed admin");
    let owner = shamir_connect::server::admin::UserDirectory::user_id(&directory, "admin")
        .expect("admin owner id");
    let owner_roles = directory
        .state_by_user_id(&owner)
        .expect("seeded owner state")
        .roles;
    drop(directory);

    let db = shamir_db::ShamirDb::init(shamir_db::SystemStoreConfig::Fjall(
        data_dir.join("shamir_db_meta.redb"),
    ))
    .await
    .expect("initialize owner seed database");
    db.create_db("default")
        .await
        .expect("create default database");
    db.add_repo(
        "default",
        RepoConfig::new(
            "main",
            BoxRepoFactory::fjall(data_dir.join("shamir_db_default_main.redb")),
        ),
    )
    .await
    .expect("create default main repository");

    for (path, mode) in [
        (
            ResourcePath::Database {
                db: "default".into(),
            },
            0o757,
        ),
        (
            ResourcePath::Store {
                db: "default".into(),
                store: "main".into(),
            },
            0o711,
        ),
    ] {
        let mut meta = db
            .resource_meta(&path)
            .await
            .expect("read resource metadata");
        meta.mode = mode;
        db.set_resource_meta(&path, &meta)
            .await
            .expect("set resource mode");
    }
    // The seed flush must not fail on a stalled fsync of a loaded Windows runner.
    guard.mark("flush owner seed database");
    db.flush_all_waiting()
        .await
        .expect("flush owner seed database");
    drop(db);
    (owner, owner_roles)
}

pub fn config_for(data_dir: &std::path::Path) -> Config {
    config_for_with(
        data_dir,
        "127.0.0.1:0",
        shamir_server::config::HttpListenerSettings {
            tls: Some(false),
            ..Default::default()
        },
    )
}

pub fn config_for_with(
    data_dir: &std::path::Path,
    addr: &str,
    settings: shamir_server::config::HttpListenerSettings,
) -> Config {
    Config {
        durability: Default::default(),
        transactions: Default::default(),
        tokens: Default::default(),
        data_dir: data_dir.to_path_buf(),
        logging: shamir_server::config::LoggingConfig {
            level: "warn".into(),
            slow_query_threshold_ms: 0,
            file: None,
            flush_interval_ms: 2000,
        },
        kdf_defaults: shamir_server::config::KdfConfig {
            memory_kb: 19_456,
            time: 2,
            parallelism: 1,
            argon2_version: 0x13,
        },
        argon2_concurrent_max: 4,
        listeners: vec![ListenerConfig {
            kind: ListenerKind::Http,
            addr: addr.into(),
            profile: None,
            path: None,
            kdf_override: None,
            browser_origin_allowlist: vec![],
            http: settings,
        }],
        tls: shamir_server::config::TlsConfig {
            cert_path: data_dir.join("cert.pem"),
            key_path: data_dir.join("key.pem"),
        },
        security: shamir_server::config::SecurityConfig {
            auth_init_rate_per_second: 100_000,
            ..Default::default()
        },
        audit: Default::default(),
        observability: shamir_server::config::ObservabilityConfig {
            addr: String::new(),
            allow_public_metrics: false,
        },
        replication: None,
    }
}

pub async fn request(
    addr: std::net::SocketAddr,
    token: Option<&str>,
    body: &[u8],
) -> (u16, String, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(15), request_inner(addr, token, body))
        .await
        .expect("HTTP request timed out")
}

async fn request_inner(
    addr: std::net::SocketAddr,
    token: Option<&str>,
    body: &[u8],
) -> (u16, String, Vec<u8>) {
    if let Some(token) = token {
        assert!(
            !token.bytes().any(|byte| matches!(byte, b'\r' | b'\n' | 0)),
            "token contains an invalid header byte"
        );
    }
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let authorization = token.map_or(String::new(), |value| {
        format!("Authorization: Bearer {value}\r\n")
    });
    let head = format!(
        "POST /shamir/v1/request HTTP/1.1\r\nHost: localhost\r\n{authorization}Content-Type: application/msgpack\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.expect("write head");
    stream.write_all(body).await.expect("write body");
    let mut response = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        assert!(response.len() < 8192, "HTTP response headers too large");
        let n = stream.read(&mut chunk).await.expect("read response");
        assert!(n > 0, "HTTP response ended before headers");
        response.extend_from_slice(&chunk[..n]);
        if let Some(end) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
    };
    let headers = String::from_utf8_lossy(&response[..header_end]).into_owned();
    let status = headers
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("numeric status");
    let content_length = headers
        .lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .expect("numeric Content-Length")
            })
        })
        .expect("Content-Length response");
    let body_start = header_end
        .checked_add(4)
        .expect("HTTP header size overflow");
    assert!(
        content_length <= 16 * 1024 * 1024,
        "HTTP response body too large"
    );
    let body_end = body_start
        .checked_add(content_length)
        .expect("HTTP response body size overflow");
    while response.len() < body_end {
        let n = stream.read(&mut chunk).await.expect("read response body");
        assert!(n > 0, "HTTP response body ended early");
        response.extend_from_slice(&chunk[..n]);
        assert!(
            response.len() <= body_end,
            "unexpected trailing HTTP response data"
        );
    }
    (status, headers, response[body_start..body_end].to_vec())
}

pub fn ping_body() -> Vec<u8> {
    rmp_serde::to_vec_named(&DbRequest::Ping).expect("encode ping")
}

pub fn assert_pong(status: u16, body: &[u8]) {
    assert_eq!(status, 200);
    assert!(matches!(
        rmp_serde::from_slice::<DbResponse>(body).expect("decode pong"),
        DbResponse::Pong
    ));
}
