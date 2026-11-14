//! Shared fixture: one live server with TCP+TLS, WebSocket, plain TCP and browser-profile WebSocket listeners.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use shamir_types::mpack;
use tempfile::TempDir;
use zeroize::Zeroizing;

use shamir_client::builder::batch::Batch;
use shamir_client::builder::ddl;
use shamir_client::builder::doc;
use shamir_client::builder::write::upsert;
use shamir_client::builder::Query;
use shamir_client::{
    BrowserResumeOptions, BrowserWsOptions, Client, ConnectOptions, ResumeOptions, TokenTrust,
};

use shamir_server::config::{
    Config, KdfConfig, ListenerConfig, ListenerKind, LoggingConfig, ProfileKind, TlsConfig,
};
use shamir_server::server::{BootstrapMode, ServerHandle, ServerLauncher};
use shamir_test_diag::FlakeGuard;

pub const USERNAME: &str = "admin";
pub const PASSWORD: &[u8] = b"correct horse battery staple";

/// The one origin the fixture's browser listener allows.
pub const BROWSER_ORIGIN: &str = "https://shamir.test";

pub struct Matrix {
    pub handle: ServerHandle,
    pub tcp: SocketAddr,
    pub ws: SocketAddr,
    pub plain: SocketAddr,
    pub ws_browser: SocketAddr,
    _temp: TempDir,
    cert_pem: Vec<u8>,
}

impl Matrix {
    /// Trust anchor for the server's self-signed certificate (the fixture
    /// reads it from disk after boot, as the token fixture does).
    pub fn browser_trust(&self) -> TokenTrust {
        TokenTrust::pem(&self.cert_pem).expect("trust generated cert")
    }

    pub async fn shutdown(self) {
        self.handle.shutdown().await;
    }
}

fn fast_kdf() -> KdfConfig {
    KdfConfig {
        memory_kb: 19_456,
        time: 2,
        parallelism: 1,
        argon2_version: 0x13,
    }
}

fn listener(kind: ListenerKind, profile: ProfileKind, path: Option<String>) -> ListenerConfig {
    ListenerConfig {
        kind,
        addr: "127.0.0.1:0".into(),
        profile: Some(profile),
        path,
        kdf_override: None,
        browser_origin_allowlist: vec![],
        http: Default::default(),
    }
}

fn browser_listener(path: String) -> ListenerConfig {
    ListenerConfig {
        kind: ListenerKind::Ws,
        addr: "127.0.0.1:0".into(),
        profile: Some(ProfileKind::TlsNoExport),
        path: Some(path),
        kdf_override: None,
        browser_origin_allowlist: vec![BROWSER_ORIGIN.to_string()],
        http: Default::default(),
    }
}

fn make_config(temp: &TempDir) -> Config {
    let data_dir: PathBuf = temp.path().to_path_buf();
    Config {
        tokens: Default::default(),
        data_dir: data_dir.clone(),
        logging: LoggingConfig {
            level: "warn".into(),
            slow_query_threshold_ms: 0,
            file: None,
            flush_interval_ms: 2000,
        },
        durability: Default::default(),
        transactions: Default::default(),
        kdf_defaults: fast_kdf(),
        argon2_concurrent_max: 4,
        listeners: vec![
            listener(ListenerKind::Tcp, ProfileKind::TlsExporter, None),
            listener(
                ListenerKind::Ws,
                ProfileKind::TlsExporter,
                Some("/shamir/v1".to_string()),
            ),
            listener(ListenerKind::Tcp, ProfileKind::Plain, None),
            browser_listener("/shamir/v1/browser".to_string()),
        ],
        tls: TlsConfig {
            cert_path: data_dir.join("cert.pem"),
            key_path: data_dir.join("key.pem"),
        },
        security: Default::default(),
        audit: Default::default(),
        observability: shamir_server::config::ObservabilityConfig {
            addr: String::new(),
            allow_public_metrics: false,
        },
        replication: None,
    }
}

pub async fn boot() -> Matrix {
    let guard = FlakeGuard::new("matrix server boot");
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let temp = TempDir::new().expect("tempdir");
    let launcher = ServerLauncher {
        node_mode: Default::default(),
        config: make_config(&temp),
        bootstrap: BootstrapMode::Password {
            username: USERNAME.into(),
            password: Zeroizing::new(PASSWORD.to_vec()),
        },
    };
    let handle = launcher.launch().await.expect("launcher boot");
    guard.mark("launcher boot done");
    let tcp = handle.bound_addrs[0].expect("listener bound");
    let ws = handle.bound_addrs[1].expect("listener bound");
    let plain = handle.bound_addrs[2].expect("listener bound");
    let ws_browser = handle.bound_addrs[3].expect("listener bound");
    let cert_pem = std::fs::read(temp.path().join("cert.pem")).expect("generated cert");
    guard.mark("fixture ready");
    Matrix {
        handle,
        tcp,
        ws,
        plain,
        ws_browser,
        _temp: temp,
        cert_pem,
    }
}

pub fn options(addr: SocketAddr) -> ConnectOptions {
    ConnectOptions {
        addr,
        server_name: "localhost".into(),
        username: USERNAME.into(),
        password: Zeroizing::new(PASSWORD.to_vec()),
        accept_new_host: true,
        trusted_pin: None,
        connect_timeout: Some(Duration::from_secs(10)),
        handshake_timeout: Some(Duration::from_secs(60)),
        request_timeout: Some(Duration::from_secs(30)),
    }
}

pub fn ticket_of(client: &Client) -> Vec<u8> {
    client
        .resumption_ticket()
        .expect("server issues a resumption ticket")
        .to_vec()
}

pub fn resume_options(addr: SocketAddr, client: &Client) -> ResumeOptions {
    ResumeOptions {
        addr,
        server_name: "localhost".into(),
        ticket: ticket_of(client),
        pinned_hash: client.server_pub_key_pin(),
    }
}

pub fn browser_session_options(matrix: &Matrix) -> BrowserWsOptions {
    BrowserWsOptions::new(
        options(matrix.ws_browser),
        BROWSER_ORIGIN,
        matrix.browser_trust(),
    )
}

pub fn browser_resume_options(matrix: &Matrix, client: &Client) -> BrowserResumeOptions {
    BrowserResumeOptions::new(
        resume_options(matrix.ws_browser, client),
        BROWSER_ORIGIN,
        matrix.browser_trust(),
    )
}

pub async fn exercise(client: &Client, db: &str) {
    client.ping().await.expect("ping");

    let mut mk_db = Batch::new();
    mk_db.id("mk-db");
    mk_db.create_db("mk", ddl::create_db(db));
    let resp = client
        .execute("default", mk_db.build())
        .await
        .expect("create db");
    assert!(resp.results.contains_key("mk"));

    let mut mk_table = Batch::new();
    mk_table.id("mk-table");
    mk_table.create_repo("mr", ddl::create_repo("main"));
    mk_table.create_table("tb", ddl::create_table("items").repo("main"));
    let resp = client
        .execute(db, mk_table.build())
        .await
        .expect("create table");
    assert!(resp.results.contains_key("mr"));
    assert!(resp.results.contains_key("tb"));

    let mut batch = Batch::new();
    batch.id("rw");
    batch
        .try_upsert(
            "ins",
            upsert("items")
                .key(mpack!({"sku": "X1"}))
                .value(doc! { "sku" => "X1", "qty" => 42 }),
        )
        .unwrap();
    batch.query("rd", Query::from("items"));
    let resp = client.execute(db, batch.build()).await.expect("rw");
    let rd = resp.results.get("rd").expect("rd alias");
    assert_eq!(rd.records.len(), 1);
    assert_eq!(rd.records[0].get_value_str("sku"), Some("X1"));
    assert_eq!(rd.records[0].get_value_i64("qty"), Some(42));
}
