#[path = "../token/views.rs"]
mod views;

use std::collections::BTreeMap;

use serde::de::IgnoredAny;
use shamir_client as core;
use shamir_query_types::wire::DbResponse;
use zeroize::Zeroizing;

const CLIENT_RS: &str = include_str!("../client.rs");
const CV_MOD_RS: &str = include_str!("../connect_variants/mod.rs");
const CV_CONNECT_LOCAL_RS: &str = include_str!("../connect_variants/connect_local.rs");
const CV_CONNECT_PLAIN_RS: &str = include_str!("../connect_variants/connect_plain.rs");
const CV_CONNECT_WS_RS: &str = include_str!("../connect_variants/connect_ws.rs");
const CV_CONNECT_WS_BROWSER_RS: &str = include_str!("../connect_variants/connect_ws_browser.rs");
const CV_OPTIONS_MOD_RS: &str = include_str!("../connect_variants/options/mod.rs");
const CV_OPTIONS_LOCAL_RS: &str = include_str!("../connect_variants/options/local.rs");
const CV_OPTIONS_PLAIN_RS: &str = include_str!("../connect_variants/options/plain.rs");
const CV_OPTIONS_RESUME_RS: &str = include_str!("../connect_variants/options/resume.rs");
const CV_OPTIONS_BROWSER_RS: &str = include_str!("../connect_variants/options/browser.rs");
const CV_OPTIONS_RESUME_LOCAL_RS: &str =
    include_str!("../connect_variants/options/resume_local.rs");
const CV_RESUME_MOD_RS: &str = include_str!("../connect_variants/resume/mod.rs");
const CV_RESUME_SHARED_RS: &str = include_str!("../connect_variants/resume/shared.rs");
const CV_RESUME_TLS_RS: &str = include_str!("../connect_variants/resume/tls.rs");
const CV_RESUME_WS_RS: &str = include_str!("../connect_variants/resume/ws.rs");
const CV_RESUME_LOCAL_RS: &str = include_str!("../connect_variants/resume/local.rs");
const CV_RESUME_PLAIN_RS: &str = include_str!("../connect_variants/resume/plain.rs");
const CV_RESUME_WS_BROWSER_RS: &str = include_str!("../connect_variants/resume/ws_browser.rs");
const TOKEN_ADMIN_RS: &str = include_str!("../token/admin.rs");
const TOKEN_ENDPOINT_RS: &str = include_str!("../token/endpoint.rs");
const TOKEN_HTTP_CLIENT_RS: &str = include_str!("../token/http_client.rs");
const TOKEN_LOGIN_RS: &str = include_str!("../token/login.rs");
const TOKEN_MOD_RS: &str = include_str!("../token/mod.rs");
const TOKEN_VIEWS_RS: &str = include_str!("../token/views.rs");
const TOKEN_WS_RS: &str = include_str!("../token/ws.rs");

#[test]
fn native_db_error_named_messagepack_matches_wrapper_discriminator() {
    let error = DbResponse::Error {
        code: "x".to_owned(),
        message: "y".to_owned(),
    };
    let bytes = rmp_serde::to_vec_named(&error).expect("encode native error");
    assert_eq!(
        bytes,
        [
            0x83, 0xa4, 0x6b, 0x69, 0x6e, 0x64, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72, 0xa4, 0x63,
            0x6f, 0x64, 0x65, 0xa1, 0x78, 0xa7, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0xa1,
            0x79,
        ]
    );
}

fn key_set(bytes: &[u8]) -> Vec<String> {
    let map: BTreeMap<String, IgnoredAny> =
        rmp_serde::from_slice(bytes).expect("view decodes as a string-keyed map");
    map.into_keys().collect()
}

fn sorted(keys: &[&str]) -> Vec<String> {
    let mut keys: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
    keys.sort();
    keys
}

#[test]
fn created_token_view_wire_shape_is_named_and_not_an_error_marker() {
    let _ = views::CreatedTokenView::from_created;
    let view = views::CreatedTokenView {
        id: "abc".to_owned(),
        token: Zeroizing::new("shk1.secret".to_owned()),
        label: "ci".to_owned(),
        owner: "admin".to_owned(),
        mode: core::ApiTokenMode::ReadWrite,
        channels: core::ApiTokenChannels::Both,
        databases: Some(vec!["prod".to_owned()]),
        created_at_ns: 1,
        expires_at_ns: 2,
    };
    let bytes = rmp_serde::to_vec_named(&view).expect("encode created token view");
    assert_eq!(
        key_set(&bytes),
        sorted(&[
            "id",
            "token",
            "label",
            "owner",
            "mode",
            "channels",
            "databases",
            "created_at_ns",
            "expires_at_ns",
        ])
    );
    assert_eq!(bytes[0], 0x89);
    assert_eq!(&bytes[1..4], [0xa2, b'i', b'd']);
    assert_ne!(
        &bytes[..12],
        [0x83, 0xa4, 0x6b, 0x69, 0x6e, 0x64, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72]
    );
}

#[test]
fn login_view_wire_shape_has_binary_id_and_is_not_an_error_marker() {
    let _ = views::LoginView::from_outcome;
    let id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let view = views::LoginView {
        token: Zeroizing::new("shk1.secret".to_owned()),
        id,
        owner: "admin".to_owned(),
        label: "http-login".to_owned(),
        mode: core::LoginMode::ReadOnly,
        channels: core::LoginChannels::Rest,
        databases: None,
        created_at_ns: 1,
        expires_at_ns: 2,
    };
    let bytes = rmp_serde::to_vec_named(&view).expect("encode login view");
    assert_eq!(
        key_set(&bytes),
        sorted(&[
            "token",
            "id",
            "owner",
            "label",
            "mode",
            "channels",
            "databases",
            "created_at_ns",
            "expires_at_ns",
        ])
    );
    assert_eq!(bytes[0], 0x89);
    assert_eq!(&bytes[1..7], [0xa5, b't', b'o', b'k', b'e', b'n']);
    assert!(bytes
        .windows(18)
        .any(|window| window[0] == 0xc4 && window[1] == 0x10 && window[2..] == id));
}

fn derive_lines_with_debug(source: &str) -> Vec<&str> {
    source
        .lines()
        .filter(|line| line.contains("derive(") && line.contains("Debug"))
        .collect()
}

#[test]
fn option_and_view_sources_never_derive_debug() {
    for (name, source) in [
        ("client.rs", CLIENT_RS),
        ("connect_variants/mod.rs", CV_MOD_RS),
        ("connect_variants/connect_local.rs", CV_CONNECT_LOCAL_RS),
        ("connect_variants/connect_plain.rs", CV_CONNECT_PLAIN_RS),
        ("connect_variants/connect_ws.rs", CV_CONNECT_WS_RS),
        ("connect_variants/options/mod.rs", CV_OPTIONS_MOD_RS),
        ("connect_variants/options/local.rs", CV_OPTIONS_LOCAL_RS),
        ("connect_variants/options/plain.rs", CV_OPTIONS_PLAIN_RS),
        ("connect_variants/options/resume.rs", CV_OPTIONS_RESUME_RS),
        ("connect_variants/options/browser.rs", CV_OPTIONS_BROWSER_RS),
        (
            "connect_variants/options/resume_local.rs",
            CV_OPTIONS_RESUME_LOCAL_RS,
        ),
        (
            "connect_variants/connect_ws_browser.rs",
            CV_CONNECT_WS_BROWSER_RS,
        ),
        ("connect_variants/resume/mod.rs", CV_RESUME_MOD_RS),
        ("connect_variants/resume/shared.rs", CV_RESUME_SHARED_RS),
        ("connect_variants/resume/tls.rs", CV_RESUME_TLS_RS),
        ("connect_variants/resume/ws.rs", CV_RESUME_WS_RS),
        ("connect_variants/resume/local.rs", CV_RESUME_LOCAL_RS),
        ("connect_variants/resume/plain.rs", CV_RESUME_PLAIN_RS),
        (
            "connect_variants/resume/ws_browser.rs",
            CV_RESUME_WS_BROWSER_RS,
        ),
        ("token/admin.rs", TOKEN_ADMIN_RS),
        ("token/endpoint.rs", TOKEN_ENDPOINT_RS),
        ("token/http_client.rs", TOKEN_HTTP_CLIENT_RS),
        ("token/login.rs", TOKEN_LOGIN_RS),
        ("token/mod.rs", TOKEN_MOD_RS),
        ("token/views.rs", TOKEN_VIEWS_RS),
        ("token/ws.rs", TOKEN_WS_RS),
    ] {
        assert!(
            derive_lines_with_debug(source).is_empty(),
            "{name} derives Debug"
        );
    }
}

#[test]
fn secret_bearing_sources_protect_their_secrets() {
    for (name, source) in [
        ("token/http_client.rs", TOKEN_HTTP_CLIENT_RS),
        ("token/ws.rs", TOKEN_WS_RS),
        ("token/login.rs", TOKEN_LOGIN_RS),
        ("connect_variants/connect_local.rs", CV_CONNECT_LOCAL_RS),
        ("connect_variants/connect_plain.rs", CV_CONNECT_PLAIN_RS),
        ("connect_variants/resume/shared.rs", CV_RESUME_SHARED_RS),
        (
            "connect_variants/connect_ws_browser.rs",
            CV_CONNECT_WS_BROWSER_RS,
        ),
        ("connect_variants/resume/local.rs", CV_RESUME_LOCAL_RS),
        ("connect_variants/resume/plain.rs", CV_RESUME_PLAIN_RS),
        (
            "connect_variants/resume/ws_browser.rs",
            CV_RESUME_WS_BROWSER_RS,
        ),
    ] {
        assert!(source.contains("secrets::protect"), "{name} lacks protect");
    }
}
