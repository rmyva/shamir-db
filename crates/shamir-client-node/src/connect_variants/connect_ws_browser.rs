use napi::bindgen_prelude::*;

use shamir_client as core;

use super::options::ConnectWsBrowserOptions;
use crate::client::{from_core, ShamirClient};
use crate::secrets;
use crate::{bounded, checked_port, infra_error, millis, optional_pin, unresolved_addr};

/// TLS trust for the browser-profile entry points: caller-supplied PEM
/// anchors, or the bundled WebPKI roots. The underlying PEM error can echo
/// a line of the input, so it is dropped in favour of a fixed message.
pub(crate) fn browser_trust(ca_pem: Option<&str>) -> Result<core::TokenTrust> {
    match ca_pem {
        Some(pem) => core::TokenTrust::pem(pem.as_bytes())
            .map_err(|_| Error::from_reason("caPem: no valid PEM certificate")),
        None => Ok(core::TokenTrust::webpki_roots()),
    }
}

/// `ShamirClient.connectWsBrowser`. The host is resolved to its first
/// address under the handshake budget, and the core then gets only the
/// remaining part of that budget for the connect.
pub(crate) fn connect_ws_browser(
    env: &Env,
    opts: ConnectWsBrowserOptions,
) -> Result<PromiseRaw<'_, ShamirClient>> {
    let (password, (port, trusted_pin, trust)) =
        secrets::protect_before(opts.password.into_bytes(), || {
            Ok::<_, Error>((
                checked_port(opts.port)?,
                optional_pin(opts.trusted_pin)?,
                browser_trust(opts.ca_pem.as_deref())?,
            ))
        })?;
    let req_timeout = millis(opts.request_timeout_ms);
    let budget = millis(opts.handshake_timeout_ms);
    let mut core_opts = core::ConnectOptions {
        addr: unresolved_addr(),
        server_name: opts.server_name,
        username: opts.username,
        password,
        accept_new_host: opts.accept_new_host.unwrap_or(false),
        trusted_pin,
        connect_timeout: millis(opts.connect_timeout_ms),
        handshake_timeout: budget,
        request_timeout: req_timeout,
    };
    let origin = opts.origin;
    let host = opts.host;
    env.spawn_future(async move {
        let started = tokio::time::Instant::now();
        core_opts.addr = bounded(budget, "dns", core::resolve_host(&host, port))
            .await
            .map_err(infra_error)?;
        core_opts.handshake_timeout = budget.map(|b| b.saturating_sub(started.elapsed()));
        let client =
            core::Client::connect_ws_browser(core::BrowserWsOptions::new(core_opts, origin, trust))
                .await
                .map_err(infra_error)?;
        Ok(from_core(client, req_timeout))
    })
}
