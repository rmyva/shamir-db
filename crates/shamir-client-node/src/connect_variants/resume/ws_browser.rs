use napi::bindgen_prelude::*;

use shamir_client as core;

use crate::client::{from_core, ShamirClient};
use crate::connect_variants::{browser_trust, ResumeWsBrowserOptions};
use crate::secrets;
use crate::{bounded, checked_port, copy_pin, infra_error, millis};

/// `ShamirClient.resumeWsBrowser`; DNS and the core resume run under ONE
/// handshake budget.
pub(crate) fn resume_ws_browser(
    env: &Env,
    opts: ResumeWsBrowserOptions,
) -> Result<PromiseRaw<'_, ShamirClient>> {
    let (mut ticket, (port, pinned_hash, trust)) =
        secrets::protect_before(opts.ticket.to_vec(), || {
            Ok::<_, Error>((
                checked_port(opts.port)?,
                copy_pin(&opts.trusted_pin)?,
                browser_trust(opts.ca_pem.as_deref())?,
            ))
        })?;
    let budget = millis(opts.handshake_timeout_ms);
    let req_timeout = millis(opts.request_timeout_ms);
    let host = opts.host;
    let server_name = opts.server_name;
    let origin = opts.origin;
    env.spawn_future(async move {
        let resumed = async move {
            let addr = core::resolve_host(&host, port).await?;
            // Ownership of the ticket bytes moves to the core, which re-wraps
            // them in its own zeroizing buffer before the first poll; only an
            // empty Vec stays behind in the local wrapper.
            let ticket = std::mem::take(&mut *ticket);
            core::Client::resume_ws_browser(core::BrowserResumeOptions::new(
                core::ResumeOptions {
                    addr,
                    server_name,
                    ticket,
                    pinned_hash,
                },
                origin,
                trust,
            ))
            .await
        };
        let client = bounded(budget, "resume", resumed)
            .await
            .map_err(infra_error)?;
        Ok(from_core(client, req_timeout))
    })
}
