use napi::bindgen_prelude::*;

use shamir_client as core;

use crate::client::{from_core, ShamirClient};
use crate::connect_variants::ResumePlainOptions;
use crate::secrets;
use crate::{bounded, checked_port, copy_pin, infra_error, millis};

/// `ShamirClient.resumePlain`; DNS and the core resume run under ONE
/// handshake budget. The core refuses a non-loopback address before any
/// socket is opened; the server refuses a ticket issued under a stronger
/// binding (anti-downgrade).
pub(crate) fn resume_plain(
    env: &Env,
    opts: ResumePlainOptions,
) -> Result<PromiseRaw<'_, ShamirClient>> {
    let (mut ticket, (port, pinned_hash)) = secrets::protect_before(opts.ticket.to_vec(), || {
        Ok::<_, Error>((checked_port(opts.port)?, copy_pin(&opts.trusted_pin)?))
    })?;
    let budget = millis(opts.handshake_timeout_ms);
    let req_timeout = millis(opts.request_timeout_ms);
    let host = opts.host;
    env.spawn_future(async move {
        let resumed = async move {
            let addr = core::resolve_host(&host, port).await?;
            // Ownership of the ticket bytes moves to the core, which re-wraps
            // them in its own zeroizing buffer before the first poll; only an
            // empty Vec stays behind in the local wrapper.
            let ticket = std::mem::take(&mut *ticket);
            core::Client::resume_plain(core::ResumeOptions {
                addr,
                // Plain TCP has no TLS, so the core ignores the server name.
                server_name: String::new(),
                ticket,
                pinned_hash,
            })
            .await
        };
        let client = bounded(budget, "resume", resumed)
            .await
            .map_err(infra_error)?;
        Ok(from_core(client, req_timeout))
    })
}
