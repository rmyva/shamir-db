use napi::bindgen_prelude::*;

use shamir_client as core;

use crate::client::{from_core, ShamirClient};
use crate::connect_variants::ResumeLocalOptions;
use crate::secrets;
use crate::{bounded, copy_pin, infra_error, millis};

/// `ShamirClient.resumeLocal`: the whole resume (dial + ticket exchange)
/// runs under one handshake budget; there is no DNS.
pub(crate) fn resume_local(
    env: &Env,
    opts: ResumeLocalOptions,
) -> Result<PromiseRaw<'_, ShamirClient>> {
    let (ticket, pinned_hash) =
        secrets::protect_before(opts.ticket.to_vec(), || copy_pin(&opts.trusted_pin))?;
    let budget = millis(opts.handshake_timeout_ms);
    let req_timeout = millis(opts.request_timeout_ms);
    let core_opts = core::ResumeLocalOptions {
        addr: opts.addr,
        ticket,
        pinned_hash,
        connect_timeout: millis(opts.connect_timeout_ms),
    };
    env.spawn_future(async move {
        let client = bounded(budget, "resume", core::Client::resume_local(core_opts))
            .await
            .map_err(infra_error)?;
        Ok(from_core(client, req_timeout))
    })
}
