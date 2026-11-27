// `tx::drain` — background drain and WAL deletion admission.
// R2a-1d: the owned RAII pause guard over the drainer's `replaying` lock,
// consumed by the capture/seal barriers.
pub mod drain_pause;
#[allow(dead_code)]
pub mod drainer;
pub(crate) mod truncation_lease;
pub mod truncation_pin;
// Widened to `pub` in R2a-1d: `RepoInstance::acquire_truncation_pin`
// (R2a-3 server-adapter facade) returns the guard across the crate edge.
pub mod truncation_pin_guard;
