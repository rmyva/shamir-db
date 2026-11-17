//! Misc integration tests.

#[path = "audit_log/audit_appender.rs"]
mod audit_appender;
#[path = "audit_log/audit_rotation.rs"]
mod audit_rotation;
#[path = "../common/mod.rs"]
mod common;
#[path = "runtime/create_function_gating.rs"]
mod create_function_gating;
#[path = "runtime/db_handler/mod.rs"]
mod db_handler;
#[path = "runtime/hmac_gate/mod.rs"]
mod hmac_gate;
#[path = "audit_log/logging_batch.rs"]
mod logging_batch;
#[path = "audit_log/observability_http.rs"]
mod observability_http;
#[path = "reference_compat.rs"]
mod reference_compat;
#[path = "runtime/runtime_serve.rs"]
mod runtime_serve;
#[path = "runtime/scheduler.rs"]
mod scheduler;
#[path = "runtime/server_meta.rs"]
mod server_meta;
#[path = "audit_log/slow_query_log.rs"]
mod slow_query_log;
