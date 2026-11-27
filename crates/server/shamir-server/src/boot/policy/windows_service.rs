//! Windows Service Control Manager (SCM) integration.
//!
//! When the binary is launched by the SCM (via `run --service`), `main`
//! delegates to [`run`], which calls the blocking SCM dispatcher. The
//! dispatcher invokes [`service_main`] on an SCM thread, which boots the
//! server and reports `Running`. On `Stop` / `Shutdown` the control handler
//! signals the async shutdown future, draining the server gracefully before
//! reporting `Stopped`.

use std::ffi::OsString;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_dispatcher;

use crate::boot::{config::Config, runtime};
use crate::engine::server::BootstrapMode;
use crate::service::SERVICE_NAME;

static SERVICE_PROCESS_EXIT: AtomicU8 = AtomicU8::new(0);

windows_service::define_windows_service!(ffi_service_main, service_main);

/// Entry point called from `main` when `--service` is present.
///
/// This is a BLOCKING call that hands the thread to the SCM dispatcher.
/// It must run BEFORE any tokio runtime is built. The dispatcher calls
/// [`service_main`] on an SCM thread.
pub fn run() -> anyhow::Result<u8> {
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)
        .map_err(|e| anyhow::anyhow!("SCM dispatcher failed: {e}"))?;
    Ok(SERVICE_PROCESS_EXIT.load(Ordering::Acquire))
}

/// Called by the SCM on its own thread. Re-parses CLI args (the SCM
/// launched the process with the full ImagePath), boots the server, and
/// blocks until Stop is requested.
fn service_main(_args: Vec<OsString>) {
    if let Err(e) = service_main_inner() {
        // Best-effort log — tracing may not be initialised yet.
        SERVICE_PROCESS_EXIT.store(1, Ordering::Release);
        eprintln!("shamir-server service_main failed: {e}");
    }
}

fn service_main_inner() -> anyhow::Result<()> {
    // The SCM _args are the service-name + any extra args from the SCM
    // start command — NOT the ImagePath argv. Re-parse from the real
    // process args so we get --config, --bootstrap-*, etc.
    // We need the Cli struct from main.rs. Since it is private to the
    // binary crate, we re-derive the config path from std::env::args
    // directly. The ImagePath is:
    //   "<exe>" --config "<config>" run --service
    // so standard clap parsing of std::env::args works.
    let args: Vec<String> = std::env::args().collect();

    // Parse --config from args (find the --config flag).
    let config_pos = args
        .iter()
        .position(|a| a == "--config")
        .ok_or_else(|| anyhow::anyhow!("--config not found in process args"))?;
    let config_path = args
        .get(config_pos + 1)
        .ok_or_else(|| anyhow::anyhow!("--config value missing"))?;
    let config_path = std::path::PathBuf::from(config_path);

    // Parse optional bootstrap flags.
    let skip_bootstrap = args.iter().any(|a| a == "--skip-bootstrap");
    let bootstrap_user = args
        .iter()
        .position(|a| a == "--bootstrap-user")
        .and_then(|i| args.get(i + 1).cloned());
    let bootstrap_password = args
        .iter()
        .position(|a| a == "--bootstrap-password")
        .and_then(|i| args.get(i + 1).cloned());
    let bootstrap_token_path = args
        .iter()
        .position(|a| a == "--bootstrap-token-path")
        .and_then(|i| args.get(i + 1).cloned())
        .map(std::path::PathBuf::from);

    // Install rustls crypto provider.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Load config.
    let config = Config::from_file(&config_path)?;
    config.validate()?;

    // Resolve relative log file path (RM-6).
    let mut config = config;
    if let Some(ref rel) = config.logging.file {
        let p = std::path::Path::new(rel);
        if !p.is_absolute() {
            config.logging.file = Some(
                crate::service::absolute(p)?
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("log file path is not valid UTF-8"))?
                    .to_string(),
            );
        }
    }
    // The SAME boot path as the foreground server: refuse a sink that would
    // land in the audit-log namespace, then install the subscriber. The
    // service is always the server path, so it always honours `logging.file`.
    let _log_guard = crate::logging::boot_logging(
        &config.logging,
        &config.data_dir,
        crate::logging::SinkMode::Config,
    )?;

    // Shutdown signal: the SCM control handler signals this Notify when
    // Stop or Shutdown is requested.
    let stop_notify = Arc::new(Notify::new());
    let stop_notify_handler = Arc::clone(&stop_notify);

    // Register the SCM control handler.
    let status_handle =
        service_control_handler::register(SERVICE_NAME, move |control| match control {
            ServiceControl::Stop | ServiceControl::Shutdown => {
                stop_notify_handler.notify_one();
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        })
        .map_err(|e| anyhow::anyhow!("failed to register SCM control handler: {e}"))?;

    // Report StartPending.
    report_status(
        &status_handle,
        ServiceState::StartPending,
        ServiceControlAccept::empty(),
        Duration::from_secs(30),
    )?;

    // Construct bootstrap policy.
    let bootstrap = if skip_bootstrap {
        BootstrapMode::Skip
    } else if let Some(pw) = bootstrap_password {
        BootstrapMode::Password {
            username: bootstrap_user
                .unwrap_or_else(|| crate::boot::bootstrap::DEFAULT_BOOTSTRAP_NAME.to_string()),
            password: zeroize::Zeroizing::new(pw.into_bytes()),
        }
    } else {
        BootstrapMode::RandomToken {
            username: bootstrap_user,
            token_path: bootstrap_token_path,
        }
    };

    // Build the tokio runtime.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build tokio runtime: {e}"))?;

    tracing::info!("shamir-server starting as Windows service");

    // Bridge the SCM stop signal into an async future.
    let shutdown = async move {
        stop_notify.notified().await;
    };

    // on_ready closure: report Running to the SCM only after listeners bind.
    // `ServiceStatusHandle` is `Copy`, so capturing it in the closure is fine.
    let on_ready = move || {
        let _ = report_status(
            &status_handle,
            ServiceState::Running,
            ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            Duration::ZERO,
        );
        tracing::info!("shamir-server running as Windows service");
    };

    // Run the server. A panic unwinding out of the polled drain must never
    // escape into the non-unwinding SCM callback: catch it at this runtime
    // owner, map it to the defined non-zero panic status (the same 70 the
    // binary exits with), and take the SAME bounded runtime shutdown as the
    // error path through `exits_through_bounded_shutdown` (the mapped `Err`
    // counts as boot_failed).
    let mut panicked = false;
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(runtime::serve(config, bootstrap, shutdown, on_ready))
    })) {
        Ok(result) => result,
        Err(_panic) => {
            panicked = true;
            tracing::error!("shamir-server service: the shutdown drain panicked");
            Err(anyhow::anyhow!("the server run panicked"))
        }
    };

    // On return (normal or error), ALWAYS report Stopped.
    // `SERVICE_PROCESS_EXIT` mirrors the report's `exit_code()` and nothing
    // forces the process exit: `set_service_status(Stopped)` is only reported
    // after `serve` returned, which is why `serve` waits for the scheduled
    // worker and keeps the data-directory lock until then.
    let panic_status = u32::from(runtime::PANIC_EXIT_CODE);
    SERVICE_PROCESS_EXIT.store(
        if panicked {
            runtime::PANIC_EXIT_CODE
        } else {
            result.as_ref().map_or(1, |report| report.exit_code())
        },
        Ordering::Release,
    );

    // Report StopPending then Stopped.
    let exit_code = if panicked {
        ServiceExitCode::Win32(panic_status)
    } else {
        match &result {
            Ok(report) => ServiceExitCode::Win32(u32::from(report.exit_code())),
            Err(_) => ServiceExitCode::Win32(1),
        }
    };

    let _ = report_status(
        &status_handle,
        ServiceState::StopPending,
        ServiceControlAccept::empty(),
        Duration::from_secs(10),
    );

    let _ = status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code,
        checkpoint: 0,
        wait_hint: Duration::ZERO,
        process_id: None,
    });

    if let Err(e) = &result {
        tracing::error!("shamir-server service stopped with error: {e}");
    } else if result.as_ref().is_ok_and(|report| report.is_clean()) {
        tracing::info!("shamir-server service stopped cleanly");
    }

    // A failed run — OR an abnormal shutdown report (exit code 70) — can
    // leave a detached blocking job: dropping the runtime would wait for it
    // without a bound and keep the process alive. Both leave through the SAME
    // bounded runtime shutdown with the same named bound as `main.rs`; the
    // data-directory lock is leaked by the owner guard, so the OS keeps it
    // until the process is gone.
    if runtime::exits_through_bounded_shutdown(
        result.is_err(),
        result.as_ref().map(|report| !report.is_clean()).ok(),
    ) {
        rt.shutdown_timeout(runtime::EXIT_RUNTIME_SHUTDOWN_TIMEOUT);
    }
    result.map(|_| ())
}

/// Helper to report a service status to the SCM.
fn report_status(
    handle: &service_control_handler::ServiceStatusHandle,
    state: ServiceState,
    accepted: ServiceControlAccept,
    wait_hint: Duration,
) -> anyhow::Result<()> {
    handle
        .set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: state,
            controls_accepted: accepted,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint,
            process_id: None,
        })
        .map_err(|e| anyhow::anyhow!("failed to set service status: {e}"))
}
