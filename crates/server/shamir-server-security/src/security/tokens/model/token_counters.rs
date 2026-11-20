use shamir_connect::server::token::Endpoint;

/// Cheap process-local service counters.
#[derive(Debug, Default)]
pub struct TokenCounters {
    created: std::sync::atomic::AtomicU64,
    revoked: std::sync::atomic::AtomicU64,
    verified_ok: std::sync::atomic::AtomicU64,
    verified_failed: std::sync::atomic::AtomicU64,
    post_auth_rejected: std::sync::atomic::AtomicU64,
}

impl TokenCounters {
    /// Number of token creations accepted.
    pub fn created(&self) -> u64 {
        self.created.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of first-time token revocations accepted.
    pub fn revoked(&self) -> u64 {
        self.revoked.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of successful token verifications (malformed headers rejected before verification are excluded).
    pub fn verified_ok(&self) -> u64 {
        self.verified_ok.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of failed token verifications (malformed headers rejected before verification are excluded).
    pub fn verified_failed(&self) -> u64 {
        self.verified_failed
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of authenticated token requests rejected by the REST or WS request bucket.
    pub fn post_auth_rejected_total(&self) -> u64 {
        self.post_auth_rejected
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Records one post-auth request-bucket rejection, regardless of reply delivery.
    pub fn record_post_auth_rejection(&self) {
        self.post_auth_rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_created(&self) {
        self.created
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_revoked(&self) {
        self.revoked
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn count_auth_result(&self, channel: Endpoint, result: &'static str) {
        let channel = match channel {
            Endpoint::Rest => "rest",
            Endpoint::Ws => "ws",
        };
        metrics::counter!("shamir_token_auth_total", "channel" => channel, "result" => result)
            .increment(1);
    }

    pub(crate) fn count_verified(&self, success: bool) {
        let counter = if success {
            &self.verified_ok
        } else {
            &self.verified_failed
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
