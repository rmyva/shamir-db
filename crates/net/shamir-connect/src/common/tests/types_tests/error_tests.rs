//! RV-CONNECT-94E37810 — exhaustive collapse-set oracle for `Error::to_wire`.
//!
//! Every variant of [`Error`] is explicitly classified here. Adding a new
//! variant without extending the match below is a compile error (the
//! oracle "fails"), so the §14 collapse set can never silently drift.

use crate::common::model::admit::AdmitReject;
use crate::common::model::error::{EnvironmentError, Error};

use std::mem::discriminant;

/// Classify one variant through `to_wire` and assert it lands on the
/// expected variant (same discriminant; `Error` has no `PartialEq`).
fn assert_collapse(input: Error, expected: Error) {
    assert_eq!(
        discriminant(&input.to_wire()),
        discriminant(&expected),
        "to_wire classification drifted for {input:?}"
    );
}

/// Exhaustive oracle over the full error vocabulary.
#[test]
fn rv_connect_94e37810_to_wire_collapse_set_oracle() {
    fn classify(error: Error) {
        let expected = match error {
            Error::AuthFailed => Error::AuthFailed,
            Error::RateLimited { retry_after } => Error::RateLimited { retry_after },
            Error::ServerBusy { retry_after } => Error::ServerBusy { retry_after },
            Error::UnsupportedVersion => Error::UnsupportedVersion,
            Error::HandshakeVersionMismatch | Error::ChallengeLayout => Error::AuthFailed,
            Error::BootstrapFailed => Error::BootstrapFailed,
            Error::Environment(_)
            | Error::ServerAuthFailed
            | Error::ServerSignatureInvalid
            | Error::ServerIdentityChanged
            | Error::KdfParamsRejected
            | Error::KnownHostsIntegrityFailed
            | Error::UserNotFound
            | Error::InvalidInput(_)
            | Error::InvalidUsername(_)
            | Error::InvalidPassword(_)
            | Error::Crypto(_)
            | Error::Encoding(_)
            | Error::StorageFailed
            | Error::AdmissionRejected { .. } => Error::AuthFailed,
        };
        assert_collapse(error, expected);
    }
    classify(Error::AuthFailed);
    classify(Error::RateLimited { retry_after: 7 });
    classify(Error::ServerBusy { retry_after: 9 });
    classify(Error::UnsupportedVersion);
    classify(Error::HandshakeVersionMismatch);
    classify(Error::ChallengeLayout);
    classify(Error::BootstrapFailed);
    classify(Error::Environment(EnvironmentError::Entropy));
    classify(Error::ServerAuthFailed);
    classify(Error::ServerSignatureInvalid);
    classify(Error::ServerIdentityChanged);
    classify(Error::KdfParamsRejected);
    classify(Error::KnownHostsIntegrityFailed);
    classify(Error::UserNotFound);
    classify(Error::InvalidInput("x"));
    classify(Error::InvalidUsername("x"));
    classify(Error::InvalidPassword("x"));
    classify(Error::Crypto("x"));
    classify(Error::Encoding("x".into()));
    classify(Error::StorageFailed);
    classify(Error::AdmissionRejected {
        reason: AdmitReject::AllActive,
        username: "alice".into(),
    });
}
