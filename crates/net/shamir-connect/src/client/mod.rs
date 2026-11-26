//! Client-side of the connection protocol.

#[cfg(test)]
mod tests;

pub mod bootstrap;
pub mod changepw;
pub mod handshake;
pub mod rotation;

pub use crate::common::wire::RotationInProgressPayload;
pub use handshake::{
    AuthInit, ClientHandshake, HandshakeBuilder, HandshakeSuccess, ServerAuthOk, ServerChallenge,
    ServerChallengeV2,
};
pub use rotation::RotationRecovery;
