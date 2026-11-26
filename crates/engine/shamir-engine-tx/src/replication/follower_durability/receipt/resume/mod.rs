//! Resume (S5c-X PR6b-2): find, validate, and match an outstanding receipt
//! left by a crashed follower apply.

pub(crate) mod activation;
pub(crate) mod identity;
pub(crate) mod lookup;
pub(crate) mod worker;

pub(crate) use lookup::{lookup, match_offered, tail_events};
