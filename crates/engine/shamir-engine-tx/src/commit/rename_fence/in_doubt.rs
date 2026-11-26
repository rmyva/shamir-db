use super::retained_freeze::RetainedFreeze;

/// Owned source state retained while a rename outcome is unresolved.
pub(crate) struct RenameInDoubt {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) freeze: RetainedFreeze,
}
