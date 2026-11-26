use super::retained_freeze::RetainedFreeze;

/// Owned source state retained while a rename outcome is unresolved.
pub struct RenameInDoubt {
    pub from: String,
    pub to: String,
    pub freeze: RetainedFreeze,
}
