use super::event::JournalEventKind;

/// The step-specific part of a journal event appended under an owned fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaMigrationStep {
    pub kind: JournalEventKind,
    pub operation_index: Option<u32>,
    pub evidence: Option<String>,
}
