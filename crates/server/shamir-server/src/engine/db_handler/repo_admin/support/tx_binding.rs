use std::sync::Arc;

use shamir_db::engine::repo::RepoInstance;
use shamir_db::ShamirDb;

/// Refuse execution when the repository name no longer identifies the instance
/// on which the interactive transaction was opened.
pub(in crate::engine::db_handler) async fn tx_execute_binding_refusal(
    db: &ShamirDb,
    db_name: &str,
    repo_name: &str,
    bound_repo: &RepoInstance,
) -> Option<String> {
    let resolved = db
        .get_db(db_name)
        .and_then(|database| database.get_repo(repo_name));
    let Some(resolved) = resolved else {
        return Some(format!(
            "Repository '{}' changed since the transaction began",
            repo_name
        ));
    };
    let (Some(bound_gate), Some(resolved_gate)) = (
        bound_repo.tx_gate().await.ok(),
        resolved.tx_gate().await.ok(),
    ) else {
        return Some(format!(
            "Repository '{}' changed since the transaction began",
            repo_name
        ));
    };
    (!Arc::ptr_eq(&bound_gate, &resolved_gate)).then(|| {
        format!(
            "Repository '{}' changed since the transaction began",
            repo_name
        )
    })
}
