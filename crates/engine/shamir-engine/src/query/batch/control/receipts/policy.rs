use std::borrow::Cow;

use crate::query::batch::BatchTxPort;

use shamir_collections::{TFxMap, TFxSet};

use crate::query::batch::{BatchError, BatchOp, BatchRequest, BatchResolver, QueryEntry};
use shamir_query_types::batch::VisibilityWait;

/// Preflight the complete tree before dispatch. Transactional DML is covered
/// by the outer receipt; Call/admin writes are independently committed.
pub(in crate::query::batch) async fn validate(
    request: &BatchRequest,
    resolver: &dyn BatchResolver,
    root_metadata: Option<&shamir_query_types::batch::RequestMetadata>,
) -> Result<(), BatchError> {
    if !request.transactional
        && !request.queries.iter().any(|(alias, entry)| {
            root_metadata
                .and_then(|metadata| metadata.writes.get(alias).copied())
                .unwrap_or_else(|| entry.op.is_write())
                && !matches!(
                    &entry.op,
                    BatchOp::Insert(_) | BatchOp::Update(_) | BatchOp::Delete(_) | BatchOp::Set(_)
                )
        })
    {
        return Ok(());
    }
    let mut bodies = vec![(request, None)];
    // Built on first need, in one walk of the tree: only a body holding an
    // independently committed write and no explicit wait reads repository sets.
    let mut body_metadata: Option<TFxMap<usize, BodyMetadata>> = None;
    while let Some((body, inherited)) = bodies.pop() {
        let wait = body.visibility_wait.or(inherited);
        let mut bounded = matches!(wait, Some(VisibilityWait::Bounded { .. }));
        let unsupported = body.queries.iter().any(|(alias, entry)| {
            !matches!(
                &entry.op,
                BatchOp::Insert(_)
                    | BatchOp::Update(_)
                    | BatchOp::Delete(_)
                    | BatchOp::Set(_)
                    | BatchOp::Batch(_)
                    | BatchOp::ForEach(_)
            ) && (if std::ptr::eq(body, request) {
                root_metadata.and_then(|metadata| metadata.writes.get(alias).copied())
            } else {
                None
            })
            .unwrap_or_else(|| entry.op.is_write())
        });
        if wait.is_none() && unsupported {
            let repos = match root_metadata.filter(|_| std::ptr::eq(body, request)) {
                Some(metadata) => &metadata.repos,
                None => {
                    let summaries =
                        body_metadata.get_or_insert_with(|| collect_body_metadata(request).0);
                    &summaries
                        .get(&(body as *const BatchRequest as usize))
                        .ok_or_else(|| {
                            BatchError::query_coded(
                                "",
                                "internal",
                                "missing static receipt metadata",
                            )
                        })?
                        .repos
                }
            };
            for repo_name in repos {
                // An unresolvable repository has no commit policy to inherit:
                // execution reports the real error where it always did.
                if let Ok(repo) = resolver.resolve_repo(repo_name).await {
                    bounded |= matches!(
                        repo.default_visibility_wait(),
                        VisibilityWait::Bounded { .. }
                    );
                }
            }
        }
        for entry in body.queries.values() {
            if let BatchOp::Call(call) = &entry.op {
                if wait.is_none() {
                    if let Ok(repo) = resolver.resolve_repo(&call.repo).await {
                        bounded |= matches!(
                            repo.default_visibility_wait(),
                            VisibilityWait::Bounded { .. }
                        );
                    }
                }
            }
        }
        for (alias, entry) in &body.queries {
            let nested = match &entry.op {
                BatchOp::Batch(sub) => Some(&sub.batch),
                BatchOp::ForEach(fe) => Some(&fe.batch),
                _ => None,
            };
            if let Some(nested) = nested {
                if body.transactional
                    && matches!(nested.visibility_wait, Some(VisibilityWait::Bounded { .. }))
                {
                    return Err(BatchError::query_coded(
                        alias,
                        "validation",
                        "nested commit policy cannot override an open outer transaction",
                    ));
                }
                bodies.push((nested, wait));
                continue;
            }
            if bounded
                && entry.op.is_write()
                && !matches!(
                    &entry.op,
                    BatchOp::Insert(_) | BatchOp::Update(_) | BatchOp::Delete(_) | BatchOp::Set(_)
                )
            {
                return Err(BatchError::query_coded(
                    alias,
                    "validation",
                    "bounded visibility is unsupported by independently committed Call/admin writes",
                ));
            }
        }
    }
    Ok(())
}

/// Metadata stays local to this immutable request borrow; its address keys are
/// never retained in RequestMetadata or an execution scope.
#[derive(Default)]
struct BodyMetadata {
    repos: TFxSet<String>,
}

// Returns the repository set of every body (nested ones included) and the
// number of entries enumerated to build them: each entry is visited once to
// find the bodies and once to summarise its body.
fn collect_body_metadata(request: &BatchRequest) -> (TFxMap<usize, BodyMetadata>, usize) {
    let mut ordered = Vec::new();
    let mut pending = vec![request];
    let mut visits = 0;
    while let Some(body) = pending.pop() {
        ordered.push(body);
        for entry in body.queries.values() {
            visits += 1;
            match &entry.op {
                BatchOp::Batch(sub) => pending.push(&sub.batch),
                BatchOp::ForEach(fe) => pending.push(&fe.batch),
                _ => {}
            }
        }
    }
    let mut metadata: TFxMap<usize, BodyMetadata> = TFxMap::default();
    for body in ordered.into_iter().rev() {
        let mut summary = BodyMetadata::default();
        for entry in body.queries.values() {
            visits += 1;
            if let Some(table) = entry.op.table_ref() {
                summary.repos.insert(table.repo.clone());
            }
            match &entry.op {
                BatchOp::Batch(sub) => {
                    let nested = &sub.batch;
                    if let Some(child) = metadata.get(&(nested as *const BatchRequest as usize)) {
                        summary.repos.extend(child.repos.iter().cloned());
                    }
                }
                BatchOp::ForEach(fe) => {
                    let nested = &fe.batch;
                    if let Some(child) = metadata.get(&(nested as *const BatchRequest as usize)) {
                        summary.repos.extend(child.repos.iter().cloned());
                    }
                }
                _ => {}
            }
        }
        metadata.insert(body as *const BatchRequest as usize, summary);
    }
    (metadata, visits)
}

#[cfg(test)]
pub(super) fn body_metadata_test_probe(
    request: &BatchRequest,
) -> (usize, TFxMap<usize, TFxSet<String>>) {
    let (metadata, visits) = collect_body_metadata(request);
    (
        visits,
        metadata
            .into_iter()
            .map(|(key, summary)| (key, summary.repos))
            .collect(),
    )
}

/// Clone only a container requiring policy inheritance, never a leaf hot path.
pub(in crate::query::batch) fn inherit(
    entry: &QueryEntry,
    wait: Option<VisibilityWait>,
) -> Cow<'_, QueryEntry> {
    let missing = match &entry.op {
        BatchOp::Batch(sub) => sub.batch.visibility_wait.is_none(),
        BatchOp::ForEach(fe) => fe.batch.visibility_wait.is_none(),
        _ => false,
    };
    if !missing || wait.is_none() {
        return Cow::Borrowed(entry);
    }
    let mut entry = entry.clone();
    match &mut entry.op {
        BatchOp::Batch(sub) => sub.batch.visibility_wait = wait,
        BatchOp::ForEach(fe) => fe.batch.visibility_wait = wait,
        _ => {}
    }
    Cow::Owned(entry)
}
