//! Staged system-catalogue preparation and post-boot consistency pass for
//! an online-copy restore (design `online-backup.md` §5). Async bodies run
//! on the dedicated phase threads of `restore/online.rs`.
//!
//! Captured manifest keys are QUALIFIED `db/repo` (split exactly once).
//! Rows match by catalogue tuple `(db, repo)` — never by path heuristics.
//! A captured row's native path must equal the manifest `store_dir`
//! resolved against the stage EXACTLY (lexical, then canonical as a case
//! alias defense): the staged catalogue only produces that exact form for
//! a row originally stored relative, so the restore relocation guarantee
//! holds after the swap. Known limitation (reported, no new API): a raw
//! ABSOLUTE persisted row whose resolved form coincides byte-for-byte with
//! the staged path is indistinguishable through `load_repositories`;
//! `load_replay_scopes` (shamir-db) re-checks the raw persisted path
//! against the manifest `store_dir` this module supplies.
//!
//! Non-captured rows are REMOVED from the STAGED catalogue
//! (`SystemStore::remove_repository`, durable: implicit transaction +
//! Durable-DDL tail, so boot's WAL replay cannot resurrect them) and
//! reported; rows whose resolved path lies OUTSIDE the stage are dropped
//! before any canonicalization (never follow external paths). A
//! non-captured row sharing or overlapping a captured route refuses (Q9
//! rename during capture — fail closed). A captured repo without a row is
//! an allowed orphan (`remove_repo_locked` deletes only the repo row,
//! retaining table rows); its replay scope still comes from the same
//! `(db, repo)` tuple and replay is still invoked, so an unprovable point
//! fails typed even at `V* = 0`. A captured repo without a manifest WAL
//! route refuses — no sibling fallback.
//!
//! Phase A opens the staged catalogue offline (`SystemStore::init`, no
//! user store) and fingerprints the staged meta and every captured
//! store/WAL route as genuine Fjall, so a normal boot cannot silently
//! initialize a fake empty directory. Phase B opens the staged copy
//! through the same init normal boot performs
//! (`ShamirDb::init_with_durability(..., default)`), then re-verifies rows
//! through `db.system_store()` and `db.get_db(..).has_repo(..)` before
//! everything drops. Store contents ABOVE `V*` are tolerated; no
//! version-watermark rejection.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use shamir_db::engine::db_instance::replay_to::ReplayScope;
use shamir_db::shamir_db::system_store::{load_replay_scopes, SystemStore, SystemStoreConfig};
use shamir_db::{is_reserved_db_name, ShamirDb, RESERVED_DB_NAMES};
use shamir_types::types::value::QueryValue;

use super::fjall_locks::require_fjall_store;
use super::RestoreError;

/// One captured repository: manifest key split EXACTLY once into
/// `db/repo`, plus its consistency data (WAL route REQUIRED).
#[derive(Debug, Clone)]
pub(super) struct CapturedRepo {
    pub db: String,
    pub repo: String,
    /// Store dir relative to the snapshot root, forward-slash (verified
    /// confined by the v2 verifier).
    pub store_dir: String,
    /// Manifest WAL route: directory relative to the snapshot root and its
    /// listed segment count.
    pub wal: (String, usize),
    /// The recorded point `V*`.
    pub v_star: u64,
}

impl CapturedRepo {
    /// Split one manifest captured key into `(db, repo)` exactly once.
    pub(super) fn key(key: &str) -> Result<(String, String), RestoreError> {
        let (db, repo) = key
            .split_once('/')
            .ok_or_else(|| RestoreError::Consistency {
                repo: key.to_string(),
                reason: "captured manifest key is not qualified 'db/repo'".into(),
            })?;
        if db.is_empty() || repo.is_empty() {
            return Err(RestoreError::Consistency {
                repo: key.to_string(),
                reason: "captured manifest key has an empty db or repo part".into(),
            });
        }
        if repo.contains('/') {
            return Err(RestoreError::Consistency {
                repo: key.to_string(),
                reason: "captured manifest key splits into more than two \
                         (ambiguous 'db/repo')"
                    .into(),
            });
        }
        Ok((db.to_string(), repo.to_string()))
    }

    pub(super) fn label(&self) -> String {
        format!("{}/{}", self.db, self.repo)
    }
}

/// One replay unit, executed by phase A.
#[derive(Debug)]
pub(super) struct ReplayJob {
    pub label: String,
    pub store_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub v_star: u64,
    pub scope: ReplayScope,
}

/// Phase A output.
#[derive(Debug)]
pub(super) struct PreparedStagedCatalogue {
    pub replay_jobs: Vec<ReplayJob>,
    /// Captured-with-row `(db, repo)` → their exact staged native path.
    pub claimed: BTreeMap<(String, String), PathBuf>,
    /// Captured directories with NO row: tolerated orphans whose
    /// directories must still exist.
    pub orphan_dirs: Vec<PathBuf>,
    /// Non-captured repositories (`db/repo`), removed and reported.
    pub excluded: BTreeSet<String>,
}

pub(super) async fn validate_staged_db_names(stage: &Path) -> Result<(), RestoreError> {
    // Opening the staged catalogue only (never user repositories) makes the check safe pre-publication.
    let meta_path = stage.join("shamir_db_meta.redb");
    if !meta_path.exists() {
        return Ok(());
    }
    require_fjall_store(&meta_path, "(staged system catalogue)")?;
    let store = Box::pin(SystemStore::init(SystemStoreConfig::Fjall(meta_path)))
        .await
        .map_err(RestoreError::Catalogue)?;
    let rows = Box::pin(store.load_databases())
        .await
        .map_err(RestoreError::Catalogue)?;
    validate_catalogue_db_name_records(&rows)
}

const RESERVED_DATABASE_RECOVERY: &str = "is reserved by the server; recover by exporting the existing reserved name with the previous release's offline logical-export (for example, --db users), importing with this release into an absent target with --as-db <non-reserved name>, and starting the new server there. A catalogue rename alone does not restore the physical repository path; if export refuses unsupported content, stay on the previous release";

pub(super) fn validate_catalogue_db_name_records(rows: &[QueryValue]) -> Result<(), RestoreError> {
    // This mirrors shamir-db's crate-private validate_catalogue_db_names on
    // the shared predicate; the predicate remains the single source of truth.
    for row in rows {
        let Some(name) = row.get("name").and_then(QueryValue::as_str) else {
            continue;
        };
        if name == "__system__" {
            continue;
        }
        if is_reserved_db_name(name) {
            return Err(RestoreError::Catalogue(shamir_db::DbError::Validation(
                format!("database name '{name}' {RESERVED_DATABASE_RECOVERY}"),
            )));
        }
    }
    Ok(())
}

/// Phase A. The staged [`SystemStore`] handle drops at the end; the phase's
/// runtime drains its background tasks before phase B opens the same
/// staged meta store.
pub(super) async fn prepare_staged_catalogue(
    stage: &Path,
    captured: &[CapturedRepo],
) -> Result<PreparedStagedCatalogue, RestoreError> {
    let meta_path = stage.join("shamir_db_meta.redb");
    require_fjall_store(&meta_path, "(staged system catalogue)")?;
    let store = Box::pin(SystemStore::init(SystemStoreConfig::Fjall(meta_path)))
        .await
        .map_err(RestoreError::Catalogue)?;

    // Captured routes: exact lexical stage join, fingerprinted genuine
    // Fjall, existing (preflight BEFORE any boot could create them), then
    // canonicalized once for alias/overlap comparisons. Duplicate or
    // overlapping captured routes (store OR wal) refuse here.
    let mut routes: BTreeMap<(String, String), PathBuf> = BTreeMap::new();
    let mut wal_routes: Vec<(String, PathBuf)> = Vec::with_capacity(captured.len());
    for repo in captured {
        reject_reserved_store_route(repo)?;
        let store_path = join_manifest_rel(stage, &repo.store_dir);
        require_fjall_store(&store_path, &repo.label())?;
        let store_canonical =
            canonical(&store_path, &repo.label(), "captured repository directory")?;
        for other_canonical in routes.values() {
            if *other_canonical == store_canonical
                || other_canonical.starts_with(&store_canonical)
                || store_canonical.starts_with(other_canonical)
            {
                return Err(RestoreError::Consistency {
                    repo: repo.label(),
                    reason: "captured store_dir duplicates or overlaps another \
                             captured store_dir"
                        .into(),
                });
            }
        }
        if routes
            .insert((repo.db.clone(), repo.repo.clone()), store_canonical)
            .is_some()
        {
            return Err(RestoreError::Consistency {
                repo: repo.label(),
                reason: "duplicate captured manifest key".into(),
            });
        }
        let wal_path = join_manifest_rel(stage, &repo.wal.0);
        reject_reserved_wal_dir(first_top_level(&wal_path, stage).as_deref(), repo)?;
        let wal_canonical = canonical(&wal_path, &repo.label(), "captured wal_dir")?;
        for (other_label, other_wal) in &wal_routes {
            if *other_wal == wal_canonical
                || other_wal.starts_with(&wal_canonical)
                || wal_canonical.starts_with(other_wal)
            {
                return Err(RestoreError::Consistency {
                    repo: repo.label(),
                    reason: format!("captured wal_dir duplicates or overlaps {other_label}"),
                });
            }
        }
        for store_canonical in routes.values() {
            if *store_canonical == wal_canonical
                || store_canonical.starts_with(&wal_canonical)
                || wal_canonical.starts_with(store_canonical)
            {
                return Err(RestoreError::Consistency {
                    repo: repo.label(),
                    reason: "captured wal_dir overlaps a captured store_dir".into(),
                });
            }
        }
        wal_routes.push((repo.label(), wal_canonical));
    }

    let rows = Box::pin(store.load_repositories())
        .await
        .map_err(RestoreError::Catalogue)?;
    let mut excluded: BTreeSet<String> = BTreeSet::new();
    let mut row_tuples: BTreeSet<(String, String)> = BTreeSet::new();
    for row in &rows {
        let db = field(row, "db_name")?;
        let repo_name = field(row, "repo_name")?;
        let tuple = (db.to_string(), repo_name.to_string());
        let label = format!("{db}/{repo_name}");
        if let Some(route) = routes.get(&tuple) {
            verify_captured_row_path(
                stage,
                &label,
                row.get("path").and_then(QueryValue::as_str),
                route,
            )?;
            row_tuples.insert(tuple);
            continue;
        }
        // Non-captured row. An external (outside-stage) resolved path is
        // dropped WITHOUT canonicalization (never follow external paths);
        // an in-stage one is canonicalized only to detect overlap with a
        // captured route (Q9 rename — fail closed).
        if let Some(path) = row.get("path").and_then(QueryValue::as_str) {
            let row_path = PathBuf::from(path);
            if row_path.starts_with(stage) {
                if let Ok(row_canonical) = row_path.canonicalize() {
                    for route in routes.values() {
                        if *route == row_canonical
                            || row_canonical.starts_with(route)
                            || route.starts_with(&row_canonical)
                        {
                            return Err(RestoreError::Consistency {
                                repo: label,
                                reason: "non-captured catalogue row shares or overlaps \
                                         a captured repository directory (rename during \
                                         capture?)"
                                    .into(),
                            });
                        }
                    }
                }
            }
        }
        // Durable exclusion from the STAGED catalogue (persisted AND
        // in-memory rows alike — boxed await; see module docs).
        Box::pin(store.remove_repository(db, repo_name))
            .await
            .map_err(RestoreError::Catalogue)?;
        excluded.insert(label);
    }

    // Replay scopes for EVERY captured tuple (row present or orphan);
    // load_replay_scopes re-checks the raw persisted path against the
    // manifest store_dir this map supplies.
    let scope_keys: BTreeMap<(&str, &str), &str> = captured
        .iter()
        .map(|repo| {
            (
                (repo.db.as_str(), repo.repo.as_str()),
                repo.store_dir.as_str(),
            )
        })
        .collect();
    let mut scopes: BTreeMap<(String, String), ReplayScope> =
        Box::pin(load_replay_scopes(&store, &scope_keys))
            .await
            .map_err(RestoreError::Catalogue)?;

    // Replay jobs: EVERY captured repo is invoked — replay_to types an
    // empty/missing WAL or an unprovable point, so nothing is silently
    // skipped, even at `V* = 0`.
    let mut replay_jobs = Vec::with_capacity(routes.len());
    let mut claimed = BTreeMap::new();
    let mut orphan_dirs = Vec::new();
    for repo in captured {
        let tuple = (repo.db.clone(), repo.repo.clone());
        let route = &routes[&tuple];
        let scope = scopes
            .remove(&tuple)
            .expect("scope preallocated per captured tuple");
        replay_jobs.push(ReplayJob {
            label: repo.label(),
            // Both replay paths use the SAME lexical stage join, so
            // replay_to's exact/sibling comparison never sees a verbatim
            // canonical prefix on one side only.
            store_dir: join_manifest_rel(stage, &repo.store_dir),
            wal_dir: join_manifest_rel(stage, &repo.wal.0),
            v_star: repo.v_star,
            scope,
        });
        if row_tuples.contains(&tuple) {
            claimed.insert(tuple, route.clone());
        } else {
            orphan_dirs.push(route.clone());
        }
    }

    drop(store);
    Ok(PreparedStagedCatalogue {
        replay_jobs,
        claimed,
        orphan_dirs,
        excluded,
    })
}

/// Phase B: open the staged copy through the same init normal boot
/// performs, then verify, BEFORE dropping every handle: the catalogue row
/// set equals the captured-with-row set exactly, each row's native path
/// still equals its captured staged path, `has_repo` holds for every
/// claimed tuple, and every orphan directory still exists. Store contents
/// above `V*` are tolerated; no version-watermark rejection.
pub(super) async fn boot_and_check_staged(
    stage: &Path,
    prepared: &PreparedStagedCatalogue,
) -> Result<(), RestoreError> {
    let meta_path = stage.join("shamir_db_meta.redb");
    let db = Box::pin(ShamirDb::init_with_durability(
        SystemStoreConfig::Fjall(meta_path),
        Default::default(),
    ))
    .await
    .map_err(RestoreError::Catalogue)?;

    let mut rows: BTreeMap<(String, String), String> = BTreeMap::new();
    for row in Box::pin(db.system_store().load_repositories())
        .await
        .map_err(RestoreError::Catalogue)?
    {
        let db_name = field(&row, "db_name")?;
        let repo_name = field(&row, "repo_name")?;
        let path = row
            .get("path")
            .and_then(QueryValue::as_str)
            .ok_or_else(|| RestoreError::Consistency {
                repo: format!("{db_name}/{repo_name}"),
                reason: "booted repository row has no native path".into(),
            })?;
        rows.insert(
            (db_name.to_string(), repo_name.to_string()),
            path.to_string(),
        );
    }

    let expected: BTreeSet<(String, String)> = prepared.claimed.keys().cloned().collect();
    let booted: BTreeSet<(String, String)> = rows.keys().cloned().collect();
    if let Some(pair) = booted.difference(&expected).next() {
        return Err(RestoreError::Consistency {
            repo: format!("{}/{}", pair.0, pair.1),
            reason: "repository row booted from the staged copy is not part of the \
                     captured set (late row resurrected)"
                .into(),
        });
    }
    if let Some(pair) = expected.difference(&booted).next() {
        return Err(RestoreError::Consistency {
            repo: format!("{}/{}", pair.0, pair.1),
            reason: "captured repository's catalogue row did not survive boot".into(),
        });
    }
    for (pair, path) in &rows {
        let route = &prepared.claimed[pair];
        verify_captured_row_path(stage, &format!("{}/{}", pair.0, pair.1), Some(path), route)?;
        let Some(database) = db.get_db(&pair.0) else {
            return Err(RestoreError::Consistency {
                repo: format!("{}/{}", pair.0, pair.1),
                reason: "booted repository row's database is not open".into(),
            });
        };
        if !database.has_repo(&pair.1) {
            return Err(RestoreError::Consistency {
                repo: format!("{}/{}", pair.0, pair.1),
                reason: "catalogue row exists but the repository did not open at boot".into(),
            });
        }
    }
    for orphan in &prepared.orphan_dirs {
        if !orphan.is_dir() {
            return Err(RestoreError::Consistency {
                repo: "(captured orphan)".into(),
                reason: format!(
                    "captured directory without a catalogue row vanished across boot: {}",
                    orphan.display()
                ),
            });
        }
    }
    drop(db);
    Ok(())
}

/// A captured row's native path: raw string equality against the captured
/// route's staged form is checked by the helper below; here both sides are
/// canonicalized for the case-alias defense.
fn verify_captured_row_path(
    stage: &Path,
    label: &str,
    path: Option<&str>,
    route: &Path,
) -> Result<(), RestoreError> {
    let path = path.ok_or_else(|| RestoreError::Consistency {
        repo: label.to_string(),
        reason: "captured repository's catalogue row has no native path".into(),
    })?;
    let row_path = PathBuf::from(path);
    if !row_path.starts_with(stage) {
        return Err(RestoreError::Consistency {
            repo: label.to_string(),
            reason: format!(
                "captured repository's native path {path:?} is outside the staged \
                 copy (absolute reference not relocated)"
            ),
        });
    }
    let canonical = canonical(&row_path, label, "captured repository's native path")?;
    if canonical != route {
        return Err(RestoreError::Consistency {
            repo: label.to_string(),
            reason: "captured repository's native path resolves to a different \
                     directory than its captured staged path (case alias?)"
                .into(),
        });
    }
    Ok(())
}

fn canonical(path: &Path, repo: &str, what: &str) -> Result<PathBuf, RestoreError> {
    path.canonicalize().map_err(|e| {
        RestoreError::Io(std::io::Error::new(
            e.kind(),
            format!("restore: {what} for '{repo}': {}: {e}", path.display()),
        ))
    })
}

fn field<'a>(row: &'a QueryValue, name: &str) -> Result<&'a str, RestoreError> {
    row.get(name)
        .and_then(QueryValue::as_str)
        .ok_or_else(|| RestoreError::Consistency {
            repo: "(catalogue)".into(),
            reason: format!("repository catalogue row is missing {name}"),
        })
}

fn join_manifest_rel(stage: &Path, rel: &str) -> PathBuf {
    rel.split('/')
        .fold(stage.to_path_buf(), |p, part| p.join(part))
}

fn first_top_level(path: &Path, stage: &Path) -> Option<String> {
    path.strip_prefix(stage)
        .ok()
        .and_then(|rel| rel.components().next())
        .and_then(|c| c.as_os_str().to_str())
        .map(str::to_string)
}

fn allowed_default_main_route(db: &str, repo: &str, first: &str, wal: bool) -> bool {
    db == "default"
        && repo == "main"
        && first
            == if wal {
                "shamir_db_default_main.redb.shamirwal"
            } else {
                "shamir_db_default_main.redb"
            }
}

pub(super) fn reject_reserved_wal_dir(
    first: Option<&str>,
    repo: &CapturedRepo,
) -> Result<(), RestoreError> {
    if let Some(first) = first {
        if RESERVED_DB_NAMES.contains(&first)
            && !allowed_default_main_route(&repo.db, &repo.repo, first, true)
        {
            return Err(RestoreError::Consistency {
                repo: repo.label(),
                reason: format!("captured wal_dir claims reserved destination '{first}'"),
            });
        }
    }
    Ok(())
}

pub(super) fn reject_reserved_store_route(repo: &CapturedRepo) -> Result<(), RestoreError> {
    match repo
        .store_dir
        .split('/')
        .find(|c| !c.is_empty() && *c != ".")
    {
        None => Err(RestoreError::Consistency {
            repo: repo.label(),
            reason: "captured store_dir is empty".into(),
        }),
        Some(c)
            if RESERVED_DB_NAMES.contains(&c)
                && !allowed_default_main_route(&repo.db, &repo.repo, c, false) =>
        {
            Err(RestoreError::Consistency {
                repo: repo.label(),
                reason: format!("captured store_dir claims reserved destination '{c}'"),
            })
        }
        Some(_) => Ok(()),
    }
}
