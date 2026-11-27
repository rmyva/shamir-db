use std::path::Path;

use shamir_db::engine::meta::repo_identity::RepoIdentity;
use shamir_db::engine::repo::{SeedImageWriter, SeedReceipt, SeedTable};
use shamir_db::{is_reserved_db_name, ShamirDb, SystemStoreConfig};

use crate::format_stamp::{check_data_dir, EpochPolicy, IgnoreSet, StampMode};
use crate::replication_seed::codec::{decode, digest, METADATA_LIMIT};
use crate::replication_seed::error::invalid;
use crate::replication_seed::{verify, Manifest, SeedError};

use super::{bounded::read_bounded, staging, stream};

pub async fn install(
    data_dir: &Path,
    db: &str,
    repo: &str,
    artifact: &Path,
    manifest_digest: &str,
) -> Result<SeedReceipt, SeedError> {
    install_with(data_dir, db, repo, artifact, manifest_digest, |_| Ok(())).await
}

pub async fn install_subscription(
    data_dir: &Path,
    db: &str,
    repo: &str,
    artifact: &Path,
    manifest_digest: &str,
    subscription: &super::subscription::SeedSubscriptionArgs,
) -> Result<SeedReceipt, SeedError> {
    if is_reserved_db_name(db) {
        // Before the destination check, the existing target/probe could mask this requested-name refusal.
        return Err(SeedError::Database(shamir_db::DbError::Validation(
            format!("database name '{db}' is reserved by the server"),
        )));
    }
    subscription.validate()?;
    install_inner(
        data_dir,
        db,
        repo,
        artifact,
        manifest_digest,
        Some(subscription),
        |_| Ok(()),
    )
    .await
}

/// Fault-injection seam (SEED-3/SEED-5): run the full installer with a
/// callback invoked at each durable boundary. Production callers pass a
/// no-op closure — the boundaries are pure observation points. Public so the
/// `tests/seed_recovery_e2e` integration target can interrupt an install
/// mid-flight and prove the boot gate refuses the abandoned image.
pub async fn install_with(
    data_dir: &Path,
    db: &str,
    repo: &str,
    artifact: &Path,
    manifest_digest: &str,
    boundary: impl Fn(u8) -> Result<(), SeedError>,
) -> Result<SeedReceipt, SeedError> {
    install_inner(
        data_dir,
        db,
        repo,
        artifact,
        manifest_digest,
        None,
        boundary,
    )
    .await
}

async fn install_inner(
    data_dir: &Path,
    db: &str,
    repo: &str,
    artifact: &Path,
    manifest_digest: &str,
    subscription: Option<&super::subscription::SeedSubscriptionArgs>,
    boundary: impl Fn(u8) -> Result<(), SeedError>,
) -> Result<SeedReceipt, SeedError> {
    if is_reserved_db_name(db) {
        // Before the destination check, the existing target/probe could mask this requested-name refusal.
        return Err(SeedError::Database(shamir_db::DbError::Validation(
            format!("database name '{db}' is reserved by the server"),
        )));
    }
    let root = artifact.to_owned();
    let expected = manifest_digest.to_owned();
    let manifest = tokio::task::spawn_blocking(move || {
        if verify(&root)? != expected {
            return Err(invalid("trusted manifest digest mismatch"));
        }
        let bytes = read_bounded(&root.join("manifest.mpack"), METADATA_LIMIT)?;
        if digest(&bytes) != expected {
            return Err(invalid("manifest changed after verification"));
        }
        decode::<Manifest>(&bytes)
    })
    .await
    .map_err(|e| invalid(e.to_string()))??;
    if !cfg!(unix) {
        return Err(invalid("durable seed installation requires Unix"));
    }
    if manifest.source_db != db || manifest.source_repo != repo {
        return Err(invalid("source/destination names differ"));
    }
    let receipt = SeedReceipt {
        artifact_digest: manifest_digest.to_owned(),
        source_db: db.to_owned(),
        source_repo: repo.to_owned(),
        source_identity: RepoIdentity {
            lineage_id: manifest.lineage_id,
            history_epoch: manifest.history_epoch,
        },
        schema_digest: manifest.schema_sha256.clone(),
        source_version: manifest.version,
        local_baseline: 1,
    };
    receipt.validate()?;
    // All destination mutation (including lock diagnostics and catalogue init)
    // follows verification. Lock outlives every handle, including on errors.
    std::fs::create_dir_all(data_dir)?;
    let data_dir = std::fs::canonicalize(data_dir)?;
    let _lock = crate::maintenance_lock::acquire(&data_dir)?;
    // Format-stamp gate (design 3.3: Writer — the install writes the target).
    check_data_dir(
        &data_dir,
        StampMode::Writer,
        &IgnoreSet::none(),
        &EpochPolicy::compiled(),
    )
    .map_err(|e| invalid(e.to_string()))?;
    let target = staging::destination(&data_dir, db, repo);
    let completed = staging::find_complete(&target, &receipt).await?;
    let home = ShamirDb::init_seed(SystemStoreConfig::Fjall(
        data_dir.join("shamir_db_meta.redb"),
    ))
    .await?;
    home.check_seed_target(db, repo, &target, &receipt).await?;
    let stage = match completed {
        Some(complete) => complete,
        None => {
            let stage = staging::fresh(&target)?;
            boundary(0)?;
            let image = staging::open_image(&stage, repo).await?;
            let tables = manifest
                .tables
                .iter()
                .map(|table| SeedTable {
                    name: table.name.clone(),
                    live_records: table.live_records,
                })
                .collect();
            // begin reserves/flushed allocation evidence 1 before data, just
            // like WAL-less follower apply. This conservative recovery evidence
            // is not image readiness: final floors, gap and Complete follow data.
            let mut writer = SeedImageWriter::begin(image.clone(), receipt.clone(), tables).await?;
            boundary(1)?;
            stream::import(artifact, &manifest, &mut writer, &boundary, &image).await?;
            boundary(2)?;
            let schemas = manifest
                .tables
                .iter()
                .map(|table| {
                    (
                        table.name.clone(),
                        table.schema.clone(),
                        table.schema_version,
                    )
                })
                .collect::<Vec<_>>();
            let schema_store = home.prepare_seed_schema(&image, db, repo, &schemas).await?;
            let root = artifact.to_owned();
            let observed = tokio::task::spawn_blocking(move || verify(&root))
                .await
                .map_err(|e| invalid(e.to_string()))??;
            if observed != manifest_digest {
                return Err(invalid("seed artifact changed during import"));
            }
            writer.finish(&[schema_store]).await?;
            image.seed_stop_journal_writer().await?;
            drop(image);
            stage
        }
    };
    boundary(3)?;
    staging::publish(&stage, &target)?;
    boundary(4)?;
    let image = match home.get_db(db).and_then(|database| database.get_repo(repo)) {
        Some(image) => image,
        None => staging::open_image(&target, repo).await?,
    };
    home.register_seed_image(&image, &receipt, &target).await?;
    boundary(5)?;
    if let Some(subscription) = subscription {
        subscription.persist(&home, &image, &receipt).await?;
    }
    Ok(receipt)
}
