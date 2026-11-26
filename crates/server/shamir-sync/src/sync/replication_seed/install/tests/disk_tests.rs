#![cfg(unix)]

use std::fs;
use std::path::Path;

use shamir_db::engine::repo::{BoxRepoFactory, RepoConfig, SeedInstallState, SeedTable};
use shamir_db::storage::types::RecordKey;
use shamir_db::{ShamirDb, SystemStoreConfig};
use shamir_query_builder::doc;
use shamir_types::codecs::interned::{inner_value_to_query_value, query_value_to_inner};
use shamir_types::types::record_id::RecordId;
use tokio::runtime::{Builder, Runtime};

use crate::replication_seed::codec::decode;
use crate::replication_seed::error::invalid;
use crate::replication_seed::exporter::export_open;
use crate::replication_seed::install;
use crate::replication_seed::install::installer::install_with;
use crate::replication_seed::install::staging::{destination, open_image};
use crate::replication_seed::{verify, Manifest};

#[test]
fn reserved_requested_seed_name_refuses_before_destination_probe() {
    // Without an entry guard, staging probes an existing seed target and artifact verification/exact __system__ rejection can mask the reserved-name refusal.
    let dir = tempfile::tempdir().unwrap();
    let artifact = dir.path().join("missing-artifact");
    let data = dir.path().join("data");
    fs::create_dir_all(&data).unwrap();
    for name in ["users", "Users"] {
        let target = destination(&data, name, "main");
        // Existing and absent paths both must refuse before find_complete probes either one.
        for exists in [false, true] {
            if exists {
                fs::create_dir_all(&target).unwrap();
            }
            let error = runtime()
                .block_on(install(&data, name, "main", &artifact, "not-a-digest"))
                .expect_err("reserved requested database name must refuse first");
            assert!(matches!(
                &error,
                crate::replication_seed::SeedError::Invalid(_)
            ));
            assert!(error
                .to_string()
                .contains(&format!("database name '{name}' is reserved by the server")));
            if !exists {
                assert!(!target.exists());
            }
            let _ = fs::remove_dir_all(&target);
        }
    }
}

fn runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

async fn artifact(root: &Path) -> String {
    let source = ShamirDb::init_memory().await.unwrap();
    source.create_db("app").await.expect("create_db");
    source
        .add_repo("app", RepoConfig::new("main", BoxRepoFactory::in_memory()))
        .await
        .unwrap();
    source
        .add_table("app", "main", "rows", false)
        .await
        .unwrap();
    source
        .add_table("app", "main", "empty", false)
        .await
        .unwrap();
    let repo = source.get_db("app").unwrap().get_repo("main").unwrap();
    let table = repo.get_table("rows").await.unwrap();
    let value = query_value_to_inner(
        &doc! { "name" => "seeded" }.into(),
        table.interner_for_test().get().await.unwrap(),
    )
    .unwrap();
    for key in [b"opaque".as_slice(), b"second".as_slice()] {
        table
            .mvcc_store_for_test()
            .unwrap()
            .set_versioned_no_wait(RecordKey::from_slice(key), value.to_bytes().unwrap())
            .await
            .unwrap();
    }
    export_open(&source, "app", "main", root, 1024)
        .await
        .unwrap()
}

async fn artifact_with_source_id(root: &Path) -> (String, String, Vec<RecordId>) {
    let source = ShamirDb::init_memory().await.unwrap();
    source.create_db("app").await.expect("create_db");
    source
        .add_repo("app", RepoConfig::new("main", BoxRepoFactory::in_memory()))
        .await
        .unwrap();
    source
        .add_table("app", "main", "rows", false)
        .await
        .unwrap();
    source
        .add_table("app", "main", "empty", false)
        .await
        .unwrap();
    let repo = source.get_db("app").unwrap().get_repo("main").unwrap();
    let table = repo.get_table("rows").await.unwrap();
    let rows = [
        query_value_to_inner(
            &doc! { "name" => "first" }.into(),
            table.interner_for_test().get().await.unwrap(),
        )
        .unwrap(),
        query_value_to_inner(
            &doc! { "name" => "second" }.into(),
            table.interner_for_test().get().await.unwrap(),
        )
        .unwrap(),
    ];
    let ids = table.insert_many(&rows).await.unwrap();
    let hash = export_open(&source, "app", "main", root, 1024)
        .await
        .unwrap();
    let record = source
        .system_store()
        .load_table_record("app", "main", "rows")
        .await
        .unwrap()
        .unwrap();
    let id = record["store_id"].as_str().unwrap().to_owned();
    (hash, id, ids)
}

#[test]
fn imported_seed_ids_are_node_local_persisted_and_digest_is_logical() {
    let dir = tempfile::tempdir().unwrap();
    let seed = dir.path().join("artifact");
    let second = dir.path().join("artifact-after-recreation");
    let data = dir.path().join("data");
    let rt = runtime();
    let (hash, source_id, ids) = rt.block_on(artifact_with_source_id(&seed));
    let manifest: Manifest = decode(&fs::read(seed.join("manifest.mpack")).unwrap()).unwrap();
    assert_eq!(verify(&seed).unwrap(), hash);
    rt.block_on(install(&data, "app", "main", &seed, &hash))
        .unwrap();
    drop(rt);
    let rt = runtime();
    let after_hash = rt.block_on(async {
        let home = ShamirDb::init_seed(SystemStoreConfig::Fjall(data.join("shamir_db_meta.redb")))
            .await
            .unwrap();
        let record = home
            .system_store()
            .load_table_record("app", "main", "rows")
            .await
            .unwrap()
            .unwrap();
        let imported_id = record["store_id"].as_str().unwrap().to_owned();
        assert_ne!(source_id, imported_id);
        let repo = home.get_db("app").unwrap().get_repo("main").unwrap();
        let rows = repo.get_table("rows").await.unwrap();
        assert_eq!(rows.counter_for_test().get().await.unwrap(), 2);
        let values = rows.get_many(&ids).await.unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(Option::is_some));
        let interner = repo.repo_interner().await.unwrap();
        for (value, expected) in values.iter().zip(["first", "second"]) {
            let decoded =
                inner_value_to_query_value(value.as_ref().unwrap(), interner.get().await.unwrap())
                    .unwrap();
            assert_eq!(decoded, doc! { "name" => expected }.build());
        }
        home.rename_table("app", "main", "rows", "renamed")
            .await
            .unwrap();
        let renamed = home
            .system_store()
            .load_table_record("app", "main", "renamed")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(renamed["store_id"].as_str(), Some(imported_id.as_str()));
        home.rename_table("app", "main", "renamed", "rows")
            .await
            .unwrap();
        let recreated = home
            .system_store()
            .load_table_record("app", "main", "rows")
            .await
            .unwrap()
            .unwrap();
        assert_ne!(recreated["store_id"].as_str(), Some(imported_id.as_str()));
        assert_ne!(recreated["store_id"].as_str(), Some(source_id.as_str()));
        export_open(&home, "app", "main", &second, 1024)
            .await
            .unwrap()
    });
    drop(rt);
    let after: Manifest = decode(&fs::read(second.join("manifest.mpack")).unwrap()).unwrap();
    assert_eq!(after.schema_sha256, manifest.schema_sha256);
    assert_eq!(
        after
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
        manifest
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
    );
    assert_eq!(verify(&second).unwrap(), after_hash);
}

#[test]
fn digest_mismatch_has_no_destination_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let seed = dir.path().join("artifact");
    let data = dir.path().join("absent");
    let rt = runtime();
    rt.block_on(artifact(&seed));
    let error = rt
        .block_on(install(&data, "app", "main", &seed, &"0".repeat(64)))
        .unwrap_err();
    assert!(error.to_string().contains("digest mismatch"));
    assert!(!data.exists());
}

fn attempts(data: &Path) -> usize {
    fs::read_dir(data)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("-attempt-"))
        .count()
}

// Runtime teardown is not power-loss simulation. Boundary 6 flushes one row
// before interruption, proving a genuinely partial cold image is abandoned.
#[test]
fn every_publication_boundary_cold_retry_converges_without_row_replay() {
    for stop in 0..=6 {
        let dir = tempfile::tempdir().unwrap();
        let seed = dir.path().join("artifact");
        let data = dir.path().join("data");
        let rt = runtime();
        let hash = rt.block_on(artifact(&seed));
        let result = rt.block_on(install_with(&data, "app", "main", &seed, &hash, |at| {
            if at == stop {
                Err(invalid("injected interruption"))
            } else {
                Ok(())
            }
        }));
        assert!(result.is_err());
        drop(rt);
        let target = destination(&data, "app", "main");
        let attempts_before = attempts(&data);
        let rt = runtime();
        rt.block_on(async {
            if stop < 3 || stop == 6 {
                assert!(!target.exists());
                for entry in fs::read_dir(&data).unwrap().filter_map(Result::ok) {
                    if entry.file_name().to_string_lossy().contains("-attempt-") {
                        let image = open_image(&entry.path(), "main").await.unwrap();
                        if stop == 6 {
                            let info = image.tx_info_store().await.unwrap();
                            let configs = SeedTable::load_configs(&info).await.unwrap();
                            let permit = image.write_domain().admit().await.unwrap();
                            image
                                .add_table_admitted(
                                    configs
                                        .into_iter()
                                        .find(|config| config.name == "rows")
                                        .unwrap(),
                                    &permit,
                                )
                                .await
                                .unwrap();
                            drop(permit);
                            let table = image.get_table("rows").await.unwrap();
                            let history = table.mvcc_store_for_test().unwrap();
                            assert!(history.get_at(b"opaque", 1).await.unwrap().is_some());
                            assert!(history.get_at(b"second", 1).await.unwrap().is_none());
                            assert_eq!(image.replication_bookmark().await.unwrap(), 0);
                        }
                        assert!(SeedInstallState::require_complete(
                            &image.tx_info_store().await.unwrap(),
                            true,
                        )
                        .await
                        .is_err());
                    }
                }
            }
            install(&data, "app", "main", &seed, &hash).await.unwrap();
        });
        drop(rt);
        let attempts_after = attempts(&data);
        if stop < 3 || stop == 6 {
            assert_eq!(attempts_before, 1);
            assert_eq!(attempts_after, 1, "abandoned attempt must be preserved");
        } else {
            assert_eq!(
                attempts_after, 0,
                "Complete retry publishes the original image"
            );
        }
        let rt = runtime();
        rt.block_on(async {
            let home =
                ShamirDb::init_seed(SystemStoreConfig::Fjall(data.join("shamir_db_meta.redb")))
                    .await
                    .unwrap();
            let repo = home.get_db("app").unwrap().get_repo("main").unwrap();
            let rows = repo.get_table("rows").await.unwrap();
            let empty = repo.get_table("empty").await.unwrap();
            assert_eq!(rows.counter_for_test().get().await.unwrap(), 2);
            assert_eq!(empty.counter_for_test().get().await.unwrap(), 0);
            let manager = repo.repo_interner().await.unwrap();
            assert!(manager.get().await.unwrap().get_ind("name").is_some());
            let history = rows.mvcc_store_for_test().unwrap();
            assert!(history.get_at(b"opaque", 1).await.unwrap().is_some());
            assert!(history.get_at(b"second", 1).await.unwrap().is_some());
            assert_eq!(
                repo.read_changelog_from(1, 8).await.unwrap().gap_at,
                Some(1)
            );
            let gate = repo.tx_gate().await.unwrap();
            assert_eq!(gate.assign_next_version_guarded().version(), 2);
        });
        drop(rt);
        let rt = runtime();
        rt.block_on(install(&data, "app", "main", &seed, &hash))
            .unwrap();
        drop(rt);
    }
}

#[test]
fn maintenance_owner_is_held_across_publication() {
    let dir = tempfile::tempdir().unwrap();
    let seed = dir.path().join("artifact");
    let data = dir.path().join("data");
    let rt = runtime();
    let hash = rt.block_on(artifact(&seed));
    rt.block_on(install_with(&data, "app", "main", &seed, &hash, |_| {
        assert!(crate::maintenance_lock::acquire(&data).is_err());
        Ok(())
    }))
    .unwrap();
    drop(rt);
    let owner = crate::maintenance_lock::acquire(&data).unwrap();
    drop(owner);
}

#[test]
fn ordinary_destination_and_different_completed_artifact_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let seed = dir.path().join("artifact");
    let other = dir.path().join("other");
    let data = dir.path().join("data");
    let rt = runtime();
    let hash = rt.block_on(artifact(&seed));
    let other_hash = rt.block_on(artifact(&other));
    rt.block_on(install(&data, "app", "main", &seed, &hash))
        .unwrap();
    drop(rt);
    let rt = runtime();
    assert!(rt
        .block_on(install(&data, "app", "main", &other, &other_hash))
        .is_err());
    drop(rt);
    let ordinary = dir.path().join("ordinary");
    fs::create_dir(&ordinary).unwrap();
    fs::create_dir(destination(&ordinary, "app", "main")).unwrap();
    let rt = runtime();
    assert!(rt
        .block_on(install(&ordinary, "app", "main", &seed, &hash))
        .is_err());
}
