//! S5c-X PR8b — ordinary READS of a quarantined (`resync_required`)
//! replication destination fail closed; status/DESCRIBE stay readable;
//! healthy and non-replicated repos are unaffected.

use super::validation::assert_absent_quarantined;
use crate::engine::repo::repo_types::BoxRepoFactory;
use crate::engine::repo::RepoConfig;
use crate::shamir_db::execute::SKIP_READ_QUARANTINE_FOR_TEST;
#[cfg(feature = "fjall")]
use crate::shamir_db::SystemStoreConfig;
use crate::{DbError, ShamirDb};
use shamir_query_builder::batch::Batch;
#[cfg(feature = "fjall")]
use shamir_query_builder::ddl::drop_replication_profile;
use shamir_query_builder::ddl::{
    alter_subscription, drop_subscription, list_subscriptions, repl_scope, replication_profile,
    subscription,
};
use shamir_query_builder::doc;
use shamir_query_builder::write::insert;
use shamir_query_builder::Query;
use shamir_query_types::admin::{ReplDirection, ReplMode};
use shamir_query_types::batch::{BatchError, BatchResponse};

/// In-memory ShamirDb with db "testdb", repos "main" + "side" (each with a
/// "users" table).
async fn setup() -> ShamirDb {
    let shamir = ShamirDb::init_memory().await.unwrap();
    let db = shamir.create_db("testdb").await.expect("create_db");
    db.add_repo(
        RepoConfig::new("main", BoxRepoFactory::in_memory())
            .add_table(crate::engine::table::TableConfig::new("users")),
    )
    .await
    .unwrap();
    db.add_repo(
        RepoConfig::new("side", BoxRepoFactory::in_memory())
            .add_table(crate::engine::table::TableConfig::new("users")),
    )
    .await
    .unwrap();
    shamir
}

/// Insert one row into `testdb/{repo}.users`.
async fn seed_row(shamir: &ShamirDb) {
    let mut b = Batch::new();
    b.id(1);
    b.insert(
        "ins",
        insert("users").row(doc! { "name" => "Alice", "age" => 30 }),
    );
    shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
        .unwrap();
}

/// Plain SELECT against `testdb/{repo}.users`.
async fn read_users(shamir: &ShamirDb, repo: &str) -> Result<BatchResponse, BatchError> {
    let mut b = Batch::new();
    b.id(9);
    b.query("r", Query::with_repo(repo, "users"));
    shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
}

/// Create a Pull profile + ACTIVE subscription covering the `testdb`
/// destination repo set, then flip the row via the REAL persisted supervisor
/// entry point; the refresh hook rebuilds the read-quarantine cache.
async fn quarantine_repos(shamir: &ShamirDb, sub: &str, profile: &str, repos: &[&str]) {
    let mut b = Batch::new();
    b.id(2);
    let mut builder = replication_profile(profile);
    for repo in repos {
        builder = builder.stream(
            repl_scope("testdb").repo(*repo).build(),
            ReplDirection::Pull,
            ReplMode::ReadOnly,
        );
    }
    b.op("profile", builder);
    b.op("sub", subscription(sub, "tcp://peer:9000", "pub", profile));
    shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
        .unwrap();
    shamir
        .mark_subscription_resync_required(sub, 7, 3)
        .await
        .unwrap();
}

/// Single-destination form of [`quarantine_repos`].
async fn quarantine(shamir: &ShamirDb, sub: &str, profile: &str, repo: &str) {
    quarantine_repos(shamir, sub, profile, &[repo]).await;
}

/// Admin `ALTER SUBSCRIPTION ... RESUME` through the batch path.
async fn resume_subscription(
    shamir: &ShamirDb,
    id: u64,
    sub: &str,
) -> Result<BatchResponse, BatchError> {
    let mut b = Batch::new();
    b.id(id);
    b.try_op("alt", alter_subscription(sub).resume()).unwrap();
    shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
}

/// Admin `ALTER SUBSCRIPTION ... PAUSE` through the batch path.
async fn pause_subscription(
    shamir: &ShamirDb,
    id: u64,
    sub: &str,
) -> Result<BatchResponse, BatchError> {
    let mut b = Batch::new();
    b.id(id);
    b.try_op("alt", alter_subscription(sub).pause()).unwrap();
    shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
}

/// Reads succeed before quarantine; SELECT fails with "resync_required"
/// once the row is persisted as `resync_required`.
#[tokio::test]
async fn t1_read_get_query_refused_while_quarantined() {
    let shamir = setup().await;
    seed_row(&shamir).await;
    assert!(read_users(&shamir, "main").await.is_ok());

    quarantine(&shamir, "sub", "prof", "main").await;

    let err = read_users(&shamir, "main").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"));
}

/// The changelog pull path refuses a quarantined repo with
/// `DbError::ResyncRequired`.
#[tokio::test]
async fn t2_changelog_pull_refused_while_quarantined() {
    let shamir = setup().await;
    quarantine(&shamir, "sub", "prof", "main").await;

    let Err(err) = shamir.read_changelog_bounded("testdb", "main", 1, 10).await else {
        panic!("changelog pull of a quarantined repo must be refused");
    };
    assert!(matches!(err, DbError::ResyncRequired { .. }));
    assert_eq!(err.code(), Some("resync_required"));
}

/// Admin status/list stays readable and reports `state == "resync_required"`.
#[tokio::test]
async fn t3_describe_and_status_stay_readable() {
    let shamir = setup().await;
    quarantine(&shamir, "sub", "prof", "main").await;

    let mut b = Batch::new();
    b.id(3);
    b.op("ls", list_subscriptions());
    let resp = shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
        .unwrap();
    let val = resp.results["ls"].records[0].as_value().into_owned();
    let subs = val.get("subscriptions").and_then(|v| v.as_array()).unwrap();
    assert_eq!(subs.len(), 1);
    let row = &subs[0];
    assert_eq!(
        row.get("state").and_then(|v| v.as_str()),
        Some("resync_required")
    );
    assert_eq!(row.get("name").and_then(|v| v.as_str()), Some("sub"));
}

/// Plain `Resume` and PAUSE of a quarantined subscription are refused with
/// "resync_required" (the durable marker survives state changes); reads stay
/// refused. DROP is refused too — dropping is not evidence of consistency.
#[tokio::test]
async fn t4_every_admin_action_including_drop_is_refused_while_quarantined() {
    let shamir = ShamirDb::init_memory().await.unwrap();
    let db = shamir.create_db("testdb").await.expect("create_db");
    // No tables here: the seed image carries the `users` inventory.
    db.add_repo(RepoConfig::new("main", BoxRepoFactory::in_memory()))
        .await
        .unwrap();
    // Quarantine FIRST, on an ORDINARY destination (no seed receipt).
    quarantine(&shamir, "sub", "prof", "main").await;
    assert!(read_users(&shamir, "main").await.is_err());

    // Plain Resume refused.
    let err = resume_subscription(&shamir, 4, "sub").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"));
    // Pause refused too — a state flip cannot clear the quarantine marker.
    let err = pause_subscription(&shamir, 5, "sub").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"));
    // Reads stay refused.
    assert!(read_users(&shamir, "main").await.is_err());

    // Round 6, traced: the DROP guard surfaces the DEDICATED
    // `replication_destination_quarantined` code (`ensure_drop_allowed` →
    // `repl_guard_error_to_batch`), not the read-path `resync_required`.
    // COUNTERFACTUAL: `resync_required` is the stale round-5 oracle.
    let mut b = Batch::new();
    b.id(6);
    b.try_op("drop", drop_subscription("sub").hmac("test-hmac"))
        .unwrap();
    let err = shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some("replication_destination_quarantined"),
        "{err}"
    );
    assert!(read_users(&shamir, "main").await.is_err());
}

/// A non-replicated repo stays readable while `main` is quarantined; so
/// does a repo whose covering subscription is "active".
#[tokio::test]
async fn t5_healthy_and_non_replicated_repos_unaffected() {
    let shamir = setup().await;
    quarantine(&shamir, "sub", "prof", "main").await;

    // "side" has no subscription covering it.
    assert!(read_users(&shamir, "side").await.is_ok());

    // A covering subscription that is "active" does not quarantine.
    let mut b = Batch::new();
    b.id(5);
    b.op(
        "profile_side",
        replication_profile("prof_side").stream(
            repl_scope("testdb").repo("side").build(),
            ReplDirection::Pull,
            ReplMode::ReadOnly,
        ),
    );
    b.op(
        "sub_side",
        subscription("sub_side", "tcp://peer:9000", "pub", "prof_side"),
    );
    shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
        .unwrap();
    assert!(read_users(&shamir, "side").await.is_ok());
    assert!(read_users(&shamir, "main").await.is_err());
}

/// Quarantine survives reopening and subscription drop is refused.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn t6_quarantine_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let sys_path = dir.path().join("meta");
    let repo_path = dir.path().join("main");

    {
        let shamir = ShamirDb::init(SystemStoreConfig::Fjall(sys_path.clone()))
            .await
            .unwrap();
        shamir.create_db("testdb").await.expect("create_db");
        // Durable repo, persisted via the facade so the boot reload finds the
        // catalogue row. No inline tables: the installed seed image carries
        // the `users` table (writer registration + catalogue write).
        // RAW fjall (no MemBuffer): this test installs a replacement SEED on
        // the repo and binds a SUBSCRIPTION to it, and the new CREATE/RESUME
        // admission (repo-wide atomic-batch capability, see supervisor.rs
        // validate_repo_wide_atomic_batch) correctly refuses MemBuffer-wrapped
        // destinations with `atomic_batch_unsupported`. Raw fjall is also the
        // backend the seed writes through, so the durable directory survives
        // the restart below.
        shamir
            .add_repo(
                "testdb",
                RepoConfig::new("main", BoxRepoFactory::fjall_raw(repo_path.clone())),
            )
            .await
            .unwrap();
        quarantine(&shamir, "sub", "prof", "main").await;
        // A SECOND subscription covering the same repo, quarantined too.
        quarantine(&shamir, "sub2", "prof2", "main").await;
        assert!(read_users(&shamir, "main").await.is_err());

        // Round 6, traced like t4: the DEDICATED drop-guard code.
        // COUNTERFACTUAL: `resync_required` is the stale round-5 oracle.
        let mut b = Batch::new();
        b.id(3);
        b.try_op("drop", drop_subscription("sub").hmac("test-hmac"))
            .unwrap();
        let err = shamir
            .execute("testdb", &b.to_request_via_msgpack().unwrap())
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Some("replication_destination_quarantined"),
            "{err}"
        );
        assert!(read_users(&shamir, "main").await.is_err());
    }

    // Reopen with a bounded retry (fjall releases its lock shortly after
    // the owning ShamirDb is dropped).
    let mut shamir = None;
    for _ in 0..100 {
        match ShamirDb::init(SystemStoreConfig::Fjall(sys_path.clone())).await {
            Ok(opened) => {
                shamir = Some(opened);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    }
    let shamir = shamir.expect("system store still locked after retries");

    // The repo must have been reloaded from the persisted catalogue (the
    // exact CI regression: `get_repo` was None).
    assert!(
        shamir.get_db("testdb").unwrap().get_repo("main").is_some(),
        "testdb/main must be re-registered from the catalogue after restart"
    );

    // Both subscriptions still quarantined across the restart.
    assert_absent_quarantined(&shamir, read_users(&shamir, "main").await, "testdb", "main").await;
    let err = resume_subscription(&shamir, 6, "sub2").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"));
}

/// Negative control: with the test seam set, the quarantined read is SERVED
/// (stale data) — proving the read gate itself is what fails the read.
#[tokio::test]
async fn t7_negative_control_seam_serves_stale_reads() {
    let shamir = setup().await;
    seed_row(&shamir).await;
    quarantine(&shamir, "sub", "prof", "main").await;
    assert!(read_users(&shamir, "main").await.is_err());

    SKIP_READ_QUARANTINE_FOR_TEST
        .scope(true, async {
            let resp = read_users(&shamir, "main").await.unwrap();
            assert_eq!(resp.results["r"].records.len(), 1);
        })
        .await;
}

/// A temporal (`Temporal::AsOf`) read of a quarantined repo is refused like
/// an ordinary read — the gate sits above the temporal dispatch.
#[tokio::test]
async fn t8_temporal_as_of_read_refused_while_quarantined() {
    let shamir = setup().await;
    seed_row(&shamir).await;
    quarantine(&shamir, "sub", "prof", "main").await;

    let mut b = Batch::new();
    b.id(8);
    b.query("r", Query::with_repo("main", "users").as_of_version(1));
    let Err(err) = shamir
        .execute("testdb", &b.to_request_via_msgpack().unwrap())
        .await
    else {
        panic!("temporal AsOf read of a quarantined repo must be refused");
    };
    assert!(matches!(err, BatchError::ResyncRequired { .. }));
    assert_eq!(err.code(), Some("resync_required"));
}

/// The unbounded changelog read entries are refused with
/// `DbError::ResyncRequired` while quarantined; healthy repos stay readable.
#[tokio::test]
async fn t9_changelog_read_entries_refused_while_quarantined() {
    let shamir = setup().await;
    seed_row(&shamir).await;
    // Healthy repo reads succeed before quarantine.
    assert!(shamir
        .read_changelog_from("testdb", "main", 1, 10)
        .await
        .unwrap()
        .is_some());
    assert!(shamir
        .read_changelog_from_journal("testdb", "main", 1, 10)
        .await
        .unwrap()
        .is_some());

    quarantine(&shamir, "sub", "prof", "main").await;

    let Err(err) = shamir.read_changelog_from("testdb", "main", 1, 10).await else {
        panic!("read_changelog_from of a quarantined repo must be refused");
    };
    assert!(matches!(err, DbError::ResyncRequired { .. }));
    assert_eq!(err.code(), Some("resync_required"));

    let Err(err) = shamir
        .read_changelog_from_journal("testdb", "main", 1, 10)
        .await
    else {
        panic!("read_changelog_from_journal of a quarantined repo must be refused");
    };
    assert!(matches!(err, DbError::ResyncRequired { .. }));
    assert_eq!(err.code(), Some("resync_required"));

    // Non-replicated repo is unaffected.
    assert!(shamir
        .read_changelog_from("testdb", "side", 1, 10)
        .await
        .unwrap()
        .is_some());
    assert!(shamir
        .read_changelog_from_journal("testdb", "side", 1, 10)
        .await
        .unwrap()
        .is_some());
}

/// The interactive-tx family fails closed: `tx_begin` refuses a quarantined
/// repo, a pre-quarantine tx is refused on `tx_execute`; `side` works.
#[tokio::test]
async fn t10_tx_reads_refused_while_quarantined() {
    let shamir = setup().await;
    seed_row(&shamir).await;

    // Open the tx while `main` is still healthy.
    let (mut tx, _guard, _) = shamir.tx_begin("testdb", "main", "snapshot").await.unwrap();
    quarantine(&shamir, "sub", "prof", "main").await;

    // BEGIN on the quarantined repo is refused.
    let Err(err) = shamir.tx_begin("testdb", "main", "snapshot").await else {
        panic!("tx_begin on a quarantined repo must be refused");
    };
    assert_eq!(err.code(), Some("resync_required"));

    // EXECUTE on the pre-quarantine tx is refused too (fail closed on use).
    let mut b = Batch::new();
    b.id(10);
    b.query("r", Query::with_repo("main", "users"));
    let Err(err) = shamir
        .tx_execute("testdb", &b.to_request_via_msgpack().unwrap(), &mut tx)
        .await
    else {
        panic!("tx_execute on a quarantined repo must be refused");
    };
    assert_eq!(err.code(), Some("resync_required"));

    // The healthy, non-replicated `side` repo is unaffected.
    let (mut tx_side, _guard_side, _) =
        shamir.tx_begin("testdb", "side", "snapshot").await.unwrap();
    let mut b_side = Batch::new();
    b_side.id(11);
    b_side.query("r", Query::with_repo("side", "users"));
    shamir
        .tx_execute(
            "testdb",
            &b_side
                .to_request_via_msgpack()
                .expect("batch request encoding"),
            &mut tx_side,
        )
        .await
        .unwrap();
}

/// S5c-X PR8b follow-up: `subscribe_changelog` refuses a quarantined repo
/// with the typed `DbError::ResyncRequired` BEFORE attaching a live event
/// stream; a non-replicated repo subscribes fine.
#[tokio::test]
async fn t11_subscribe_changelog_refused_while_quarantined() {
    let shamir = setup().await;
    seed_row(&shamir).await;
    quarantine(&shamir, "sub", "prof", "main").await;

    let Err(err) = shamir.subscribe_changelog("testdb", "main").await else {
        panic!("subscribe_changelog on a quarantined repo must be refused");
    };
    assert!(matches!(err, DbError::ResyncRequired { .. }));
    assert_eq!(err.code(), Some("resync_required"));

    // Non-replicated repo subscribes fine; drop the receiver immediately so
    // no background subscriber leaks.
    let rx = shamir
        .subscribe_changelog("testdb", "side")
        .await
        .unwrap()
        .expect("side repo exists");
    drop(rx);
}

/// The bounded journal read (the exact call the server's
/// `DbJournalSource::read` makes) refuses once the repo is quarantined, so
/// a bridge attached BEFORE the quarantine terminates on its next drain
/// (`JournalDrain::run` propagates the `Err`). `side` keeps serving `Ok`.
#[tokio::test]
async fn t12_gated_journal_source_refuses_once_quarantined() {
    let shamir = setup().await;
    seed_row(&shamir).await;

    // Healthy before the quarantine.
    assert!(shamir
        .read_changelog_bounded("testdb", "main", 1, 10)
        .await
        .unwrap()
        .is_some());

    quarantine(&shamir, "sub", "prof", "main").await;

    // The gated journal source's read fails closed with the typed error.
    let Err(err) = shamir.read_changelog_bounded("testdb", "main", 1, 10).await else {
        panic!("read_changelog_bounded on a quarantined repo must be refused");
    };
    assert!(matches!(err, DbError::ResyncRequired { .. }));
    assert_eq!(err.code(), Some("resync_required"));

    // Non-replicated repo is unaffected.
    assert!(shamir
        .read_changelog_bounded("testdb", "side", 1, 10)
        .await
        .unwrap()
        .is_some());
}

/// Unix-only recovery helpers: `SeedImageWriter::begin` (and the whole
/// seed-install path) refuses non-Unix targets, so every helper touching the
/// real installer is compiled out on Windows/macOS.
#[cfg(unix)]
pub(crate) mod recovery {
    use std::sync::Arc;

    use async_trait::async_trait;
    use shamir_engine::meta::repo_identity::RepoIdentity;
    use shamir_engine::repo::{SeedImageWriter, SeedReceipt, SeedTable};
    use shamir_query_builder::doc;
    use shamir_query_types::admin::{
        CreatePublicationOp, CreateSubscriptionOp, ReplDirection, ReplMode, ReplScope, ReplStream,
    };
    use shamir_query_types::wire::repl::{ReplRepoInfo, ReplResponse};
    use shamir_types::codecs::interned::query_value_to_inner_with;
    use shamir_types::codecs::CodecError;
    use shamir_types::core::interner::InternerKey;
    use shamir_types::types::record_id::RecordId;

    use crate::{ResumeProbe, SeedRefusal, ShamirDb};

    struct HelloProbe(ReplResponse);

    #[async_trait]
    impl ResumeProbe for HelloProbe {
        async fn hello(&self, _: &str) -> Result<ReplResponse, SeedRefusal> {
            Ok(self.0.clone())
        }
    }

    /// The receipt the installer binds: fresh staging for `testdb/main`,
    /// lineage `[1; 16]`, epoch 2, version 1.
    fn replacement_receipt() -> SeedReceipt {
        SeedReceipt {
            artifact_digest: "a".repeat(64),
            source_db: "testdb".into(),
            source_repo: "main".into(),
            source_identity: RepoIdentity {
                lineage_id: [1; 16],
                history_epoch: 2,
            },
            schema_digest: "b".repeat(64),
            source_version: 1,
            local_baseline: 1,
        }
    }

    /// Upstream Hello consistent with [`replacement_receipt`] and the fresh
    /// (bookmark 0) destination.
    pub(super) fn matching_hello() -> ReplResponse {
        ReplResponse::Hello {
            leader_epoch: 1,
            repos: vec![ReplRepoInfo {
                db: "testdb".into(),
                repo: "main".into(),
                current_version: 1,
                journal_floor: 0,
                repo_lineage_id: Some([1; 16]),
                history_epoch: Some(2),
                gap_first: None,
                gap_through: None,
                schema_digest: Some("b".repeat(64)),
                settled_version: Some(1),
                publications: vec![CreatePublicationOp {
                    create_publication: "pub".into(),
                    scopes: vec![ReplScope {
                        db: "testdb".into(),
                        repo: Some("main".into()),
                        table: None,
                    }],
                }],
            }],
        }
    }

    pub(crate) fn probe() -> Arc<dyn ResumeProbe> {
        Arc::new(HelloProbe(matching_hello()))
    }

    /// Source-interner field ids for the replacement `users` row (arbitrary
    /// — `SeedImageWriter::binding` remaps them to the local interner).
    const NAME_SOURCE_ID: u64 = 100;
    const AGE_SOURCE_ID: u64 = 101;

    /// Image half of the REAL recovery procedure: build the replacement
    /// seed image (`SeedImageWriter`, epoch bump, ONE replacement `users`
    /// row) and persist the table to the catalogue — rebinds are
    /// [`rebind_to_replacement`].
    pub(crate) async fn install_replacement_image(shamir: &ShamirDb) {
        let repo = shamir.get_db("testdb").unwrap().get_repo("main").unwrap();
        let receipt = replacement_receipt();
        let mut writer = SeedImageWriter::begin(
            repo.clone(),
            receipt.clone(),
            vec![SeedTable {
                name: "users".into(),
                live_records: 1,
            }],
        )
        .await
        .unwrap();
        writer.binding(NAME_SOURCE_ID, "name").await.unwrap();
        writer.binding(AGE_SOURCE_ID, "age").await.unwrap();
        let row = query_value_to_inner_with(
            &doc! { "name" => "replacement", "age" => 1 }.into(),
            &|key| match key {
                "name" => Ok(InternerKey::new(NAME_SOURCE_ID)),
                "age" => Ok(InternerKey::new(AGE_SOURCE_ID)),
                other => Err(CodecError::Decode(format!(
                    "unexpected replacement row field: {other}"
                ))),
            },
        )
        .unwrap();
        writer
            .row(
                0,
                RecordId::system("replacement").to_bytes(),
                row.to_bytes().unwrap(),
            )
            .await
            .unwrap();
        writer.finish(&[]).await.unwrap();
        // Persist the `users` table to the table catalogue: the writer only
        // registers it on the RUNNING repo instance.
        shamir
            .add_table("testdb", "main", "users", false)
            .await
            .unwrap();
    }

    /// Rebind half of the REAL recovery procedure: `install_seed_profile` +
    /// `install_seed_subscription` over the EXISTING (quarantined) row with
    /// the image's receipt. The NEW receipt lands WITHOUT clearing the
    /// quarantine; only a post-reseed `Resume` lifts it.
    pub(crate) async fn rebind_to_replacement(shamir: &ShamirDb, sub: &str, profile_name: &str) {
        rebind_to_replacement_covering(shamir, sub, profile_name, &[("testdb", "main")]).await;
    }

    /// [`rebind_to_replacement`] variant with an EXPLICIT destination set
    /// (round 6 item 6 fixtures: shared + exclusive destinations).
    pub(crate) async fn rebind_to_replacement_covering(
        shamir: &ShamirDb,
        sub: &str,
        profile_name: &str,
        destinations: &[(&str, &str)],
    ) {
        let repo = shamir.get_db("testdb").unwrap().get_repo("main").unwrap();
        let receipt = replacement_receipt();
        let streams: Vec<ReplStream> = destinations
            .iter()
            .map(|(db, repo)| ReplStream {
                scope: ReplScope {
                    db: (*db).into(),
                    repo: Some((*repo).into()),
                    table: None,
                },
                direction: ReplDirection::Pull,
                mode: ReplMode::ReadOnly,
            })
            .collect();
        shamir
            .install_seed_profile(profile_name, &streams)
            .await
            .unwrap();
        shamir
            .install_seed_subscription(
                &repo,
                &receipt,
                &CreateSubscriptionOp {
                    create_subscription: sub.to_string(),
                    upstream: "tcp://peer:9000".into(),
                    publication: "pub".into(),
                    profile: profile_name.to_string(),
                    paused: true,
                },
            )
            .await
            .unwrap();
    }
}

/// Sanctioned recovery path (evidence row 9, Unix-only): quarantine via the
/// real supervisor entry point; typed refusals while no replacement seed
/// exists; the REAL installer rebinds a NEW receipt (still quarantined);
/// then `Resume` durably removes the marker (item 1) and the destination
/// serves the replacement row.
#[cfg(unix)]
#[tokio::test]
async fn t13_replacement_seed_rebinds_quarantined_row_then_resume() {
    let shamir = ShamirDb::init_memory().await.unwrap();
    let db = shamir.create_db("testdb").await.expect("create_db");
    // No tables on `main`: the seed image carries them. `side` is an
    // unrelated, always-readable repo.
    db.add_repo(RepoConfig::new("main", BoxRepoFactory::in_memory()))
        .await
        .unwrap();
    db.add_repo(
        RepoConfig::new("side", BoxRepoFactory::in_memory())
            .add_table(crate::engine::table::TableConfig::new("users")),
    )
    .await
    .unwrap();
    assert!(read_users(&shamir, "side").await.is_ok());
    quarantine(&shamir, "sub", "prof", "main").await;

    // Bound destination refuses reads with the typed code, and a plain
    // `Resume` is refused (no replacement seed yet). `side` stays readable.
    assert_absent_quarantined(&shamir, read_users(&shamir, "main").await, "testdb", "main").await;
    let err = resume_subscription(&shamir, 4, "sub").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"));
    assert!(read_users(&shamir, "side").await.is_ok());

    // Real recovery: the OFFLINE installer installs the replacement image
    // once, then rebinds the NEW receipt on the EXISTING quarantined row.
    recovery::install_replacement_image(&shamir).await;
    recovery::rebind_to_replacement(&shamir, "sub", "prof").await;

    // The row carries the NEW receipt but is STILL quarantined. Read the
    // RAW catalogue row (`rows` — status/list project only a subset).
    let records = super::super::catalogue::rows(&shamir, "subscriptions")
        .await
        .unwrap();
    let row = records
        .iter()
        .find(|r| r.get("name").and_then(|v| v.as_str()) == Some("sub"))
        .unwrap();
    assert_eq!(
        row.get("state").and_then(|v| v.as_str()),
        Some("resync_required")
    );
    assert_eq!(row.get("quarantined").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        row["seed_receipt"]["source_identity"]["history_epoch"].as_u64(),
        Some(2),
        "row must carry the REPLACEMENT receipt (history_epoch 2)"
    );
    assert!(row
        .get("resync_from_receipt")
        .map(|v| v.is_null())
        .unwrap_or(false));
    let err = read_users(&shamir, "main").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"));
    assert!(read_users(&shamir, "side").await.is_ok());

    // Post-reseed `Resume` now SUCCEEDS: the row is active, the durable
    // marker is gone, and the destination serves the replacement row.
    shamir.set_replication_resume_probe(recovery::probe());
    resume_subscription(&shamir, 6, "sub").await.unwrap();
    let resp = read_users(&shamir, "main").await.unwrap();
    assert_eq!(resp.results["r"].records.len(), 1);
    let served = resp.results["r"].records[0].as_value().into_owned();
    assert_eq!(
        served.get("name").and_then(|v| v.as_str()),
        Some("replacement")
    );
    assert_eq!(served.get("age").and_then(|v| v.as_i64()), Some(1));
    assert!(read_users(&shamir, "side").await.is_ok());
}

/// Evidence row 3 (REOPEN leg): DROP the profile and REOPEN — the bound
/// destination STILL refuses: the durable row binding (not the profile) is
/// the quarantine authority.
#[cfg(feature = "fjall")]
#[tokio::test]
async fn t14_bound_destination_stays_refused_after_profile_drop_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let sys_path = dir.path().join("meta");
    let repo_path = dir.path().join("main");

    {
        let shamir = ShamirDb::init(SystemStoreConfig::Fjall(sys_path.clone()))
            .await
            .unwrap();
        shamir.create_db("testdb").await.expect("create_db");
        shamir
            .add_repo(
                "testdb",
                // RAW fjall (no MemBuffer): this test binds a SUBSCRIPTION to
                // this repo, and the CREATE/RESUME admission
                // (validate_repo_wide_atomic_batch) refuses MemBuffer-wrapped
                // destinations with `atomic_batch_unsupported`. Raw fjall is also
                // the backend the seed writes through, so the durable directory
                // survives the reopen below.
                RepoConfig::new("main", BoxRepoFactory::fjall_raw(repo_path.clone())),
            )
            .await
            .unwrap();
        quarantine(&shamir, "sub", "prof", "main").await;
        assert!(read_users(&shamir, "main").await.is_err());

        // Delete/corrupt the profile: DROP the replication profile the
        // subscription was created against.
        let mut b = Batch::new();
        b.id(7);
        b.try_op("drp", drop_replication_profile("prof").hmac("test-hmac"))
            .unwrap();
        shamir
            .execute("testdb", &b.to_request_via_msgpack().unwrap())
            .await
            .unwrap();

        // Still refused in-process — the binding outlived the profile.
        assert_absent_quarantined(&shamir, read_users(&shamir, "main").await, "testdb", "main")
            .await;
    }

    // Reopen with the bounded fjall-lock retry (mirrors `durable_repo_tests`).
    let mut shamir = None;
    for _ in 0..100 {
        match ShamirDb::init(SystemStoreConfig::Fjall(sys_path.clone())).await {
            Ok(opened) => {
                shamir = Some(opened);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    }
    let shamir = shamir.expect("system store still locked after retries");

    // After reopen the cache is rebuilt from the persisted rows' durable
    // bindings — the profile is gone, the refusal is not.
    assert_absent_quarantined(&shamir, read_users(&shamir, "main").await, "testdb", "main").await;
}

/// Round 2 (C12, Unix-only): the durable reseed→reopen→Resume proof with
/// the REAL installer (monotonic generations). The quarantine survives a
/// reopen and the round-7 item-2 aggregation rule holds through the real
/// admin `Resume`: every subscription keeps a SINGLE-stream scope (the
/// `matches_scope` admission rule, `catalogue.rs` — exactly ONE stream) and
/// the two rows OVERLAP on `testdb/main`; lifting one quarantine must NOT
/// unblock the destination the other still covers.
#[cfg(all(unix, feature = "fjall"))]
#[tokio::test]
async fn t15_replacement_rebind_survives_reopen_and_resume_requires_full_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let sys_path = dir.path().join("meta");
    let repo_path = dir.path().join("main");

    {
        let shamir = ShamirDb::init(SystemStoreConfig::Fjall(sys_path.clone()))
            .await
            .unwrap();
        shamir.create_db("testdb").await.expect("create_db");
        // No inline tables: the installed seed image carries the `users`
        // inventory.
        shamir
            .add_repo(
                "testdb",
                // RAW fjall (no MemBuffer): this test binds a SUBSCRIPTION to
                // this repo, and the CREATE/RESUME admission
                // (validate_repo_wide_atomic_batch) refuses MemBuffer-wrapped
                // destinations with `atomic_batch_unsupported`. Raw fjall is also
                // the backend the seed writes through, so the durable directory
                // survives the reopen below.
                RepoConfig::new("main", BoxRepoFactory::fjall_raw(repo_path.clone())),
            )
            .await
            .unwrap();
        // Round 7 item 2 fixture admission rule: each row keeps a
        // SINGLE-stream scope (`matches_scope`, `catalogue.rs`); the overlap
        // on `testdb/main` comes from TWO single-destination subscriptions.
        quarantine(&shamir, "sub1", "prof", "main").await;
        quarantine(&shamir, "sub2", "prof2", "main").await;
        assert_absent_quarantined(&shamir, read_users(&shamir, "main").await, "testdb", "main")
            .await;

        // Durable-sequence generations are monotonic (values unpredictable;
        // only ordering matters).
        let before = super::super::catalogue::rows(&shamir, "subscriptions")
            .await
            .unwrap();
        let gen_before = |name: &str| {
            before
                .iter()
                .find(|r| r.get("name").and_then(|v| v.as_str()) == Some(name))
                .unwrap_or_else(|| panic!("subscription '{name}' must exist"))
                .get("binding_generation")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| panic!("'{name}' must carry binding_generation"))
        };
        let sub1_gen_before = gen_before("sub1");
        let sub2_gen_before = gen_before("sub2");

        // Real recovery: install the image ONCE, rebind BOTH rows (same
        // receipt).
        recovery::install_replacement_image(&shamir).await;
        recovery::rebind_to_replacement(&shamir, "sub1", "prof").await;
        recovery::rebind_to_replacement(&shamir, "sub2", "prof2").await;

        // Raw-row assertions: still quarantined, generations strictly
        // bumped (sub2 > sub1), REPLACEMENT receipt on both.
        let records = super::super::catalogue::rows(&shamir, "subscriptions")
            .await
            .unwrap();
        let mut generations = std::collections::BTreeMap::new();
        for name in ["sub1", "sub2"] {
            let row = records
                .iter()
                .find(|r| r.get("name").and_then(|v| v.as_str()) == Some(name))
                .unwrap_or_else(|| panic!("subscription '{name}' must exist"));
            assert_eq!(
                row.get("quarantined").and_then(|v| v.as_bool()),
                Some(true),
                "'{name}' must still be quarantined after the rebind"
            );
            let generation = row
                .get("binding_generation")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| panic!("'{name}' must carry binding_generation"));
            let gen_before = if name == "sub1" {
                sub1_gen_before
            } else {
                sub2_gen_before
            };
            assert!(
                generation > gen_before,
                "'{name}' must carry its monotonic replacement generation"
            );
            generations.insert(name, generation);
            assert_eq!(
                row["seed_receipt"]["source_identity"]["history_epoch"].as_u64(),
                Some(2),
                "'{name}' must carry the REPLACEMENT receipt (history_epoch 2)"
            );
        }
        let sub1_gen_after = generations["sub1"];
        let sub2_gen_after = generations["sub2"];
        assert!(
            sub2_gen_after > sub1_gen_after,
            "creation + bump ordering: sub2's generation must exceed sub1's"
        );
        // Rebinding alone never unblocks.
        let err = read_users(&shamir, "main").await.unwrap_err();
        assert_eq!(err.code(), Some("resync_required"));
    }

    // Reopen with the bounded fjall-lock retry (mirrors `durable_repo_tests`).
    let mut shamir = None;
    for _ in 0..100 {
        match ShamirDb::init(SystemStoreConfig::Fjall(sys_path.clone())).await {
            Ok(opened) => {
                shamir = Some(opened);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    }
    let shamir = shamir.expect("system store still locked after retries");

    // The quarantine survives the reopen (cache rebuilt from the persisted
    // durable bindings).
    let err = read_users(&shamir, "main").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"), "{err}");

    // Fresh probe for the new instance; both rows were rebound to the same
    // replacement receipt, so one probe serves the Resume.
    shamir.set_replication_resume_probe(recovery::probe());
    resume_subscription(&shamir, 6, "sub1").await.unwrap();
    // Round 7 item 2 — coverage aggregation, NOT a second active writer:
    // sub1's Resume succeeds, but the SHARED `main` STILL refuses (sub2's
    // quarantined row covers it). The test ENDS here: resolving sub2 needs
    // a second reseed, and the rule needs no second writer. (There is no
    // exclusive destination anymore: each row's single bound destination IS
    // the shared one.)
    // COUNTERFACTUAL: pre-round-6 semantics (a resumed row clears the
    // shared destination's protection) turn the still-refused `main` red.
    let err = read_users(&shamir, "main").await.unwrap_err();
    assert_eq!(err.code(), Some("resync_required"), "{err}");
}
