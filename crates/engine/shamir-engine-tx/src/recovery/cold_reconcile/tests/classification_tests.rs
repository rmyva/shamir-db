use crate::tx::cold_reconcile::classification::{classify, Evidence};

#[test]
fn wal_header_distinguishes_eventless_from_legacy_unknown() {
    let legacy = shamir_wal::WalEntryV2::new(1, 1, Vec::new());
    assert_eq!(classify(&legacy), Evidence::LegacyUnknown);

    let eventless = shamir_wal::WalEntryV2::new(2, 1, Vec::new()).with_event_header(
        shamir_wal::WalEventHeader {
            repo: "repo".to_string(),
            actor: shamir_types::access::Actor::System,
            tx_id: 0,
            timestamp_ns: 0,
            table_names: Vec::new(),
            extra_changes: Vec::new(),
        },
    );
    assert_eq!(classify(&eventless), Evidence::Eventless);
}
