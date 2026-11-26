use shamir_tx::ChangelogEvent;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Evidence {
    Eventless,
    Derived(ChangelogEvent),
    LegacyUnknown,
}

pub(crate) fn classify(entry: &shamir_wal::WalEntryV2) -> Evidence {
    match shamir_tx::event_from_wal_entry(entry) {
        Some(event) => Evidence::Derived(event),
        None if entry.event.is_some() => Evidence::Eventless,
        None => Evidence::LegacyUnknown,
    }
}
