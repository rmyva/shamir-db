# reference_epoch_1 — reference format data directory

GENERATED — never hand-edited. Produced by
`scripts/runners/make_reference_datadir.py` (run with the release binary
against an empty data dir), uploaded by
`.github/workflows/reference-datadir.yml` as artifact `reference-epoch-1`,
and committed here by the orchestrator. Regenerate, do not patch.

## Scenario contract (checked by `tests/misc/reference_compat.rs`)

- db `default`, repo `main`.
- Table `ref_schema_basic`: schema_version >= 1, rows (k, n):
  r1/10, r2/20, r3/30.
- Table `ref_schema_versioned`: schema_version > 1, rows: v1/7, v2/8.
- Indexes on `ref_schema_basic`, one per kind, named
  `ref_hash_idx`, `ref_sorted_idx`, `ref_vector_idx`, `ref_fulltext_idx`.
- Functions: `ref_wasm_fn` (compiled, loaded) and `ref_lang_fn`
  (`shamir` source with `call_signature`; `ref_lang_fn(x) = x + 1`).
- Principals: `admin` (superuser, password
  `reference-epoch-1-admin`), `ref_user` (password
  `reference-epoch-1-user`, read on `default.main`), plus one access
  token for `ref_user`.
- An unfinished WAL segment (content not yet checkpointed).
- A valid `format.stamp` at epoch 1. No `cache/`, no TLS material, no
  logs; tree size <= ~1 MiB.
