בְּשֵׁם יהוה הָרַחֲמָן וְהַחַנּוּן

# Этаж 8 — Interconnect: P2P / Chat (the "I")

**Когда подниматься:** нужна децентрализация.

Буква **"I"** в S.H.A.M.I.R. — Interconnected. Это самый высокий этаж
спирали. Часть его (changefeed, network pull, leader-follower
репликация, live subscriptions) уже реализована и покрыта тестами;
верхний слой — децентрализованный P2P/gossip/chat — ещё нет. Здесь
честно описано, что существует, что планируется и почему.

> ⚠ **Этот этаж — частично реализация, частично роадмап.**
> Leader-follower репликация, network changefeed (pull API) и live
> subscriptions (server-push) реализованы и покрыты тестами — см.
> §1. Но P2P-протокол, gossip, peer discovery и chat **не имеют
> рабочего кода** — это по-прежнему чистый роадмап. Не пытайся
> использовать примеры Шага 4 ниже как API — их нет.

## 1. Что существует сегодня

### Changefeed — the substrate under replication

Базовый строительный блок для "I" — локальный changefeed
(`shamir-tx/src/changefeed.rs`):

* **Hybrid live-push** (`tokio::sync::broadcast`) + **durable journal**
  (append-log, keyed by `commit_version`).
* `ChangelogEvent` = `{ repo, commit_version, tx_id, actor, timestamp_ns,
  changes: Vec<RecordChange> }`.
* `read_from(version, limit)` — resumable pull API.
* `commit_version` identifies the MVCC version (issue #179), but journal
  publication is not guaranteed to follow version order under concurrent
  writes. A follower can advance past a late event and silently miss it.
* Gap markers (`gap_at`) and a persistence watermark exist, but gap
  detection is incomplete; see the replication limitations below.
* E2E-тесты: `shamir-db/tests/query_dml/changefeeds/changefeed_e2e.rs`.
* **Журнальное событие выводится из WAL-записи (S5c-X, инвариант I1).**
  WAL-запись формата версии 3 несёт заголовок события
  (`shamir_wal::WalEventHeader`: repo, actor, публичный tx_id — 0 для
  implicit-записи, timestamp, имена таблиц, редкие изменения с
  не-`RecordId` ключами). Единственная функция
  `shamir_tx::event_from_wal_entry` строит `ChangelogEvent` из записи;
  live-коммит использует именно её. Записи старых форматов (версии 1-2)
  декодируются без заголовка — событие из них вывести нельзя, recovery
  трактует такие коммиты консервативно (GAP). Неизвестная будущая версия
  — ошибка декодирования, никогда не молчаливый пропуск.

Начиналось как **локальный** механизм (внутри одного процесса). Сейчас
поверх него уже построен network-транспорт — pull API по wire-протоколу
(`ReplRequest::Pull`) и push-доставка подписчикам (live subscriptions),
см. ниже.

### Транспорты

`shamir-transport-tcp` и `shamir-transport-ws` — клиент↔серверные
транспорты (TLS 1.3, framing). Они не содержат peer-discovery, gossip
или mesh-логики.

### Network changefeed (pull API over wire) — реализовано

`ReplRequest::Pull` is an implemented wire operation
(`crates/query/shamir-query-types/src/wire/repl.rs`). An authorized follower
requests changelog events for one repo from `from_version`, optionally
using long-poll (`wait_ms`). `ReplResponse::Pull` carries `leader_epoch`
(VR-style fencing), msgpack-encoded `events`, optional `gap_at` for a
reported journal gap, and `current_version` for lag calculation. A missing
`gap_at` does not prove a complete journal; no snapshot/reseed is available.
Dispatcher: `crates/server/shamir-server/src/db_handler/repl_handler.rs`.

### PR1: локальный WAL и replication lag

Durable Pull ограничен `min(confirmed, W, R)`; R — **непрерывный префикс
recoverability**, а не максимальная версия, прошедшая fsync. При нехватке
R pull запрашивает coalesced demand sync и ограниченно ждёт продвижения R;
на ошибке/таймауте остаётся старый безопасный bound. Lag — не обязательное
ожидание 250 мс фонового tick: journal confirmation, WAL recoverability и
скорость follower apply — разные источники отставания. Не продвигайте
bookmark по одному лишь `current_version` вместо реально применённых событий.

Серверный `durability.wal_commit: buffered` подтверждает локальный WAL write
на файловом backend; process crash ≠ power loss. Номинальные 250 мс
background fsync не hard maximum (storage errors и задержки могут растянуть
окно). `synced` ждёт coalesced fsync, но не follower. Mem/hybrid не persistent,
tier игнорируется с одним warn при open. Retention budget/action parsed only
до PR2; `declare_gap` не включает эвикцию. `replication_serve` принимает `durable` (default) и `completed_reseed`: с PR8
`completed_reseed` поддерживается — safe convergence, reseed после unclean
restart (см. [07-operations](./07-operations.md)).

`shamir_wal_synced_lag_versions`, `shamir_wal_sync_now_calls`,
`shamir_replication_pull_held_back_ns` и `..._held_back_count` существуют в
exporter с labels `db` и `repo`; источники принадлежат repo/WAL instance.
Sync calls учитываются сразу при demand, rounds — после завершения;
duration включает snapshot replay и sync, не только fsync syscall. Подробнее:
[durability](./02-durability.md) и [operations](./07-operations.md).
PR1 не превращает экспериментальную репликацию в HA/failover и не заменяет
проверки identity/schema/gaps при seeded Resume.

### Leader-follower репликация — реализовано (Experimental)

Follower-цикл, супервизор и источники (`InProcess`/`Wire`) живут в
`crates/server/shamir-server/src/replication/`: `follower_loop.rs`,
`supervisor.rs`, `source.rs`, `wire_source.rs`, `in_process.rs`,
`prod_factory.rs`. Follower подключается к leader'у через
`ReplRequest::Hello` (handshake — версия протокола, epoch, список
реплицируемых репо) и затем циклически тянет `Pull`, применяя события
локально. Тесты:
`replication::tests::{follower_loop_tests, supervisor_tests}` (lib),
плюс e2e-сьюты `crates/server/shamir-server/tests/repl_pull_e2e.rs`,
`repl_convergence_e2e.rs`, `repl_supervisor_boot.rs`,
`set_replicator_wire.rs`.

### Offline seeded subscriptions: paused install and validated Resume (SEED-4)

This contract supersedes the older metadata-only Resume description below for
seeded repositories. Replication remains experimental; this is offline R2-lite,
not online snapshots, automatic reseeding, promotion, or full disaster recovery.
The operator runbook for the full recovery procedure (export / verify /
install / Resume, refusal codes, failure boundaries) is
[`09-seed-recovery.md`](./09-seed-recovery.md); the artifact wire/format
contract is [`../architecture/SEED_FORMAT.md`](../architecture/SEED_FORMAT.md).

On Unix, `replication-seed-install --db app --repo main --artifact DIR
--manifest-digest DIGEST --subscription app-main --upstream ENDPOINT
--publication PUBLICATION --profile PROFILE` installs a new isolated image and,
as its final durable step, creates an exact whole-repository pull profile and a
**paused** subscription bound to the repository's Complete receipt. No follower
loop or Pull starts while paused. An exact artifact/identity/arguments retry
finishes a missing subscription without importing rows again; an existing
matching subscription is left unchanged, including if already active. Conflicts
are refused. Durable publication is unsupported off Unix. Offline authorization
is OS filesystem authority; protect seed artifacts as sensitive data.

Online `Resume` still requires root `Manage`. Before writing active state it
contacts the real authenticated upstream using Hello and validates the named
publication and local read-only pull profile as exactly the receipt's complete
`(db, repo)` scope. Hello publication scopes are advertised only when all their
repository read checks succeed. An older peer without publication metadata is
refused. Complete installation, lineage, history epoch, canonical schema,
settled source position, certified affected horizon, exclusive upstream owner,
and checked bookmark-plus-one are required. Refusal leaves the subscription
unchanged and returns a distinct `seed_*` code. Dropping/recreating a subscription
does not remove the repository receipt or bypass these checks.

The follower fences Hello and every Pull's identity/schema metadata **before
payload decoding or apply**. Missing identity, a new lineage/history epoch, or
schema divergence is a terminal typed reason, persisted for status. DDL is not
replicated in R2-lite: freeze source DDL for this relationship. A new certified
gap beyond the bookmark still stops the follower; no certificate is cleared and
no missing interval is excused. The first pull after seed version V is V+1.

Root `List` / `ReplicationStatus` reports per repository:
`installation_state`, `artifact_digest`, `source_db`, `source_repo`,
`source_lineage_id`, `source_history_epoch`, `seed_source_version`,
`upstream_bookmark`, `next_pull_version`, `local_baseline_version`,
`local_consistent_version`, `schema_digest`, `certified_gap_first`,
`certified_gap_through`, `subscription_state`, and `last_terminal_reason`.
Source seed/bookmark/next-pull positions are **upstream versions**; baseline and
consistent positions are **destination-local versions** (seed baseline 1).
The certified interval shown is the destination repository's certificate.
New peer metadata is additive/defaulted so old payloads decode, but seeded
activation fails closed when required evidence is absent. The full recovery
runbook and real process-crash acceptance scenario belong to SEED-5.

### Live subscriptions (server-push) — реализовано

`crates/server/shamir-server/src/subscriptions/` (`bridge.rs`, `registry.rs`,
`target_match.rs`, `filter_eval.rs`, `reactive.rs`, `push.rs`,
`decode_cache.rs`, `deliver_cache.rs`, `payload.rs`) — сервер держит
живое соединение и отправляет unsolicited push-фреймы с изменениями,
матчащими подписку клиента. При отказе повторной ACL-проверки во время activation весь ответ запроса заменяется фиксированным `unknown_table` (`table is unavailable`), хотя более ранние записи batch уже могли commit-иться. Раньше, ещё на batch-preflight (до выполнения любой операции batch), каждый источник подписки классифицируется: отсутствующий ИЛИ закрытый для актора источник отказывает весь запрос ОДИН раз фиксированным `unknown_table` (`table is unavailable`) — без частичной подписки и без деталей по конкретным таблицам (`SUBSCRIPTIONS.md`, §7). На wire-уровне это `BatchOp::Subscribe`
(`SubscribeOp`) / `BatchOp::Unsubscribe` (`UnsubscribeOp`)
(`crates/query/shamir-query-types/src/batch/batch_op.rs`), Не путать с DDL
`CreateSubscription`/`DropSubscription`/`AlterSubscription`/`ListSubscriptions`:
это подписки **репликации** (follower → leader, `REPLICATION.md §5.5`), а
не клиентский push. Тесты:
`subscriptions::tests::{bridge_tests, registry_tests,
target_match_tests, filter_eval_tests, filter_lens_parity_tests,
cache_eviction_tests, cache_depth_probe_tests, reactive_limits_tests}`
— широкая, реальная сьюта.

### Что не существует

|| Компонент | Статус |
||---|---|
|| P2P-протокол / gossip | ❌ Нет кода |
|| Peer discovery | ❌ Нет кода |
|| Chat-протокол | ❌ Нет кода |
|| `shamir-interconnect` crate | ❌ Не существует |

## 2. Планируемая архитектура

Из роадмапа (`docs/dev-artifacts/roadmap/PLAN.md`, Movement C — "parked by request").
Шаги 1–3 ниже уже реализованы (см. §1); Шаг 4 остаётся чистым роадмапом:

```
                ┌─────────────────────┐
                │  Этап F: Interconnect  │
                └─────────┬───────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
   Network changefeed  Live subs     Leader-follower
   (pull API over      (server-push   replication
    existing journal)   to clients)   (follower subscribes)
     ✅ реализовано     ✅ реализовано   ✅ реализовано
          │               │               │
          └───────────────┼───────────────┘
                          ▼
                    P2P / gossip → chat
                    ❌ роадмап (Шаг 4)
```

### Шаг 1: Network changefeed — реализовано

Wire-запрос поверх существующего durable journal:
`ReplRequest::Pull` (`crates/query/shamir-query-types/src/wire/repl.rs`).
Реальные поля (не иллюстративный набросок) —

```
REQUEST  → ReplRequest::Pull { db, repo, from_version: 42, limit: 100, wait_ms: Some(5000) }
RESPONSE ← ReplResponse::Pull { leader_epoch, events: <msgpack Vec<ChangelogEvent>>, gap_at: None, current_version }
```

`from_version` is a `commit_version`. The follower records the last
applied version and requests subsequent events after reconnecting;
`wait_ms` enables long-poll. This is not a guarantee of complete replay:
late or missing journal events can be skipped without `gap_at`. A reported
gap stops replication; recovery is manual, not a snapshot/reseed API
(see Step 3).

### Шаг 2: Live subscriptions (server-push) — реализовано

Транспортный механизм для server→client push:
`crates/server/shamir-server/src/subscriptions/`. Design doc (история задачи):
`docs/dev-artifacts/roadmap/LIVE_SUBSCRIPTIONS.md`.

* Wire-операции: `BatchOp::Subscribe(SubscribeOp)` /
  `BatchOp::Unsubscribe(UnsubscribeOp)`
  (`crates/query/shamir-query-types/src/batch/batch_op.rs`).
* Сервер отправляет unsolicited push-фреймы с change-событиями,
  матчащими зарегистрированную подписку (`registry.rs`,
  `target_match.rs`, `filter_eval.rs`, `reactive.rs`, `push.rs`).
* Публикации/подписки управляются DDL-операциями
  (`CreateSubscription`/`DropSubscription`/`AlterSubscription`/
  `ListSubscriptions`).

### Шаг 3: Leader-follower replication — реализовано (Experimental)

Follower подключается к leader'у и тянет changefeed:

```
Follower  ──ReplRequest::Hello──►  Leader   (handshake: proto_ver, node_id)
Follower  ◄──ReplResponse::Hello──  Leader   (leader_epoch, реплицируемые репо)
Follower  ──ReplRequest::Pull────►  Leader   (from_version, limit, wait_ms)
Follower  ◄──ReplResponse::Pull──  Leader   (events, gap_at?, current_version)
Follower  ──apply changes───────►  Local store
```

The intended result is a single-leader async read replica, **not a
clustering/HA solution and not for production data**. The follower pulls
and applies changes through
`crates/server/shamir-server/src/replication/follower_loop.rs` and `supervisor.rs`.
VR-style epoch fencing protects against events from a stale leader, but
**`ReplMode::ReadOnly` is not enforced: followers accept local writes**.

* **Upstream identity pinning.** `[replication].upstream_pins` maps normalized
  upstream addresses (`tcp://`, `tls://` and bare `host:port` share a key) to
  lists of 64-hex SHA256 hashes of the leader Ed25519 public key. Configured
  pins are the only durable, verified enrollment. Without a configured pin,
  the first successful connection in each process is trusted (TOFU), logged
  at `warn` with a ready-to-paste enrollment line, then enforced for the rest
  of that process. Learned pins are not persisted and are re-learned after
  every restart. The leader logs its identity pin at `info` on boot for
  out-of-band comparison. During key rotation, add the new pin alongside the
  old one during the 7-day overlap or the subscription stops with
  `upstream_key_mismatch`.
* **Map records cannot reliably be read on the follower.** Field keys are
  leader interner ids; events carry no id-to-name bindings, so decoding can
  fail or return wrong field names. Per-event field-name translation is planned.
* **Follower secondary indexes are unsupported and fail closed.** Events
  touching any indexed table (hash, unique, sorted, FTS, functional or vector)
  are rejected before any data or version is applied, naming the table and
  index. Full index maintenance is a later project. **`COUNT(*)` is maintained
  best effort:** per-table current pre-images and the final operation per key
  determine the delta; successful data apply is followed by counter increment
  before the next table. Raw replay and retry after an earlier table succeeded
  do not double-count. Increment failures are logged, not repaired by replay;
  counter persistence/crash repair and failures after MVCC overlay publication
  are not atomic with data apply.
* **DDL, schema and metadata are not replicated**, only transaction data
  writes. Create the same databases, repos and tables on the follower first;
  **do not create secondary indexes there**. A missing repo causes backoff
  (`ReplError::UnknownFollowerRepo`), but an event for a missing table is
  skipped with a warning and its bookmark still advances, losing those writes.
* **Journal holes can be silent.** Events are queued after version
  publication, outside the commit lock. Concurrent leader writes can journal
  11 before 10; once a follower applies 11, it never asks for 10. Persistence
  failures are only logged, corrupt journal records are silently skipped,
  the in-memory gap marker is forgotten on leader restart, and a crash can
  lose the unflushed journal tail. Gap detection does not catch all of these;
  a follower can silently miss an event under concurrent leader writes.
* **Detected gaps stop the loop** with `ReplError::JournalGap` and mark the
  subscription `resync_required` (visible via
  `ReplicationStatus`/`ListSubscriptions`). Runtime checkpoints retain WAL only
  until the settled journal prefix is durably checkpointed; `refuse_writes`
  blocks new admissions when the cap is exceeded, while `declare_gap` records a
  durable GAP but never evicts history-unsafe WAL. There is still no automatic
  snapshot/reseed, and cold reconciliation remains PR4, so crashes can leave
  genuine gaps. If behind a gap, recreate the follower from
  scratch with bookmark 0 and full replay, only if the leader's journal has
  no hole; otherwise restore the follower from a backup. `Resume` only
  changes subscription state: it does not repair data or fix journal holes.
* **Two subscriptions for the same `(db, repo)` start two loops**, and
  bookmark advancement is not atomic. Bookmarks live in `__tx__` without
  flush ordering against follower data; after a crash a bookmark may be
  ahead of the data. This persistence risk is **not verified on the on-disk
  engine**.

Existing operational limits remain: one shared `replicator` account
(username+password) authenticates all follower subscriptions; there is no
per-subscription credential store. The supervisor is **reconcile-driven,
not event-driven**: catalog changes (`system/subscriptions`) are picked up
on `reconcile()`/`notify_changed()`, not by continuous observation. There
is no leader election, quorum, automatic failover, multi-primary or
sharding. There is no replication on/off configuration switch: the follower
supervisor always boots, and leader-side `ReplRequest::Pull` is available
to any superuser/replicator session. Replication is inactive until
subscriptions and replicator credentials exist. See
[Known Limitations §5](../KNOWN_LIMITATIONS.md#5-replication).

### Шаг 4: P2P / gossip → chat

Децентрализованное состояние. Несколько ShamirDB-инстансов образуют mesh:

* **Gossip** — распространение метаданных (кто жив, какие данные у кого).
* **Anti-entropy** — фоновая синхронизация: Merkle-tree → diff → merge.
* **Chat** — прикладной слой поверх mesh: messaging, collaboration.

Это конечная цель "I" — полностью децентрализованная СУБД без
единой точки отказа.

## 3. Где это в роадмапе

Из `docs/dev-artifacts/roadmap/STAGES.md`:

```
A (эксплуатация) → C (доки) → B (бизнес-доступ) → D (поиск) →
E (перф) → F (P2P)
```

**Этап F — последний.** Перед ним: эксплуатация, документация,
бизнес-доступ, поиск, производительность. Это осознанный выбор:
P2P строится на стабильном фундаменте, а фундамент ещё дорабатывается.

## 4. Сквозной принцип и "I"

Из README:

> Истина живёт в **одном** месте — версионированном MVCC-сторе. Всё
> производное (кеши, индексы, проекции) — overlay над ней,
> восстановимый из WAL.

"I" не ломает этот принцип. Каждый peer владеет **своей** истиной
(свой MVCC store). Репликация — это **broadcast изменений** (через
changefeed), а не distributed lock. Конфликты — на уровне application
logic (CRDT, last-write-wins, merge-функции).

## Что важно знать уже сейчас (дозированно)

* **Changefeed and network pull are implemented; leader-follower
  replication remains experimental and not for production data.** Version
  ids, `from_version`/`wait_ms`, `gap_at` and VR-style epoch fencing exist,
  but journal publication is not reliably ordered and gap detection is
  incomplete. Reported gaps stop the loop with `resync_required`; there is
  no snapshot/reseed. Follower reads, indexes, counters and bookmarks have
  the limitations listed in Step 3; no election/quorum/failover/multi-primary/
  sharding is provided. Replication uses `ReplRequest`/`ReplResponse`.
  Live subscriptions (server-push) are implemented and covered by tests,
  using `BatchOp::Subscribe`/`Unsubscribe`.
* **P2P, gossip, peer discovery и chat — по-прежнему роадмап.** Не жди
  децентрализованного mesh в ближайших релизах.
* **Смотри роадмап.** `docs/dev-artifacts/roadmap/STAGES.md` и `docs/dev-artifacts/roadmap/PLAN.md`
  — актуальное состояние планов.
