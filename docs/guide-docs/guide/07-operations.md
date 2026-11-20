בְּשֵׁם יהוה הָרַחֲמָן וְהַחַנּוּן

# Этаж 7 — Эксплуатация: рантайм, метрики, логи, конфиг

**Когда подниматься:** выкатка в прод.

## Repository durability changes

Use `ALTER REPO` to patch a repository's durability override. Each successfully applied ALTER produces its own audit event, including operations inside sub-batches and loop bodies; row-result suppression does not suppress the event. Earlier successful operations remain audited if a later operation fails. Failed or suppressed operations do not generate success events.

До этого этажа сервер запускался вручную: `shamir-server --config …` в
терминале. Для разработки этого хватает. Но продакшен требует: сервисный
режим (systemd / Windows service), observability (`/healthz`, `/metrics`),
структурированные логи, rate-limit, audit trail. Этот этаж — о том, как
ShamirDB живёт в проде.

Server configuration is strict: unknown top-level sections are rejected, and `logging.level` is validated against supported levels. Correct section/key names and log-level values before startup; these mistakes are not silently ignored. See [PR1c migration](./PR1C-MIGRATION.md).

## 1. Один бинарник — все режимы
определяется подкомандой:

```bash
# Foreground (разработка / Docker)
shamir-server --config db.ktav

# То же, явно:
shamir-server --config db.ktav run

# Как systemd-сервис (Linux)
shamir-server --config db.ktav run --service
```

Все режимы проходят через одну точку входа — `runtime::serve()`.
Различается только триггер остановки: Ctrl+C/SIGTERM (foreground),
SCM Stop (Windows), `systemctl stop` (systemd). Drain-логика одинакова:
прекратить приём → доработать in-flight (30 s deadline) → долить буферы
→ fsync → отпустить блокировки.

### Foreground

```bash
PW='admin-pass'
printf '%s\n' "$PW" | shamir-server --config /etc/shamir/server.ktav --bootstrap-password-stdin
```

Остановка: `Ctrl+C` (везде) или `SIGTERM` (unix). Процесс доливает все
буферы и выходит.

### systemd (Linux)

```bash
# Установить сервис (генерирует .service + включает):
shamir-server service install --user shamir

# После установки:
systemctl start shamir-db
systemctl status shamir-db
systemctl stop shamir-db
```

`service install` генерирует юнит с абсолютными путями и вызывает
`systemctl enable`. Тип юнита — `Type=notify`: сервер вызывает
`sd_notify(READY=1)` после bind listener'ов — systemd знает, что сервис
готов.

Готовый production-юнит — `deploy/shamir-db.service` (`Type=notify`,
hardening, `RuntimeDirectory=shamir` + `--bootstrap-token-path` для
одноразового bootstrap-токена). Бери файл, а не фрагменты из руководства.

### Windows Service

```powershell
shamir-server service install
```

Регистрирует сервис в Windows SCM. Остановка — через `sc stop` или
`Services MMC`. Recovery policy настраивается через SCM.

### macOS / BSD

`service install` генерирует launchd plist (macOS) или rc.d-скрипт (BSD).

### Per-repository durability

Repository durability can override the server default at creation or be patched while running with `ALTER REPO`. Overrides are sparse: unspecified fields inherit the server policy, and ALTER preserves previously stored fields not present in the patch. The policy is swapped live for the open repository and persisted in its `system.repositories` record; legacy records without a `durability` field mean no override. At boot, a stored override takes precedence over the server default.

Use the query builder for these operations (builder examples are not protocol wire specifications):

```ts
const create = ddl.createRepo('archive', { durability: { wal_commit: 'synced' } });
const alter = ddl.alterRepo('archive', { wal_retention_max_bytes: 1073741824 });
const described = ddl.describeRepo('archive');
```

`DESCRIBE REPO` строится через `ddl.describeRepo('archive')` и сообщает эффективную `durability` и sparse `durability_override`. Rust DTO запроса всегда передаёт retention десятичной строкой. TypeScript принимает положительные безопасные числа, `bigint` или канонические десятичные строки и без потери точности нормализует значения выше `i64::MAX` в текст. В эффективном ответе значения до `i64::MAX` включительно — native MessagePack integer, декодируемый в TypeScript как `number` или `bigint` согласно ширине wire integer; значения выше `i64::MAX` — десятичные строки. Неизвестные поля и недопустимые значения enum отклоняются. Retention должен быть не меньше удвоенного effective WAL segment size. `replication_serve` принимает `durable` (default) и `completed_reseed` (см. ниже). In-memory и hybrid WAL игнорируют `wal_commit`; выбор `synced` не делает их persistent.

### After a leader crash

После нештатной остановки cold open записывает консервативный GAP
`[J + 1, H]`, если восстановленный horizon H больше persisted clean horizon J.
Во время работы перед каждой попыткой удаления запечатанных WAL-сегментов
drainer фиксирует settled-префикс: сначала сохраняет и сбрасывает recovery floor
не ниже текущего J, затем сбрасывает события и GAP-журнал и продвигает J.
Если checkpoint завершается ошибкой, WAL-сегменты не удаляются. При превышении
лимита retained WAL режим `refuse_writes` отклоняет новые транзакции до успешного
безопасного удаления; режим `declare_gap` сначала надёжно записывает GAP и J с
соответствующим recovery floor, затем удаляет только сегменты ниже безопасной
границы history/interner. Если журнал недоступен, запись отклоняется. Для
in-memory/hybrid репозиториев без постоянного WAL checkpoint и ограничение
retention не применяются. Холодная сверка сохранившихся WAL-событий относится
к PR4, поэтому runtime-checkpoint не устраняет все возможные GAP после сбоя.
Follower при холодном старте (PR6c) сертифицирует журнал через J из своей
bookmark-записи: неопределённым остаётся только in-flight батч; без известного
J действует прежний консервативный gap.
Follower при чтении через известный GAP останавливается с `JournalGap`, не
применяя сопутствующие события и не продвигая bookmark.

Дополнительно действует универсальный identity-fence (S5c-X PR7) на КАЖДОЙ
подписке — с сидом и без: follower записывает `(lineage, history epoch)`
лидера при первом успешном Hello/Pull в один durable record со своим
bookmark; каждое продвижение bookmark сохраняет identity. Отсутствующая
identity запрещает даже первый контакт; отключения fence в production нет.
Старый bookmark без identity принимает её один раз с WARN. Follower терминально останавливается с `seed_wrong_identity` /
`seed_wrong_history_epoch` (структурированные `recorded_identity` и `observed_identity`
в `ReplicationStatus`),
если позже лидер ответит другой lineage или epoch. Перезапущенный
`in_memory`/`hybrid` лидер получает новый lineage (его `__tx__` живёт
только в RAM) и потому останавливает подписки; durable (`fjall`) лидер
сохраняет идентичность. Ремонт — операторский: проверить историю лидера и
пересидировать (см. `guide/09-seed-recovery.md`).

1. Остановите replication и writes; сохраните **offline** backup лидера и
   followers. Не копируйте живой data_dir: backup — stop-and-copy, не online
   consistent snapshot.
2. Не используйте `Resume` как ремонт и не поднимайте bookmark без данных.
   Новый follower с bookmark 0 снова встретит сохранённый GAP; clean stop
   лидера и пересоздание subscription тоже не очищают интервал.
3. Для полного replay с нуля выгрузите проверенные authoritative logical data,
   создайте **новую независимую source lineage** с тем же schema и загрузите
   данные через обычные writes (они создают новый gap-free journal). Создайте
   пустые followers с тем же schema и без secondary indexes; пересоздайте
   profiles/subscriptions на новый upstream и запустите replay. Старую lineage
   сохраните offline, не удаляйте GAP metadata вручную. Проверяйте данные и
   достижение нового head, прежде чем вернуть writes.
4. Альтернатива — проверенный follower image с данными **и его matching upstream
   bookmark** через H. Произвольный старый backup недостаточен. Встроенного
   snapshot-plus-bookmark seed command и автоматического reseed нет; такой
   image требует отдельного проверенного tooling/procedure.

Источники: `crates/engine/shamir-engine/src/repo/repo_instance.rs:1829-1877`,
`crates/server/shamir-server/src/replication/follower_loop.rs:311-325`,
`crates/server/shamir-server/src/replication/supervisor.rs:309-334`,
`crates/engine/shamir-db/src/shamir_db/execute/admin_replication.rs:413-467`,
`crates/server/shamir-server/src/backup.rs:3-32`.

## API token listener operations

Bearer APIs and HTTP password login are served by the configured HTTP listener. See [the TypeScript token guide](../../../crates/client/shamir-client-ts/docs/TOKENS.md) for client usage. The examples there assume an HTTPS endpoint at `https://db.example.test`, port 443, with the UI served same-origin (or through a same-origin reverse proxy); token requests do not provide CORS headers.

A minimal illustrative example of **one `listeners[]` entry** for that deployment is:

```ktav
{
    kind: http
    addr: 0.0.0.0:443
    tls: true
    host_authority_allowlist: [db.example.test]
    browser_origin_allowlist: [https://db.example.test]
    trusted_proxies: []
    external_tls: false
    max_request_body_bytes: 16777216
    body_timeout_ms: 30000
    request_timeout_ms: 120000
    login: {
        enabled: true
        authority_allowlist: [db.example.test]
        ## Optional explicit stable identity; if omitted, generated identity is persisted in server metadata.
        listener_id: 0123456789abcdef0123456789abcdef
    }
}
```

A `kind: http` listener requires `tls: true` or `tls: false`; this example uses direct TLS termination and assumes a valid TLS 1.3 certificate. Login configuration contains only `enabled`, `authority_allowlist`, and optional `listener_id`; `external_tls` is a flattened listener-level field. `login.authority_allowlist` defaults to loopback aliases; the example sets the exact HTTPS authority. A supplied listener ID must be stable and exactly 32 lowercase hex characters. `browser_origin_allowlist` is listener-level, separate from `login`. Login BEGIN input is capped at 4096 bytes and FINISH at 256 bytes (not a 4 KiB cap on both messages).

| Key | Type | Default | Scope | Validation / semantics |
|---|---|---|---|---|
| `login.enabled` | bool | `false` | per listener | Disabled routes return 404; enabled wrong method returns 405. |
| `login.authority_allowlist` | array | loopback aliases | per listener | Exact canonical Host authority allowlist. |
| `login.listener_id` | string | generated once | per logical listener | Optional stable 16-byte identity, configured as exactly 32 lowercase hex characters; a generated ID persists in server metadata keyed by the configured listener `addr` (two listeners with the same `addr` share it; changing `addr` generates a new one). |
| `tokens.login_ttl` | u32 seconds | 86400 | global | Range 1–86400, additionally capped by `tokens.max_ttl_days`; invalid configuration fails startup. |
| `tokens.login_allow_read_write` | bool | `false` | global | Opt-in required for read-write login issuance; ordinary rights still apply. |
| `security.login_enumeration_hardening` | bool | `true` | global | HTTP login only; mismatching stored KDF tuples make accounts individually ineligible. Raising defaults may affect existing users; preflight and disclose impact. |

`login.authority_allowlist` is optional in configuration; when omitted on loopback, the launcher derives bound-port loopback aliases. Direct non-loopback TLS and proxy TLS require an explicit nonempty authority list. `login.listener_id` can be generated and persisted if omitted. Listener defaults: `max_request_body_bytes` 16 MiB, body/request timeouts 30 s/120 s, head timeout inherited from `security.connection.auth_init_timeout_ms` (typically 5 s); remote plaintext login must stay disabled.

Shared token policy: `tokens.max_per_user` is 20, `tokens.max_sockets_per_token` is 8, `tokens.max_sockets_per_owner` is 32, `tokens.session_max_age_hours` is 24, `tokens.default_ttl_days` is 30, and `tokens.max_ttl_days` is 365. These are global shared token settings, not login-only defaults.

During shutdown, login workers receive `min(request_timeout_ms, 7s)` and the audit consumer gets up to 2s within the outer 10s shutdown deadline. Detached workers may outlive the listener; runtime stop may interrupt issuance or compensation, leaving a token live until its TTL. Audit is best-effort; events abandoned on abort are not counted as dropped.

`login_enumeration_hardening` is under the actual `security` namespace, not a top-level key. `kdf_override` is forbidden on login-enabled listeners. Login's BEGIN/FINISH body caps (4096/256 bytes) and admission/store capacities are fixed, not tunable; server request body and timeout settings are separate. Actual proxy TLS requires `tls: false`, loopback bind, nonempty `trusted_proxies`, nonempty authority, and nonempty canonical HTTPS origins; combining actual TLS with `external_tls` is invalid. Login head timeout must be <=5000 ms. HSTS is enabled for login over direct TLS; with proxy TLS the external HTTPS edge must provide HSTS. The internal HTTP backend does not itself establish browser HTTPS security.

These names/defaults and profile constraints are defined by `crates/server/shamir-server/src/boot/config/login_listener.rs`, `http_listener.rs`, `boot/config.rs`, `boot/config/tokens.rs`, and enforced by `engine/server/server_launcher/http/launcher.rs`.

For full listener setup, token administration, browser constraints, limits and troubleshooting, see the [token API operator guide](./token-api.md).

## 2. Конфиг: `.ktav`

Конфиг — файл в формате Ktav ≥ 0.6: без кавычек и запятых, комментарии —
только целой строкой с префиксом `##`. Минимальный конфиг:

```ktav
data_dir: /tmp/shamir-test

kdf_defaults: {
    memory_kb: 19456
    time: 2
    parallelism: 1
    argon2_version: 19
}

listeners: [
    {
        kind: tcp
        addr: 127.0.0.1:7000
        profile: tls_exporter
    }
]

tls: {
    cert_path: /tmp/shamir-test/cert.pem
    key_path:  /tmp/shamir-test/key.pem
}
```

Production-конфиг — `deploy/server.example.ktav` (все поля с
комментариями); профили `server.{small,medium,large}.example.ktav` — под
объём RAM. Не копируй фрагменты отсюда: разбор и валидация этих файлов
закреплены тестами (`reference_example_parses_and_validates` и соседние).

### Секции конфига

|| Секция | Назначение |
||---|---|
|| `data_dir` | Корень данных (базы, WAL, сертификаты) |
|| `logging` | Уровень логирования, slow-query threshold, файл |
|| `kdf_defaults` | Параметры Argon2id (memory, time, parallelism) |
|| `argon2_concurrent_max` | Лимит одновременных Argon2-верификаций |
|| `listeners[]` | Сетевые endpoints (TCP / WebSocket, TLS-профили) |
|| `tls` | Пути к сертификату и ключу |
|| `security` | Таймауты, лимиты соединений, query limits |
|| `audit` | Audit log с ротацией (audit-line-формат файла) |
|| `observability` | Адрес HTTP-сервера метрик |

### Лимиты выполнения запросов

`security.query_limits` задаёт серверную policy для `Execute` и `TxExecute`.
Эффективный лимит — минимум из лимита клиента, server-owned `ExecPolicy.caps`
оператора и жёсткого `BatchLimits::CEILING` движка; вложенные батчи не могут
поднять его. Потолки движка: 100 MiB / 1000 queries / 3600 s; это caps,
а не defaults запроса (у клиента defaults 10 MiB / 50 / 30 s).
Источники: `crates/server/shamir-server/src/db_handler/config.rs::policy`,
`crates/query/shamir-query-types/src/batch/batch_limits.rs`.

|| Поле | Default | Максимум при старте |
||---|---|---|
|| `max_result_size_bytes` | 67108864 (64 MiB) | 104857600 (100 MiB) |
|| `max_execution_time_secs` | 60 | 3600 |
|| `max_queries_per_batch` | 100 | 1000 |
|| `max_nesting_depth` | 64 | 64 |
|| `max_dependency_depth` | 1000 | 1000 |
|| `max_iterations` | 10000 | 10000 |
|| `max_total_queries` | 10000 | 10000 |
|| `max_total_work` | 1000000 | 1000000 |
|| `max_page_size` | 100000 | 100000 |
|| `max_offset` | 1000000 | 1000000 |

Defaults и startup validation: `crates/server/shamir-server/src/config.rs:456-487,768-777`;
engine caps: `crates/query/shamir-query-types/src/batch/batch_limits.rs:109-120`.

`max_total_queries` ограничивает число узлов всего дерева батча;
`max_total_work` ограничивает и статическую оценку вложенных батчей/циклов,
и **runtime dispatch**: `ExecScope::charge_work(1)` списывает единицу перед
каждым реально выполняемым alias, включая Batch/ForEach-контейнер и их
вложенные dispatch. Пропущенный по `when` alias не списывает work.
Динамические повторения тоже учитываются; все вложенные исполнения делят
один root counter, предварительный `check_work` не резервирует работу.
Это счётчик dispatch, не строк, CPU-инструкций или времени сканирования
(`crates/engine/shamir-engine/src/query/batch/exec_scope.rs`, `batch_execute.rs`,
`query_runner.rs`). `max_page_size` и
`max_offset` ограничивают пагинацию query, отдельно от `security.cursors`.
Превышение запрошенной страницы/offset возвращает `pagination_too_large`,
работы — `work_budget_exceeded`, вложенности — `nesting_too_deep`,
итераций — `too_many_iterations`.

Все поля выше валидируются при старте. Ноль допустим только для
`max_nesting_depth` (без вложенности), `max_dependency_depth` (только
независимые запросы) и `max_offset` (без пропуска строк). Новые поля по
умолчанию равны CEILING, а не request-default: клиент сохраняет возможность
поднять свои лимиты в пределах движка. `max_result_size_bytes` выше 100 MiB
(потолок движка) отвергается при старте; large-профиль использует ровно
100 MiB.

Для батчей `security.query_limits.enforce_result_size` по умолчанию `true`:
движок консервативно оценивает накопленные материализованные байты и
проверяет `max_result_size_bytes` до commit, включая промежуточные результаты
с `return_result: false`, ForEach и вложенные контейнеры, без возврата бюджета
при фильтрации или удалении результатов. Это не cap сериализованного ответа
и не live RSS. Явное `false` отключает runtime-проверку батчей; оценка
upfront-резерва RI-15 сохраняется. Embedded API остаётся OFF по умолчанию.
Страницы курсоров сохраняют жёсткий байтовый cap `max_result_size_bytes`
на сериализованный ответ.
Embedded `ExecPolicy::EMBEDDED` и WASM `FacadeDbGateway` → `execute_as`
оставляют enforcement OFF (флаг серверный:
`crates/server/shamir-server/src/db_handler/config.rs`,
`crates/query/shamir-query-types/src/batch/exec_policy.rs`,
`crates/engine/shamir-db/src/shamir_db/shamir_db/db_gateway.rs`).

Embedded direct CRUD: `TableManager::{insert, insert_many, delete, set}_with_receipt`
принимает explicit `VisibilityWait` и возвращает must-use `WriteOutcome<T>`:
value + `commit_version: Option<u64>` + visibility. `set` сохраняет `bool`
(created), не unit. Missing delete / empty insert-many / non-MVCC не выдают
позицию 0: version = `None`, без таймера. Bounded Pending — committed, **не retry**;
reconcile на той же incarnation repo, затем открыть новый snapshot. Старые
value-only методы — explicit Strict adapters, даже при bounded repo policy;
reopen-before-visible возвращает `WriteCommittedVisibilityPending`
(`write_committed_visibility_pending`) с committed version, sampled W и reason,
не обычный retryable write failure. Direct CRUD остаётся best-effort/no-WAL,
не transactional atomicity / crash-durability и unsafe to cancel mid-sequence;
отмена только observer освобождает waiter, не отменяет write/recovery. Guards
освобождаются до observation. Аналогичные MVCC receipt APIs используют local
storage/tx visibility types. Implicit batch propagation — VIS-4b; bounded server
default / SDK rollout всё ещё gated. Полный контракт:
[Transaction outcomes](../client-server-protocol-spec/TRANSACTION_OUTCOMES.md).


При включённом enforcement (`ExecPolicy { enforce_result_size: true, … }`) общий root
budget означает **«bytes materialized by this execution»**, а не длину
MessagePack, allocator capacity или live RSS. `ResultSize` детерминированно
оценивает payload и контейнеры: промежуточные результаты, даже не возвращаемые
клиенту, nested Batch и ForEach-оболочки тоже платят. Фильтрация/удаление
результатов не возвращает bytes; внутренние payload контейнера не считаются
повторно при его сборке. Превышение возвращает `LimitExceeded(ResultBytes)`
→ `result_too_large` до решения о commit; уже выполненные non-transactional
операции этим не откатываются. Источники:
`crates/engine/shamir-engine/src/query/batch/exec_scope.rs::charge_bytes`,
`result_size.rs`, `batch_execute.rs`, `query_runner.rs`.

### Время TxExecute и string_agg

Абсолютный срок interactive handle — 300 s. Budget одного `TxExecute` —
минимум effective request/policy time и оставшегося срока handle **с полной
точностью `Duration`**, без округления до секунд и без продления на каждый
вызов. Остаток берётся **после** захвата context lock; authorization тоже
вычитается из него (`crates/server/shamir-server/src/db_handler/tx_handlers.rs:169-181`,
`crates/engine/shamir-db/src/shamir_db/execute/db_tx.rs:158-165`,
`crates/engine/shamir-engine/src/query/batch/exec_scope.rs`). Deadline cooperative:
застрявшая операция не прерывается таймером; перед commit проверяется deadline
и work budget, а начатый commit не отменяется по timeout.

`string_agg` имеет независимые caps, даже при byte enforcement OFF:
separator ≤ **1024 bytes** (не символов), default separator `,`;
каждый accumulator, в том числе отдельная GROUP BY-группа, ограничен root
`max_result_size` (не более 100 MiB). Без execution context cap — 100 MiB.
Размер строки проверяется **до** добавления значения и разделителя; NULL
пропускается. Nested batch наследует root accumulator cap, даже если его
локальный `max_result_size` меньше. Превышение идёт как **query error** с
диагностикой `result_too_large: ResultBytes …`, не `LimitExceeded`, и само по
себе не удаляет interactive handle. Источники:
`crates/engine/shamir-engine/src/query/read/aggregate.rs::CappedStringAgg`,
`crates/engine/shamir-engine/src/query/filter/eval_context.rs`,
`crates/engine/shamir-engine/src/query/batch/query_runner.rs:1194`.

### Migration source freeze и исход commit

При подготовке миграции source table замораживается для data writes:
raise freeze bit → drain writers → write-lock fence. Freeze guard живёт до
завершения/отмены миграции; отмена подготовки снимает bit. Это не удержание
DDL admission на весь срок: DDL остаётся разрешённым. Direct writes и commit
транзакции, попавшие на freeze, получают `table_migrating`; не делайте busy
retry, дождитесь завершения/отмены и заново разрешите активную таблицу
(`crates/engine/shamir-engine/src/table/table_manager.rs::begin_migration_freeze`,
`table_manager_crud.rs`, `crates/engine/shamir-engine/src/tx/pre_commit.rs`,
`crates/engine/shamir-db/src/shamir_db/execute/admin_migration.rs`).

`wal_requires_reopen` требует reopen/reconciliation, новые commits отказаны.
Попытка commit с неизвестным исходом WAL (frame уже поставлен в очередь)
возвращается как `status: "indeterminate"`, `error: "wal_outcome_unknown"`, с ATTEMPTED
`commit_version` (без claim успеха) и `snapshot_version`; reopen и сверка чтением решают, что сохранилось.
Отказ ДО admission (repository уже остановлен) остаётся `status: "aborted"`.
`status: committed` с `error: wal_durability` — уже опубликованные writes,
**не retry и не rollback**: TS `db.tx()` сохраняет warning в committed receipt.
`materialized: false` не означает abort: проекции отложены до recovery на
следующем open; при durability error нельзя обещать crash-durability.
Полный wire contract и cleanup/retry table:
[Transaction outcomes](../client-server-protocol-spec/TRANSACTION_OUTCOMES.md).

### Client visibility receipts (VIS-5a)

Pending is **committed**, never an instruction to resend. Rust exposes
`CommitResult::Committed { transaction, visibility }` from `tx_commit` and
`execute_transactional`; TS `db.tx()` returns `{ status: 'committed', state,
value, transaction, visibility }`. Published `wal_durability` warnings remain
in `transaction.error`, independently of pending/visible. Aborts and
`wal_requires_reopen` remain errors with their original reason; indeterminate
WAL is not proof that writes are absent and is reported as
`status: "indeterminate"`. No value-only transaction convenience
hides these outcomes.

```ts
const receipt = await db.tx(async (tx) => {
  await tx.run(write.insert('items', [{ id: 'receipt-example' }]));
  return 'saved';
}, { repo: 'main', visibilityWait: { mode: 'bounded', timeout_ms: 0 } });
if (receipt.state === 'pending') {
  const version = receipt.transaction.commit_version;
  if (version === undefined) throw new Error('missing commit position');
  const observed = await client.waitForVisibility(
    db.name, 'main', BigInt(version), { mode: 'bounded', timeout_ms: 0 },
  );
  if (observed.state === 'visible') {
    // A fresh builder read opens a NEW snapshot; an old transaction never advances.
    const rows = await db.query('items').rows();
  }
}
// Inspect receipt.transaction.error even when state is visible.
```

Rust reconciliation is `client.wait_for_visibility(db, repo, version,
VisibilityWait::Bounded { timeout_ms: 0 }).await?`. Zero is an immediate
predicate check, not infinity. Strict is explicit and may wait indefinitely.
Use the SAME repository incarnation; names cannot detect deletion/recreation.
Observation proves settlement through a position, not whether an unknown write
committed, and upgrades neither durability nor `async` index visibility.

Rust positions stay `u64`; TS reconciliation/prefix positions are `bigint`.
Transaction version fields accept a documented lossless wire dual (`number |
bigint`); SDK decoding normalizes present versions to bigint. MessagePack
preserves 2^53+1 and u64::MAX without JSON or Number conversion. Transport
timeouts/disconnects mean UNKNOWN acknowledgement, not pending/abort proof.
Never rollback or replay after a commit attempt; reconcile externally without
assuming settlement proves an unknown transaction committed.

Current wire limitation: only `as_of_visibility_timeout` carries structured
`requested`, `visible_through`, `blocking_version`. Other visibility codes
carry code/message only; typed client mappings do not parse diagnostics or
invent missing positions.

### Клиентские квитанции неявных батчей и changes_since (VIS-5b)

Не-транзакционный батч с записями возвращает квитанцию на КАЖДЫЙ write-алиас
в `QueryResult.receipt`. Квитанция — это ИЗВЕСТНОЕ committed-решение:
`pending` — не ошибка и не команда на повторную отправку. Rust:
`client.execute_with_receipts(db, batch, visibility_wait)` возвращает ответ и
`Vec<AliasReceipt>` (alias, transaction, visibility, repo); репозиторий
выводится из table-ref запроса. Если репозиторий вывести однозначно нельзя
(алиас внутри вложенного sub-batch/for-each) — `repo: None`, и
`reconcile_receipts` ОТКАЗЫВАЕТСЯ угадывать типизированной ошибкой.
TS: `client.executeWithReceipts(db, batch, visibilityWait?)` — то же самое;
подавленный алиас (`return_result: false`) приходит «оболочкой» без строк, но
с квитанцией — читайте её из оболочки, а не из строк.

Reconciliation группирует pending-квитанции по репозиторию и делает ОДНО
наблюдение на репозиторий на максимальном pending-позиции; ни одной записи
не отправляется повторно, циклов нет. Позиции привязаны к ТЕКУЩЕЙ инкарнации
репозитория: удаление и повторное создание под тем же именем — это другой
репозиторий, wire этого не различает — не мирите квитанцию через
delete/recreate. После наблюдения открывайте НОВЫЙ снапшот.

```ts
const { receipts } = await client.executeWithReceipts('db', batch, { mode: 'bounded', timeout_ms: 0 });
const pending = receipts.filter((r) => r.visibility.state === 'pending');
// один вызов waitForVisibility на репозиторий, на максимальном позиции:
for (const r of pending) {
  if (r.repo === null) throw new Error('cannot derive repository; refusing to guess');
}
```

`changes_since` теперь типизирован: Rust `client.changes_since(db, repo,
cursor, limit)` (запрос строит query-builder) и TS
`client.changesSince(db, repo, cursor, limit?)` возвращают
`{ changes_since, events, gap_at, visible_through }`. Клиент декодирует u64
точно (`u64` в Rust DTO — включая десятично-строковую форму
`QueryValue::Big`; `bigint` в TS; курсор принимает `number | bigint`,
значения выше 2^53 — только `bigint`). ОГОВОРКА: сервер сегодня семплирует
и кастирует эти позиции через `i64`, поэтому значения выше `i64::MAX` он
сейчас не производит — lossless-декодирование это клиентская гарантия, а не
свойство текущего wire. `visible_through` — выборка settled-префикса W ДО чтения
журнала: это образец settlement, а НЕ сертификат покрытия. Пустой `events` —
не доказательство abort/vanish pending-записи; подтверждение журнала может
отставать от W; события по-прежнему ограничены cursor/limit/gaps/retention.
НЕ двигайте курсор событий до `visible_through` только на основании этого
поля; `WaitForVisibility` остаётся основным reconciliation-API, `gap_at` и
retention требуют ресинхронизации.

### Серверная durability и диагностика replication lag (PR1)

Блок `durability` задаёт серверный default (см. [Этаж 2](./02-durability.md)):
`wal_commit: buffered`, `wal_retention_max_bytes: 1073741824`,
`on_wal_retention_exceeded: refuse_writes`, `replication_serve: durable`.
`wal_commit: synced` ждёт coalesced WAL fsync; клиентский `buffered` его
не ослабляет. Для файлового WAL `buffered` ack — локальный WAL write:
падение процесса не равно потере питания. Фоновый таймер 250 мс номинальный,
**не hard maximum**; проверяйте storage errors и задержки scheduler/I/O.
In-memory/hybrid WAL не persistent; tier игнорируется с одним warn при open.

Boot проверяет enum/неизвестные вложенные ключи и retention floor
`>= 2 × effective segment bytes` через общий resolver, включая env override
`SHAMIR_WAL_SEGMENT_MAX_BYTES`, до init базы. Верхнеуровневый детектор
похожих на durability опечаток не добавлен. `resolve` принимает и
`completed_reseed` (см. ниже).
Retention budget и overflow action **только parsed/validated до PR2**:
не считайте 1 GiB enforced лимитом диска, `declare_gap` пока только warns.

Durable Pull ограничен `min(confirmed, W, R)`, где R — непрерывный префикс
recoverability (не max fsynced version). Pull запрашивает coalesced sync
по требованию и ограниченно ждёт продвижения R; ошибки/таймауты сохраняют
старый safe bound. Lag может включать journal confirmation, uncovered WAL
prefix и собственно скорость apply follower; это разные причины.

### replication_serve: durable и completed_reseed (S5c-X PR8)

`durability.replication_serve` задаётся как серверный default (блок
`durability`, `deny_unknown_fields`) и как per-repository override
(`CREATE`/`ALTER REPO` `durability`). Режимы:

- `durable` (DEFAULT): отдача ограничена `min(confirmed, W, R)`, где
  R — непрерывный префикс recoverability (WAL fsync coverage).
- `completed_reseed`: отдача ограничена `min(confirmed, W)` — completed
  prefix БЕЗ R-clamp. Выбирайте его, когда минимальная replication latency
  важнее, и вы принимаете reseed-after-crash. Это
  "safe convergence, not lossless replication: speculative observations
  may be retracted, and an unclean restart conservatively costs a reseed
  even if the bytes survived."

Модель lifecycle/epoch под `completed_reseed`:

1. До первой speculative страницы пишется и сбрасывается durable lifecycle
   record `Armed` (per-repo, в `__tx__` info store; атомарный fast path
   избавляет от его повторного чтения в каждом pull).
2. Чистая остановка (admission stopped, writes settled, durable data/WAL,
   journal horizon через `record_clean_horizon`, allocation evidence)
   пишет сертификат `Clean` ПОСЛЕ того, как всё это зафиксировано. Под
   `durable` shutdown ничего не пишет.
3. При открытии под `completed_reseed` запись `Armed` без более позднего
   `Clean` = unclean restart: recovery завершается приватно, затем
   `history_epoch` инкрементируется durably (checked add; неудачный
   bump/flush БЛОКИРУЕТ admission, overflow тоже), старая replication
   continuity инвалидируется (lineage остаётся STABLE — двигается только
   epoch), после чего service допускается. Это стоит reseed, даже если
   WAL-байты пережили сбой — консервативно by design.
4. Fencing (PR7): каждая subscription/bookmark привязана к lineage+epoch
   (fence выполняется на Hello и перед каждым Pull, до любого
   decode/apply). Follower со старым epoch в bookmark получает отказ;
   supervisor помечает подписку `resync_required` (counter
   `shamir_repl_resync_required_total`) и она остаётся в карантине до
   установки replacement seed. Legacy peers без lineage/epoch identity
   отвергаются (`MissingIdentity`), fail-closed.
5. Переключение `durable` → `completed_reseed` при существующей записи
   `Armed` безопасно: следующий fast-mode open видит Armed-without-Clean
   и делает bump один раз. Обратное переключение не стирает `Armed`; под
   `durable` он инертен.
6. In-memory и hybrid: ничего не переживает рестарт (identity store
   RAM-backed — каждый reopen, чистый или нет, выпускает новую
   incarnation/new lineage); для репликации такие репозитории
   обслуживаются только с явным `completed_reseed`.
7. Карантин исключает обычные чтения (ordinary reads, PR8b): пока подписка
   в `resync_required`, чтения (get/query/scan, включая temporal
   AsOf/History) и локальные changefeed/journal pull покрытых репозиториев
   возвращают ошибку `resync_required` (non-retryable). Карантин хранится
   как durable-маркер `quarantined` на строке подписки (плюс baseline
   `resync_from_receipt`), поэтому он ПЕРЕЖИВАЕТ Pause/SetProfile: Pause,
   SetProfile и обычный `Resume` карантинной подписки отклоняются той же
   типизированной ошибкой `resync_required` — сменой состояния карантин не
   снять. Оператор видит причину через `ReplicationStatus`/
   `ListSubscriptions` (state + status_message), DESCRIBE и статус остаются
   читаемыми. Порядок действий оператора: 1) увидеть карантин через
   status/list; 2) `DROP` карантинной подписки; 3) установить replacement
   seed настоящим установщиком по runbook (`09-seed-recovery.md`) — в НОВЫЙ
   каталог данных follower'а (установщик отклоняет существующую обычную
   репозиторий/БД назначения — `check_seed_target` в `seed/catalogue.rs`) —
   он создаёт СВЕЖУЮ paused-подписку с новым `seed_receipt` (без маркера);
   4) `Resume` новой подписки; 5) первое чтение после этого успешно
   (автоматического reseed нет). На healthy path стоимость — один
   атомарный load на batch.

`wal_commit` (`buffered|synced`) независим от `replication_serve`:
запросная `durability` может УСИЛИТЬ подтверждение, но никогда не может
ослабить serve-политику репозитория. WAL-less follower apply path
сохраняет durable-floor-before-history ordering независимо от режима.

Явная админ-установка `set_durability` с `wal_commit: synced` на
in-memory/hybrid репозитории (нет persistent WAL) НЕ отвергается:
`synced` игнорируется — тихое понижение до `buffered` через
`for_wal_backend` с одним warn в лог на каждый open; `completed_reseed`
на них разрешён (и обязателен явно при publication), `durable`
разрешён на engine-уровне и проверяется при publication time.

Стоимость (hot path): commit path не изменён; serve path добавляет один
атомарный load policy/mode на каждую replication page read и, при первом
speculative pull после открытия, одну запись Armed в store с flush;
unclean-open epoch bump — это одна запись metadata-ключа + flush на
recovery path, никогда на commit path.

Наблюдаемость: `DESCRIBE REPO` теперь несёт `durability` (effective,
включая `replication_serve`), `durability_override`, `lineage_id` (hex) и
`history_epoch`; gauges `shamir_replication_serve` (effective mode),
`shamir_replication_served_bound`, `shamir_wal_watermark_w` и новый
counter `shamir_repo_history_epoch_bumps_total` (плюс существующие
counters identity-mismatch и новый resync-required).

В текущем exporter все durability series имеют **только labels `db` и `repo`**.
Источники принадлежат конкретному repo и его WAL; clones разделяют одно
состояние. `shamir_wal_effective_commit` сохраняет configured tier
(0 buffered, 1 synced), несмотря на историческое имя: Mem WAL игнорирует
configured Synced и использует effective Buffered без sync demands.
`shamir_replication_serve` (0 durable, 1 completed_reseed — effective mode
репозитория) и
`shamir_wal_backend_mem` (1 для initialized Mem WAL, 0 для
file/uninitialized) семплируются при scrape, без открытия WAL или stores.
`shamir_wal_synced_lag_versions` и `shamir_wal_pending_versions` —
**span W − R**, не количество sparse coverage entries.
`shamir_wal_first_pending_version` — R + 1 при ненулевом span, иначе 0;
`shamir_wal_first_pending_age_ns` — нижняя оценка возраста с первого
наблюдения этой позиции scrape, **не** время allocation/commit. Смена
первой uncovered позиции сбрасывает её observed age.

`shamir_wal_sync_now_calls` считает demands сразу при регистрации, включая
already-covered и cancelled calls. `shamir_wal_sync_rounds` считает
завершённые rounds; `shamir_wal_sync_coalesced` — demands, наблюдавшие
активного worker до своего start (не calls − rounds и не размер очереди).
`shamir_wal_sync_errors` включает failed rounds и unwind/cancellation worker;
`shamir_wal_sync_duration_ns` — cumulative время snapshot replay и sink sync,
не только syscall fsync. Pending demands уже включены в calls.
`shamir_replication_pull_held_back_ns` и
`shamir_replication_pull_held_back_count` — total-ns/count ожиданий R,
включая cancelled waits, **не latency histogram**.

`shamir_wal_watermark_w`, `shamir_wal_recoverable_through` — текущие
наблюдения initialized WAL; `shamir_replication_confirmed_bound` и
`shamir_replication_served_bound` — последний pre-clamp/served sample pull
этого repo. Разные gauges/counters не являются единым атомарным снимком
и не заменяют fencing/coverage certificate. Counters экспортируются через
`absolute`, поэтому повторный scrape не удваивает totals. При reopen или
замене repo с теми же labels source totals сбрасываются, но recorder хранит
старый максимум: меньшие totals скрыты, пока не превысят его (или до restart
recorder). Старые labelled series могут оставаться после удаления repo.

### Валидация при старте

`Config::validate()` проверяет: `kdf_defaults` (и per-listener override) не
ниже floor'ов spec §3.7.2; `argon2_concurrent_max` ∈ 1..=1024;
`auth_init_rate_per_second` ∈ 1..=100000; `listeners` не пуст и каждый
валиден (адрес, `path` для ws, `browser_origin_allowlist` для
browser-профиля, `plain` — только loopback); лимиты курсоров ≥ 1;
`max_inflight_response_bytes` ≥ `max_result_size_bytes`; query limits
не превышают CEILING (нулевые значения — только где имеют смысл). `data_dir`
создаётся, а отсутствующие cert/key генерируются (self-signed) при старте.
Ошибка → процесс не стартует.

`security.query_limits.max_inflight_response_bytes` (RI-15) — если задан,
должен быть `>= max_result_size_bytes`: иначе ни один максимально большой
батч-ответ не смог бы пройти глобальный бюджет памяти. Если бюджет не задан,
он не ограничивает суммарный размер одновременно висящих в памяти ответов;
`max_result_size_bytes` остаётся оценкой резерва для батчей, при
`enforce_result_size: true` также ограничивает накопленные материализованные
байты до commit, и сохраняет жёсткий байтовый cap для страниц курсоров.

Важная деталь для планирования ёмкости: с CR-B2 резервирование из
`max_inflight_response_bytes` происходит ЗАРАНЕЕ (upfront) и ПЕССИМИСТИЧНО —
на основе `max_result_size` батча (обрезанного до серверного cap), а не
реального итогового размера ответа. Поэтому `max_inflight_response_bytes` —
это не только предел памяти, но и ЖЁСТКИЙ лимит числа одновременно
ВЫПОЛНЯЮЩИХСЯ батчей: `effective_concurrency ≈ max_inflight_response_bytes /
max_result_size_bytes` (батч с меньшим `limits.max_result_size` резервирует
меньше и меньше влияет на этот предел — граница считается по ХУДШЕМУ
случаю, а не по типичному). На числах из примера конфига выше: `4 GiB /
100 MiB ≈ 40` — не более 40 одновременных батчей, даже если каждый реальный ответ
занимает всего несколько KiB. Оператору стоит подбирать
`max_inflight_response_bytes` с учётом этого деления, а не только как
бюджет памяти: если нужная конкурентность превышает то, что подсказывает
чистый memory-бюджет, либо увеличивайте `max_inflight_response_bytes`
соответственно, либо просите клиентов задавать меньший per-batch
`limits.max_result_size` (это уменьшает каждое резервирование и повышает
число батчей, которые тот же cap допускает одновременно).

`security.cursors.*` (FG-5b — результирующие курсоры `CreateCursor`/
`FetchNext`/`CancelCursor`):
- `idle_timeout_secs` (default **60**) — сколько секунд курсор может
  простоять без `FetchNext`, прежде чем фоновый reaper его закроет
  (освобождая закреплённый MVCC snapshot). `0` отклоняется при старте —
  нулевой idle-timeout закрыл бы почти любой курсор мгновенно.
- `max_cursors_per_session` (default **16**) — сколько курсоров может быть
  одновременно открыто в рамках одной сессии; `CreateCursor` сверх лимита
  возвращает `cursor_limit_exceeded`. `0` отклоняется при старте — это
  сделало бы `CreateCursor` невозможным вообще.
- `max_cursor_page_size` (default **10000**) — верхняя граница `page_size`
  для `CreateCursor`/`FetchNext`; запрос выше границы отклоняется целиком
  (не обрезается молча). `0` отклоняется при старте по той же причине.

## 3. Observability: `/healthz`, `/readyz`, `/metrics`, `/info`

Отдельный HTTP-сервер (по умолчанию `127.0.0.1:9090`) — **не** на
основном listener'е. Предназначен для K8s probes, Prometheus, мониторинга.

```bash
curl http://127.0.0.1:9090/healthz   # → 200 OK "ok\n"
curl http://127.0.0.1:9090/readyz     # → 200 (ready) или 503 (not yet)
curl http://127.0.0.1:9090/metrics    # → Prometheus text format
curl http://127.0.0.1:9090/info       # → msgpack: uptime, bound_addrs, ready
```

### `/healthz`

Всегда `200 OK "ok\n"`. Тривиальный liveness probe: процесс жив и
реагирует на HTTP.

### `/readyz`

`200 OK` — после того, как все listener'ы привязаны (accept loop
запущен). `503 Service Unavailable` — до готовности. K8s readinessProbe.

### `/metrics`

Prometheus text format. Метрики:

|| Метрика | Тип | Описание |
||---|---|---|
|| `process_cpu_seconds_total` | counter | CPU time |
|| `process_resident_memory_bytes` | gauge | RSS |
|| `process_open_fds` | gauge | Открытые файловые дескрипторы |
|| `auth_attempts_total{result=…}` | counter | Аутентификации (success, bad_proof, locked_out, …) |
|| `shamir_tx_started_total` | counter | Транзакции начатые |
|| `shamir_tx_committed_total` | counter | Транзакции закоммиченные |
|| `shamir_tx_aborted_ssi_total` | counter | SSI-конфликты |
|| `shamir_tx_aborted_expired_total` | counter | Истёкшие tx |
|| `shamir_gc_runs_total` | counter | GC-циклы |
|| `shamir_gc_entries_deleted_total` | counter | GC-удаления |
|| `shamir_visibility_ack_total{mode,result}` | counter | Acknowledgement outcomes: `mode=strict\|bounded`, `result=visible\|pending_timeout\|pending_reopen\|error` (VIS-4c) |
|| `shamir_visibility_ack_wait_seconds_bucket{mode,result,le}` | gauge (cumulative histogram) | Duration of acknowledgements that ACTUALLY waited; the non-blocking fast path is counted only in `shamir_visibility_ack_total` |
|| `shamir_visibility_ack_wait_seconds_count{mode,result}` | gauge | Observed waits per label set |
|| `shamir_visibility_pending_total{reason,kind}` | counter | Pending outcomes: `reason=timeout\|reopen_required`, `kind=explicit_tx\|implicit_batch\|direct_write` |
|| `shamir_visibility_read_wait_timeouts_total` | counter | AsOf reads that timed out waiting for visibility |
|| `shamir_visibility_reconciliation_total{result}` | counter | `WaitForVisibility` outcomes |
|| `shamir_visibility_reconciliation_wait_seconds_bucket{result,le}` | gauge | Reconciliation wait durations |
|| `shamir_visibility_ack_waiters{db,repo}` | gauge | Acknowledgements currently parked (reuses the completion tracker's waiter registration) |
|| `shamir_gate_*` | gauge | REPO-DIAGNOSTICS per-repo gate snapshot (allocation high water, visible floor, oldest blocker, …) |
|| `shamir_wal_size_bytes{db,repo}` | gauge | Приблизительный размер WAL на диске: активный сегмент + запечатанные удержанные (Mem-бэкенд пишет 0) |
|| `shamir_wal_segments{db,repo}` | gauge | Удержанные файлы сегментов WAL: запечатанные + один активный (Mem-бэкенд пишет 0) |
|| `shamir_wal_truncation_lag_versions{db,repo}` | gauge | Отставание truncate: max committed version минус старейшая усекаемая версия |
|| `shamir_group_commit_waiters{db,repo}` | gauge | Участники группового коммита, ожидающие покрытия WAL fsync |
|| `shamir_wal_bytes_total{db,repo}` | counter | Байты полезной нагрузки WAL с момента создания каталога WAL |
|| `shamir_wal_fsync_seconds{db,repo}` | histogram (`_bucket`/`_count`/`_sum`) | Длительность раунда sync-worker: replay снапшота + fsync приёмника (тот же интервал, что `shamir_wal_sync_duration_ns`); `le` и `_sum` — в секундах |
|| `shamir_index_entries{db,repo,table,index}` | gauge | Записи в индексах, снимок за scrape (усечение до 64 таблиц / 32 индексов на репозиторий) |
|| `shamir_index_build_seconds{db,repo,kind}` | histogram (`_bucket`/`_count`/`_sum`) | Длительность завершённой сборки индекса, в секундах |
|| `shamir_index_rebuild_seconds{db,repo,kind}` | histogram (`_bucket`/`_count`/`_sum`) | Длительность завершённого rebuild (doctor/repair, миграция identity), в секундах |
|| `shamir_interner_names{db,repo}` | gauge | Число интернированных имён полей |
|| `shamir_interner_high_water{db,repo}` | gauge | Персистентный high-water mark интернера |
|| `shamir_export_tables_truncated{db,repo}` | gauge (1/0) | Усечение по лимитам кардинальности активно |
|| `shamir_export_tables_known{db,repo}` | gauge | Всего открытых таблиц, известных репозиторию |
|| `shamir_backup_started_total{kind}` | counter | Запущенные online-backup прогоны (`kind=online`) |
|| `shamir_backup_completed_total{kind}` | counter | Успешно завершённые online-backup прогоны (`kind=online`) |
|| `shamir_backup_failed_total{kind,stage}` | counter | Неудачные online-backup прогоны по стадии сбоя (`stage=preparing\|captured\|copying\|sealing\|verifying`) |
|| `shamir_backup_active` | gauge (0/1) | 1, пока online-backup выполняется |
|| `shamir_backup_bytes_copied` | gauge (bytes) | Кумулятивные байты, скопированные завершёнными online-backup прогонами |
|| `shamir_backup_duration_seconds` | gauge (seconds) | Длительность последнего завершённого online-backup прогона (не выставляется до первого завершения) |
|| `shamir_backup_writer_block_seconds{phase}` | gauge (seconds) | Последнее закрытое окно блокировки writer'ов (`phase=capture\|seal`), включая неудачные удержания; не выставляется до первого закрытого окна |
|| `shamir_backup_pin_seconds` | gauge (seconds) | Длительность удержания truncation-pin; не экспортируется, пока не измеряется |
|| `shamir_backup_wal_segments_copied_total` | counter | Скопированные сегменты WAL; не экспортируется, пока протокол не отдаёт счётчик сегментов |

Серии WAL/storage (R4a-2) — снапшоты за scrape, метки только `db`/`repo`
(у bucket-серий дополнительно `le`):

`shamir_wal_size_bytes` — приблизительный размер WAL на диске в байтах
(активный сегмент + запечатанные удержанные; Mem-бэкенд пишет 0). Норма —
ограничена конфигурацией retention; рост сверх неё — проверить удержание
сегментов.

`shamir_wal_segments` — число удержанных файлов сегментов (запечатанные +
один активный), безразмерное. Норма — от единиц до десятков; непрерывный рост
означает, что truncate не успевает за seal.

`shamir_wal_truncation_lag_versions` — отставание truncate в версиях (0, когда
усекать нечего), безразмерное. Норма — небольшая; устойчивый рост — накопился
backlog на truncate, проверить фоновую очистку сегментов.

`shamir_group_commit_waiters` — число участников группового коммита,
приостановленных в ожидании покрытия WAL fsync, безразмерное. Норма — меньше
параллелизма коммитов; устойчивый рост указывает на узкое место fsync.

`shamir_wal_bytes_total` — кумулятивный счётчик байтов полезной нагрузки,
добавленных в WAL с момента создания каталога WAL. Растёт пропорционально
скорости записи; по нему оценивают интенсивность записи (rate()).

`shamir_wal_fsync_seconds` — гистограмма длительности раунда sync-worker
(replay снапшота + fsync приёмника — тот же интервал, что
`shamir_wal_sync_duration_ns`) в секундах: кумулятивные `_bucket` с меткой
`le` (`+Inf` последней), `_count`, `_sum` — сумма в секундах (внутренние
ячейки — наносекунды). Норма на локальном диске — суб-миллисекундная. Серии
не экспортируются, пока WAL не инициализирован (`fsync == None`); «рваный»
снапшот (`torn`) отбрасывается вместо блокировки scrape.

Серии индексов и интернера (R4a-2) — снапшоты за scrape, берутся из уже
открытых таблиц/индексов; закрытые репозитории, таблицы и индексы невидимы —
то же ограничение open-registry, что у `shamir_gate_*` и degraded-index.

`shamir_index_entries{db,repo,table,index}` — gauge числа записей в индексе,
снимается при scrape по живым счётчикам записей. Норма — порядок числа строк
таблицы. Кардинальность ограничена: не более 64 таблиц и 32 индексов на
репозиторий с полными метками; выбор — по текущему числу записей (tie-break —
имя таблицы, детерминированно), остальные агрегируются в одну строку
`table="(other)"` на репозиторий. Лимиты 64/32 — константы, а не настройки.

`shamir_index_build_seconds{db,repo,kind}` — гистограмма длительности
завершённой сборки индекса в секундах, с меткой `kind`; наблюдение один раз на
завершённую сборку (на горячем пути — только чтение часов и observe).

`shamir_index_rebuild_seconds{db,repo,kind}` — та же гистограмма для
завершённого rebuild (doctor/repair и миграция identity), те же метки.

`shamir_interner_names{db,repo}` — gauge числа различных интернированных имён
полей (чтение длины map без итерации). Норма — рост с числом различных имён;
intent для алерта — неограниченный рост без новых полей.

`shamir_interner_high_water{db,repo}` — gauge персистентного high-water mark
интернера; значение монотонно почти не убывает.

`shamir_export_tables_truncated{db,repo}` — gauge 1/0: усечение по лимитам
кардинальности активно (есть таблицы сверх 64/32, попавшие в `(other)`).

`shamir_export_tables_known{db,repo}` — gauge общего числа открытых таблиц,
известных репозиторию; вместе с `_truncated` показывает, сколько осталось за
лимитом (`_known`, не `_total` — суффикс `_total` зарезервирован за counter'ами).

Версии, ожидающие GC, уже экспортируются как `shamir_gate_pending_versions` —
отдельная серия `shamir_gc_pending_versions` не создаётся.

`shamir_query_duration_seconds` — гистограмма длительности операций чтения (`read`), записи (`write`), пакетного запроса (`batch`), commit транзакции (`tx`) и dispatch вызываемой функции (`function`), в секундах, с метками `db`,`repo`,`op`; bucket-серии дополнительно имеют `le`, `_count` — число наблюдений, `_sum` — сумма секунд. Read/write измеряют span leaf-dispatch, batch — весь запрос, tx — span commit, а function — dispatch вызова callable, отдельно от внешнего вызова WASM, учитываемого только на глубине 0. Контейнерные и дочерние spans отражаются разными `op` и не складываются между метками. Типичный диапазон — 1 мс–1 с; метки ограничены открытыми `db`/`repo`, без таблиц, функций, пользователей и кодов. Состояние хранится в движке и опрашивается сервером; рваный снимок гистограммы пропускается, учитываются только открытые репозитории.

`shamir_query_errors_total{db,repo,code}` — counter ошибок запросов, увеличиваемый один раз при отображении ошибки leaf-операции; `code` ограничен статическим allowlist, включая `other`, поэтому пользовательские коды не создают меток. Обычно близок к нулю; всплески указывают на инцидент.

`shamir_query_rows_viewed_total{db,repo}` — кумулятивный counter строк, реально просмотренных движком при выполнении запросов, а не число строк в финальном результате; строки, отброшенные фильтром, вытесненным в поток хранилища, движок не видит и не учитывает. Обновляется без горячих аллокаций; ожидаемо пропорционален объёму сканирования.

`shamir_query_rows_returned_total{db,repo}` — кумулятивный counter строк, фактически возвращённых запросами после обработки; это отдельное значение от просмотренных строк и оно обычно близко к размеру запрошенных страниц.

`shamir_tx_aborted_unique_total` — counter транзакций, прерванных конфликтами уникальности, из существующего `TxMetrics`; обычно редок.

`shamir_tx_aborted_phantom_total` — counter транзакций, прерванных phantom-конфликтами, из существующего `TxMetrics`; обычно редок.

`shamir_tx_materialization_deferred_total` — counter commit-транзакций с отложенной материализацией, из существующего `TxMetrics`; ожидаемо не выше темпа транзакций.

`shamir_tx_post_wal_orphans_total` — counter операций, оставшихся orphan после WAL, из существующего `TxMetrics`; в штатной работе равен нулю.

Серии нагрузки снимаются при каждом запросе `/metrics` с источников текущего запуска сервера. Имена `shamir_inflight_response_bytes_used` и `shamir_inflight_response_bytes_cap` сохранены для совместимости, без алиасов; глобальный опрос бюджета раз в 5 секунд больше не используется.

| Метрика | Тип | Метки | Единицы и смысл | Норма | Что означает отклонение |
|---|---|---|---|---|---|
| `shamir_connections_active` | gauge | — | Число соединений, удерживающих guard `ConnLimiter`, даже при лимите `0`. Включает начальный handshake и HTTP-сокеты с guard, но не listener observability. | `0..limit` при конечном лимите; иначе от `0`, по нагрузке. | Устойчивое приближение к конечному лимиту означает риск отказов при новых подключениях. |
| `shamir_connections_limit` | gauge | — | Соединения: единый глобальный лимит для всех listeners, обслуживающих данные; `0` — без ограничения. | Настроенное значение, постоянное в рамках запуска. | Неожиданное значение, в том числе `0`, — повод проверить конфигурацию. |
| `shamir_connections_rejected_total` | counter | — | Отказы: по одному на неудачную попытку получить guard из-за глобального лимита; отказы по лимиту на IP не входят. Сбрасывается при перезапуске. | Прирост и скорость прироста обычно равны `0`. | Положительный прирост означает, что глобальный лимит уже отсекает подключения. |
| `shamir_inflight_response_bytes_used` | gauge | — | Зарезервированные байты бюджета ответов, не фактический RSS. `0`, если бюджет не ограничен или не отслеживается. | `0..cap` при конечном бюджете. | Близость к `cap` вместе с устойчивым ожиданием указывает на нехватку бюджета. Возможно временное превышение: небольшое при уточнении резерва либо при резервировании сверх `cap` на пустом бюджете. |
| `shamir_inflight_response_bytes_cap` | gauge | — | Байты: настроенный предел бюджета ответов; `-1` — бюджет не ограничен или отсутствует. | Настроенное значение, постоянное в рамках запуска. | Неожиданное значение, в том числе `-1`, — повод проверить конфигурацию. |
| `shamir_response_budget_waiters` | gauge | — | Число одновременных попыток резервирования, перешедших на путь ожидания. Доступность бюджета проверяется повторно; порядок не FIFO. | `0`. | Устойчивое ненулевое значение означает, что запросы регулярно ждут освобождения бюджета. |
| `shamir_post_auth_rate_limit_rejected_total` | counter | — | Запросы нативного протокола и токенных REST/WS, отклонённые посессионным лимитом частоты после аутентификации. Не включает отказы на входном gate, при аутентификации или по scope/политике. | Прирост и скорость прироста обычно равны `0`. | Положительный прирост означает, что запросы упираются в посессионный лимит. |

`shamir_sessions_active{db}` не экспортируется: на доступном пути наблюдения сессия не привязана к БД. Глобальная `shamir_active_sessions` не изменилась. `shamir_admission_queue_depth` нет, поскольку общей очереди допуска нет. Серии `shamir_argon2_{in_flight,waiters,capacity}` не экспортируются: в рабочем коде никто не берёт разрешения у семафора, созданного при запуске. В SCRAM клиент выполняет KDF, сервер проверяет доказательство без этого семафора; отдельно ограничиваемая SQL-функция хеширования к нему не относится.

Отказы native auth-init в синхронном handshake уже учитывает `auth_attempts_total{result="rate_limited"}`, а отказы входного токенного gate по частоте — `shamir_token_auth_total{channel,result="rate_limited"}`; дублирующих счётчиков нет. `shamir_token_request_denied_total{endpoint}` учитывает отказы по scope/политике, а не посессионный лимит частоты после аутентификации.

`process_*` опрашиваются каждые 5 секунд (crate `metrics-process`).

Серии online backup `shamir_backup_*` (R2a, дизайн `online-backup.md` §4.8) —
счётчики несут метку `kind=online` (offline-прогоны, если появятся, получат
`kind=offline`); `shamir_backup_failed_total` дополнительно — `stage`, стадию,
на которой протокол зафиксировал сбой (`preparing|captured|copying|sealing|verifying`).

`shamir_backup_started_total` / `shamir_backup_completed_total` /
`shamir_backup_failed_total{kind,stage}` — кумулятивные счётчики прогонов за
время жизни процесса. В норме completed растёт вместе с started, а failed
остаётся нулевым; рост failed по конкретной стадии указывает, где деградирует
backup (например, `stage=sealing` — не успевает seal-фаза).

`shamir_backup_active` — gauge 0/1: выполняется ли online-backup прямо сейчас.
Одновременный допускается только один (одна блокировка на сервер); `1` дольше
ожидаемого — признак зависшего прогона.

`shamir_backup_bytes_copied` — кумулятивные байты, скопированные завершёнными
прогонами (счётчики хешированных байтов протокола); по rate() оценивают
скорость копирования.

`shamir_backup_duration_seconds` — длительность последнего завершённого
прогона в секундах; не выставляется до первого завершения. Сравнивают между
себя последовательные значения, а не с абсолютной нормой.

`shamir_backup_writer_block_seconds{phase=capture|seal}` — gauge в секундах,
содержит последнее закрытое окно блокировки writer'ов по фазе, включая
неудачные удержания; не выставляется до первого закрытого окна.
`shamir_backup_pin_seconds` и `shamir_backup_wal_segments_copied_total`
по-прежнему **не экспортируются**: протокол не отдаёт удержание pin и счётчик
сегментов WAL (серии появятся без изменения имён, как только появятся данные).

`shamir_function_duration_seconds` — гистограмма длительности вызовов WASM в секундах, с метками `db`,`repo` (без метки функции); серии `*_bucket` имеют дополнительную метку `le`, это кумулятивные gauge, `*_count` — counter, `*_sum` — gauge в секундах. Учитывается только внешний вызов глубины 0, включая вложенное время, ошибки и отмену; обычно — микросекунды–миллисекунды. Снимки берутся при scrape без блокировок и I/O экспортера; реестр ограничен 64 именованными парами с агрегатом `(other)`.

`shamir_function_fuel_consumed_total` — counter фактически израсходованных единиц fuel с метками `db`,`repo`; суммируется расход при приостановке и окончательном расчёте, без повторного учёта вложенных вызовов и без зарезервированного fuel. Норма зависит от числа и сложности вызовов.

`shamir_compile_cache_hits_total` — counter успешных обращений к инкрементальному compile-cache, без меток; считает единицы компиляции, а не вызовы функций. После прогрева обычно преобладают попадания.

`shamir_compile_cache_misses_total` — counter промахов кэша и компиляций без кэша, без меток; считается по единицам компиляции, не по вызовам функций. После прогрева обычно близок к нулю.

`shamir_compile_cache_bytes` — gauge размера индексированных живых кэшей компиляции в байтах, без меток; обычное значение укладывается в бюджет кэша.

`shamir_compile_cache_entries` — gauge индексированных записей кэша, без меток; запись инкрементальной компиляции не обязательно соответствует целому модулю. Обычно значение укладывается в бюджет кэша.

`shamir_module_admission_rejected_total{reason}` — counter отклонённых модулей при admission, с одной из причин `parse`,`validation`,`import`,`operator`,`abi`,`interface`,`limits`; обычное значение — 0.

`shamir_function_call_depth_high_water` — gauge максимальной глубины стека вызовов в кадрах, с метками `db`,`repo`; глубина контекста начинается с 0, значение монотонно растёт. Норма — ниже настроенного предела; действует реестр на 64 именованные пары и агрегат `(other)`.

`shamir_replication_lag_versions` — gauge текущего отставания в версиях, с метками `db`,`repo`. Оно равно текущей версии лидера минус durable bookmark, сообщённый follower'ом; для `from_version` bookmark — это `from_version - 1`. Отрицательное значение ограничивается нулём.

Лидер учитывает bookmark из каждого Pull-запроса в пределах времени жизни этого запроса, включая запросы, завершившиеся успешно. Отставание считается от подтверждённого durable bookmark, а не от последнего отданного события: ещё не подтверждённое событие не уменьшает lag.

После завершения Pull-запроса его лидерский поток помечается `stopped`. Перекрывающиеся запросы остаются отдельными активными потоками; завершение одного прекращает только его собственную запись, и агрегированное состояние остаётся активным, пока существует другой запрос.

`shamir_replication_lag_seconds` — gauge возраста последнего наблюдённого durable receipt, в секундах, с метками `db`,`repo`; значение равно 0 до первого receipt. У follower время фиксируется при фактическом применении receipt.

У лидера время фиксируется при наблюдаемом продвижении сообщённого durable bookmark. Это время наблюдения лидером, а не точное время удалённого commit; оно может быть позже фактического commit.

Возраст receipt продолжает расти при здоровом простое без новых bookmark/receipt. Поэтому сам по себе большой `lag_seconds` во время отсутствия новых записей не доказывает задержку репликации.

`shamir_replication_stream_state{state}` — one-hot gauge состояния с метками `db`,`repo`,`state`; `state` — одно из `starting`,`streaming`,`backing_off`,`stopped`. Состояние учитывает как follower subscriptions, так и активные лидерские Pull-запросы.

Роли не имеют отдельной метки: для каждой пары `db`,`repo` `lag_versions` и состояние агрегируются только по активным ролям; `lag_versions` выбирает максимальный текущий lag. Время receipt — самый старый ненулевой timestamp среди активных ролей и сохранённой истории подтверждений именованного leader, даже если сейчас активен только follower. Если активных ролей нет, lag и состояние берутся из последней наблюдавшейся роли; завершённый leader lag не сохраняется, но история подтверждений именованного leader сохраняет timestamp receipt.

Общее состояние пары также детерминированно агрегируется: приоритет от худшего к лучшему — `backing_off` > `starting` > `streaming` > `stopped`; экспортируется одно соответствующее one-hot состояние. Поэтому активный проблемный поток не скрывается здоровым потоком той же пары.

Restart follower'а отслеживается маркером конкретной subscription. Reconnect counter увеличивается только при фактическом пересоздании источника на границе supervisor factory; первоначальное создание даёт 0, а повторы запросов без пересоздания источника не считаются.

`shamir_replication_reconnects_total` — lifetime counter пересозданий источника, с метками `db`,`repo`; это не счётчик сетевых retry. Метрики не добавляют метку роли: обе роли используют те же пары `db`,`repo` и существующие метки состояния.

Нормальный диапазон `shamir_replication_reconnects_total` — 0 при стабильной работе; при наличии записей lag обычно мал. После потери состояния lag восстанавливается при следующем наблюдении durable progress.

До 256 пар `db`,`repo` хранятся поимённо. Значения для превышающих лимит потоков агрегируются через слабо удерживаемые handles: пока есть активные handles, lag равен максимальному текущему lag среди них, не lifetime high-water; когда lag падает с 10 до 0, агрегат тоже падает до 0. Overflow-leader Pull учитывается только в `lag_versions` и не сохраняет receipt timestamp; поэтому без receipt от overflow follower `lag_seconds` равен 0. Если активных handles нет, overflow-агрегат выдаёт нулевые значения.

Остановленные overflow-потоки исключаются из lag-агрегации. После завершения всех overflow-потоков строка `(other)` остаётся с нулевыми значениями gauges; lifetime reconnect counter при этом сохраняется и продолжает экспортироваться. Новых серий или меток для этой агрегации нет.

### `/info`

```msgpack
{
  "uptime_seconds": 3600,
  "bound_addrs": ["0.0.0.0:7331", "0.0.0.0:7332"],
  "ready": true
}
```

### Безопасность observability

Loopback-only по умолчанию. Для non-loopback — нужно явно разрешить
`allow_public_metrics: true` в блоке `observability` (не рекомендуется).
Экспозиция метрик на public network — M-tier audit event (M5).

Пустой `addr: ""` отключает observability-сервер полностью.

## 4. Логирование

Два режима:

|| Режим | Когда | Как работает |
||---|---|---|
|| Stdout | `logging.file` не указан | Non-blocking tracing_appender, lossy on overflow |
|| Batched file | `logging.file: "/var/log/shamir-db/server.log"` | MPSC → single worker → BufWriter, flush каждые `flush_interval_ms` (default 2000) или при burst ≥ 256 KB |

### Slow query logging

```ktav
logging: {
    level: info
    slow_query_threshold_ms: 1000
}
```

Батч, выполняющийся дольше `slow_query_threshold_ms`, логируется на
уровне WARN с деталями (batch id, execution_time_us, num_queries).

### Namespace masks (lock-free)

13 неймспейсов с индивидуальными уровнями: `shomer`, `wal`, `tx`,
`storage`, `engine`, `query`, `vector`, `fts`, `fn`, `auth`, `wire`,
`server`, `migration`. Hot-path `enabled()` — одно atomic load +
longest-prefix match. Нет mutex/RwLock.

### Live reload (SIGHUP)

На unix: `kill -HUP <pid>` → сервер перечитывает конфиг → lock-free mask
swap. Без рестарта. Windows: restart required.

## 5. Rate limiting

Token-bucket per `/24` IPv4 или `/64` IPv6 subnet:

```ktav
security: {
    auth_init_rate_per_second: 10
}
```

* Default: 10 tokens/sec per subnet.
* Warmup: ÷4 в первые 60 секунд после boot (spec §8.6).
* GC: idle buckets удаляются через 5 минут.
* Persistence: `RateLimitSnapshotSink` — durable rehydration при restart.

## 6. Connection limiter

Глобальный лимит одновременных соединений:

```ktav
security: {
    connection: {
        max_active_connections: 10000
    }
}
```

Atomic counter + RAII guard. Новое соединение сверх лимита —
отклоняется. 0 = без лимита.

### Slow-loris защита

```ktav
security: {
    connection: {
        auth_init_timeout_ms: 5000
    }
}
```

Если клиент не отправил `auth_init` за 5000 ms — соединение закрывается.

## 7. Audit log

```ktav
audit: {
    max_file_size_mb: 100
    retention_days: 30
}
```

Audit-лог в audit-line-формате с HMAC-chain (каждая запись включает HMAC от предыдущей).
Ротация по размеру; устаревшие файлы удаляются автоматически.
Покрытие (состояние реализации): в durable HMAC-chained audit-лог сегодня попадают **только события аутентификации** (успех/сбой/прерывание рукопожатия — это единственный append call site, `crates/server/shamir-server/src/connection/handshake.rs`). DDL, ACL-изменения (chmod/chown) и admin-операции (`CreateScramUser`, `SetSuperuser`, retention/purge, drop и т.п.) durable-следа **не оставляют** — для них существует только эфемерный вывод `log`/`tracing`, который не персистируется и не является enforcement-gate (см. `shamir-types::access::trace_access`). Расширение покрытия (мост `AuditSink` → `AuditChainWriter` + append call sites на DDL/ACL/admin-операциях) — запланированное улучшение, пока не реализовано (P1).

## 8. Backup

Есть два режима резервного копирования:

- **Online-копия** — на **работающем** сервере: вручную через `StartBackup` /
  `BackupStatus` / `CancelBackup` или встроенным планировщиком. Сервер
  продолжает принимать записи, снимок пишется в заданный каталог.
- **Offline `backup --to`** — stop-and-copy: сервер должен быть **остановлен**
  перед запуском. Против живого источника команда теперь **отказывает** с
  адресной ошибкой, указывающей на online-операцию; force-флага нет.

Offline-режим:

```bash
# 1) Остановить сервер (systemctl stop shamir-server / SIGTERM — дождаться graceful shutdown)
# 2) Запустить backup — data_dir берётся из --config, снимок пишется в <to>/<UTC-timestamp>/
shamir-server --config db.ktav backup --to /backup/shamir
# 3) Перезапустить сервер
```

Offline-команда рекурсивно копирует `data_dir` (хранилище, WAL, реестры
таблиц, audit-лог и его ротации, TLS PEM) в каталог
`<to>/YYYYMMDD_HHMMSS/`, исключая производные данные (`cache/`); существующий целевой каталог перезаписываться
отказывается. Источник (`data_dir`) задаётся файлом `--config`, отдельного
флага источника нет; реальная точка входа —
`crates/server/shamir-server/src/main.rs` (`Subcmd::Backup { to }`).

### Online-копия: когда и как

Используйте online-копию, когда сервер нельзя останавливать; offline
`backup --to` — когда остановка допустима и нужна максимально простая
гарантия (полная остановка writer'ов).

Условия и ограничения:

- Операция требует прав **superuser**; follower (read-only replica) отказывает
  в `StartBackup` и `CancelBackup` со стабильным кодом
  `online_backup_follower_read_only`; `BackupStatus` на follower разрешён.
- **Одна копия за раз** на сервер (in-process permit; второй `StartBackup`
  получает отказ).
- Операция node-local: копируется локальный `data_dir` этого узла.

Процедура через типизированный клиент и builder (только builder, без сырого
JSON; на каждую операцию — свежий `Batch` и `batch.build()`):

```rust
use shamir_client::builder::batch::Batch;
use shamir_client::builder::ddl::{backup_status, cancel_backup, start_backup};

// 1) Запуск копии в каталог назначения:
let mut batch = Batch::new();
batch.op("start-bk", start_backup("/backups/snap-1"));
let resp = client.execute("default", batch.build()).await?;

// 2) Статус (в т.ч. на follower) — опрашивать до стадии Done,
//    прежде чем использовать снимок:
let mut batch = Batch::new();
batch.op("bk-status", backup_status());
let resp = client.execute("default", batch.build()).await?;

// 3) Отмена выполняющейся копии (кооперативная: проверяется в точках
//    протокола и между файлами копирующего прохода — между файлами
//    копирование останавливается сразу, стадия не «донашивается»):
let mut batch = Batch::new();
batch.op("bk-cancel", cancel_backup());
let resp = client.execute("default", batch.build()).await?;
```

Статус — приблизительный: стадия меняется по мере продвижения, `dest_dir`
известен только после завершения прогона и только при успехе. Прогон
обычно завершается терминальной стадией: `Done` — при успешной
заключительной проверке, иначе `Failed`; исключение — отказ создания
timestamped-каталога (см. ниже). Ориентируйтесь на `running` и
`last_error`, а не только на стадию. При отказе в статусе доступен
отдельный необязательный `progress.failed_stage` (не во всех случаях —
см. ниже) и `last_error`.

### Кооперативная отмена, живой статус и границы снимка

**Точки проверки отмены.** `CancelBackup` кооперативный: флаг отмены
проверяется в фиксированных точках протокола — в начале подготовки (шаг A),
перед барьером захвата (шаг B), перед началом копирования (шаг C), перед
началом уплотнения (шаг E, seal) и перед началом верификации и записи
манифеста (шаг F), а также **между файлами** внутри каждого копирующего
прохода (отмена, замеченная между файлами, — это отмена, а не сбой).
Стадия при этом **не донашивается**: отмена между файлами выходит из
копирования сразу, ни текущий проход, ни копия дальше не продвигаются;
на пути отмены каталог
назначения удаляется, pins/seals снимаются. После начала заключительной
верификации новых проверок нет: отмена, запрошенная в этот момент, прогон
не остановит — он завершится со стадией `Done`, но только при успешной
проверке (при её сбое — `Failed(Verifying)`), а `cancel_requested` в
статусе просто останется выставленным.

**Ранняя ошибка — не один случай.** Отказы до тела протокола различаются
по тому, что при них публикует реестр:

- `StartBackup` валидирует каталог назначения **до** регистрации в реестре
  (`registry.rs::validate_destination`): невалидный каталог — просто ошибка
  вызова, запись реестра не создаётся и не меняется.
- Протокол атомарно создаёт каталог `<to>/<timestamp>/`. При отказе
  создания реестр публикует `running = false`, стадию `Failed`,
  `progress.failed_stage = Preparing` и `last_error`.
- Если поток прогона не удалось запустить, вызов возвращает ошибку, а реестр
  публикует `running = false`, стадию `Failed`, `progress.failed_stage = Preparing`
  и `last_error`. Следующий прогон может получить допуск.

**Живой статус.** Пока прогон выполняется, стадия — это подсказки адаптера
на границах обращений к движку: видны только `Preparing` (включая фазу
копирования), `Sealing` и `Verifying`; живых `Captured` и `Copying` не
бывает. Нормальные терминальные стадии — `Done` и `Failed`. Per-repo
счётчики хеширования: во время `running` публикуемый `per_repo` пуст
(локальные нулевые учётные записи шага B в статус не попадают —
`registry.rs:173-177`, `212-227`, `305-322`); реальные байты/файлы по
репозиториям появляются только после завершения прогона — при успехе
владелец заполняет их из манифеста снимка, а `running`-запись может
уже включать финальное подведение итогов.

**Поздние репозитории.** Различайте два случая. Репозиторий, созданный
после захвата каталога (шаг A), не попадает в captured-набор: его файлы и
строка каталога ещё могут быть скопированы в снимок, но это не делает его
частью набора; не зарегистрированный после restore — другой случай. При
restore он именуется в отчёте только если его строка каталога оказалась
в staged-каталоге snapshot-копии: тогда строка
durable-удаляется из staged-каталога и репозиторий попадает в список
исключённых. Если строки в копии нет, репозиторий в снимке и в отчёте
просто отсутствует.

**Lock-файлы в снимке.** Online-снимок не содержит `.shamir.lock`
(maintenance-lock сервера) и вложенных Fjall-артефактов `lock` — это
runtime-артефакты, копирующий движок пропускает их по имени на всех ОС,
чтобы манифест не зависел от платформы. Обычный старт сервера
пересоздаёт `.shamir.lock` (`create(true)` + эксклюзивный fs4-lock).
Fjall свои `lock` **сам** не пересоздаёт: артефакт восстанавливает
staged-restore (`restore/online.rs` через `restore/fjall_locks.rs`) в
каталогах, опознанных как Fjall-хранилища. Offline `backup --to` своего
поведения не менял и артефакты lock по-прежнему копирует; остальные
исключения (`cache/`) без изменений.

### Точка консистентности манифеста

`manifest.json` online-копии содержит `kind = "online"`, `format_epoch`,
`binary_version` и на **каждый репозиторий** — `wal_position` (точка
консистентности, `V*`). Restore гарантированно содержит все записи,
**подтверждённые до** захвата точки; записи, подтверждённые **после** захвата,
в копию могут не попасть — это НЕ обещается.

**Поведение writer'ов при копировании.** Записи продолжаются во время
первого прохода копирования. При seal приём новых коммитов приостанавливается
по репозиторию до конца повторного копирования: внутри этой паузы снимается
seal-образец и повторно копируются WAL и хранилище; gate снова открывается
только после того, как безопасный шаг завершился и guard повторного
копирования снят.
Бюджет по умолчанию один: 8 с монотонно на репозиторий (включая барьер).
Байтового лимита по умолчанию в сервере НЕТ: повторное копирование
переписывает изменившиеся файлы целиком, а не только дельту, поэтому размер
изменяемого активного WAL/journal/LSM-хранилища — не граница задержки
записи. (Протокол синхронизации сохраняет опциональный явный байтовый лимит,
кумулятивный и включая повторы, но это не конфигурационный параметр для
оператора, пока код его не выставляет.)
Часы кооперативные — проверяются на границах шагов барьера и при повторном
копировании между файлами, порциями ввода-вывода/чтением/хешированием и в
паузах повторов, поэтому незавершённая запись/системный вызов прервана быть
не может и может превысить границу. Превышение — типизированный
`Failed(Sealing)` с очисткой. Writer ждёт, а после существующих 15 с может
получить типизированный повторяемый отказ; запись можно повторить.

### Verify

Отдельной CLI-подкоманды `verify` в `main.rs` нет; проверка снимка — API
`shamir_server::backup::verify_manifest(<каталог снимка>)`. Она проверяет
**файлы**: контрольные суммы по манифесту, наличие всех перечисленных
файлов, отказ от незаявленных файлов; сверяет эпоху манифеста со штампом и
перечисляет WAL-последовательности. Тело WAL она **не** валидирует — это
делает restore (replay). `binary_version` из метаданных — информационное
поле, не условие отказа.

### Restore online-копии

Restore — offline-действие на **остановленном** целевом сервере. Для
`kind = "online"` перед подменой `data_dir` выполняется replay WAL каждой
репозитории из `consistency.per_repo` до её `wal_position`. Fail-closed
гарантия относится к валидациям **до подмены** `data_dir`: любой отказ до
swap (verify, replay, проверки консистентности) оставляет живой `data_dir`
нетронутым. Репозиторий, созданный **после** захвата, не входит в
захваченный набор: его каталог может физически оказаться в снимке, но в
restore он не регистрируется и не восстанавливается — когда и с какой
меткой он появляется в отчёте, см. выше («Поздние репозитории»).

**Содержимое хранилища и WAL выше точки `V*` — fail-closed отказ.** Strict
replay сверяет версии с точкой `V*` **до** применения какой-либо записи:
WAL-версия выше точки даёт типизированный отказ `EntryAboveVStar`, версия
открытого хранилища выше точки — отказ `Recovery` поверх `Validation`, —
не трогая целевой `data_dir` и не усекая излишек. Хранилище на уровне точки
или ниже — не повреждение: это допустимо, только если сохранённый WAL
покрывает его до точки, и replay доводит состояние до `V*`. Ручная подмена
файлов, дающая версии выше точки, всё же приводит к отказу, а не к приёму
сомнительных данных.

**Windows:** копирующий движок выполняет ограниченное число повторов при
sharing violation (os error 32/33); при исчерпании повторов копия завершается
отказом, а не портит данные.

Метрики online backup — экспортируемые сегодня серии `shamir_backup_*`
(`kind=online`): счётчики
`shamir_backup_started_total`, `shamir_backup_completed_total`,
`shamir_backup_failed_total{kind,stage}` (стадии сбоя
`preparing|captured|copying|sealing|verifying`), gauges
`shamir_backup_active` (0/1), `shamir_backup_bytes_copied`
(кумулятивные байты завершённых прогонов), `shamir_backup_duration_seconds`
(длительность последнего **успешного** прогона) и
`shamir_backup_writer_block_seconds{phase=capture|seal}` — последнее закрытое
окно блокировки writer'ов по фазе, включая неудачные удержания; не выставляется
до первого закрытого окна. Зарезервированные серии `shamir_backup_pin_seconds`
и `shamir_backup_wal_segments_copied_total` **пока не экспортируются**.

Хранилище записывает в отчёт метки исключённых репозиториев; агрегированный
online-статус — только по новым репозиториям.

### Расписание и хранение копий

Без блока `backup` встроенное расписание выключено. Пример блока верхнего
уровня в конфиге сервера:

```ktav
backup: {
    destination: /backup/shamir-scheduled
    interval: 24h
    run_on_start: true
    retention: {
        keep_last: 7
    }
}
```

- `destination`: заранее создайте отдельный приватный абсолютный каталог,
  не ссылку и не reparse point. Канонический путь не должен совпадать с
  `data_dir`, лежать внутри него или быть его предком. Сервер не создаёт
  этот корень и не исправляет права; перед использованием проверяет путь
  и приватность заново.
- `interval`: целое положительное число ASCII-цифрами с суффиксом `s`, `m`,
  `h` или `d` (`d` = 24 часа), без пробелов, дробей и составных значений.
  Минимум 60 секунд; переполнение и некорректные значения отклоняются при старте.
- `window` (например, `window: 23:00-04:00`) по умолчанию отсутствует. Это
  локальное время сервера, начало включено, конец исключён; одинаковые границы
  запрещены. Окно ограничивает **запуск**, не завершение. При переходе часов
  повторённый час может принять два разных тика, пропущенный — ни одного.
- `run_on_start` по умолчанию `false`: первый тик через интервал после
  готовности сервера. При `true` первый тик сразу после готовности, но окно
  и занятость по-прежнему проверяются.
- `retention.keep_last` по умолчанию 7, минимум 1. Необязательный
  `retention.max_age_days` — положительное целое число 24-часовых суток.

Тики идут по монотонным часам; расчёт прошедшего времени ограничен снизу
нулём. При позднем пробуждении рассматривается не более одного тика;
пропущенные тики не догоняются. При остановке новых
запусков нет; затем проверяется окно (`skipped_window`), затем занятость
(`skipped_busy`). Ручные и плановые копии используют один реестр и один
допуск, удерживаемый планировщиком до завершения обработки хранения. Очереди
и немедленных повторов нет. Планировщик работает локально от имени системы,
без сессии; ограничения superuser/follower для ручных операций не меняются.
При штатной остановке сервер запрашивает кооперативную отмену и ждёт
текущую операцию; выполняющийся системный вызов не прерывается. Новая
очистка при остановке не начинается. Общий 30-секундный тайм-аут `serve`
может отметить остановку как нештатную до завершения операции; принудительный
выход может оставить недописанную или частично удалённую копию.

Очистка запускается только после успешной плановой копии. Она рассматривает
только непосредственные каталоги с именем `YYYYMMDD_HHMMSS` (UTC), меткой
владельца этого источника и проверенным манифестом v2 online. Порядок удаления
— от старых к новым по времени из манифеста, при равенстве — по имени каталога.
Новейшие `keep_last` и только что созданная копия защищены даже при откате
часов. `max_age_days` не снимает защиту и не добавляет удалений сверх уже
допустимых по `keep_last`.

Чужие, ручные, немаркированные, частичные и непроверяемые копии, посторонние
файлы и небезопасные деревья не удаляются. Если не удалось пометить или
проверить новую копию, завершить обход либо уложиться в лимиты проверки,
удалений нет. Ошибки очистки записываются в журнал и метрики отдельно от
исхода копирования. Метка владельца зависит от постоянного случайного ID в
`server_meta` и канонического пути `data_dir`, а не от публичной идентичности
или ключей сервера. Ротация ключей не меняет метку; перенос или клонирование
на другой канонический путь меняет её, и старые копии не удаляются по новой
метке. Для клона и ручных копий используйте отдельные каталоги.
Не редактируйте содержимое управляемого корня, не делите его с другим
писателем и ничего не монтируйте внутри: Unix-проверка устройства не
обнаруживает bind mount на том же устройстве.

Дополнительные серии `/metrics` (кроме `result` у счётчика запусков,
собственных меток у них нет):

| Метрика | Что показывает |
|---|---|
| `shamir_backup_last_success_timestamp_seconds` | Время последней успешной online-копии (ручной или плановой), Unix-секунды; 0 до первого успеха после запуска. |
| `shamir_backup_last_duration_seconds`, `shamir_backup_last_bytes` | Секунды и байты данных по манифесту последней успешной копии; 0 до успеха. |
| `shamir_backup_scheduled_runs_total{result=…}` | `started`, `skipped_busy`, `skipped_window`, `failed`, `completed`; исход копии не включает очистку. |
| `shamir_backup_scheduled_failure_streak`, `shamir_backup_schedule_enabled` | Серия сбоев подряд и признак включения (0/1). Плановый успех сбрасывает серию, пропуски и ручные копии её не меняют. |
| `shamir_backup_schedule_started_timestamp_seconds` | Время запуска расписания в секундах Unix; 0 до включения. |
| `shamir_backup_retention_deleted_total`, `shamir_backup_retention_failed_total`, `shamir_backup_retention_untouched` | Счётчики удалений и ошибок, число оставленных чужих или непроверяемых элементов по последнему полному обходу. |

**Prometheus.** Настройте сбор `/metrics` каждого узла, установите
`deploy/alerts/shamir-backup.rules.yml` на сервер Prometheus и добавьте его путь
в `rule_files`. Проверьте итоговый файл через `promtool check rules`,
перезагрузите конфигурацию Prometheus и проверьте доставку в Alertmanager.

- **BackupStale:** при включённом расписании отсчёт ведётся от последней
  успешной копии, а до первого успеха — от запуска расписания. Сигнал
  срабатывает, когда возраст превышает один и тот же порог непрерывно в
  течение `for: 5m`. `BACKUP_STALE_THRESHOLD_SECONDS` — параметр подстановки
  при развёртывании, не переменная Prometheus: замените литерал `129600` секунд
  (1,5 × 24 ч) на 1,5 × интервал в секундах, увеличив порог при необходимости
  с учётом времени копирования, допустимого окна и плановых простоев. Для
  разных расписаний ограничьте варианты правила соответствующими целями сбора.
  Ручная успешная копия сбрасывает устаревание.
- **BackupFailing:** расписание включено, серия плановых сбоев и счётчик
  плановых исходов `failed` оба не меньше 3. Именно серия определяет, что
  сбои шли подряд.
- **BackupRetentionFailing:** счётчик ошибок очистки вырос за последний час;
  сама копия при этом могла быть успешной.

**Если копии перестали появляться**, сначала проверьте доступность цели
сбора и `schedule_enabled`, затем окно, часовой пояс, счётчики пропусков,
занятость ручной копией и журнал сервера. При сбоях проверьте свободное место,
приватность и доступность корня; при ошибках очистки дополнительно проверьте
привязку владельца, целостность и лимиты обхода. Не удаляйте необследованные
каталоги автоматически. Для проверки восстановления вынесите полный
завершённый снимок из управляемого корня, проверьте манифест и выполните
обычный offline `restore --from` на остановленном целевом сервере. Для
доставки в объектное хранилище используйте свои инструменты и копируйте
полный каталог наружу до его удаления по retention; обратно в управляемый
корень ничего не записывайте.

### Шифрование копий своими средствами

Снимок — непрозрачный каталог с `manifest.json`. Шифруйте/расшифровывайте
**весь каталог** средствами вне сервера (например, `age`/`gpg` по
tar-архиву каталога); никогда не редактируйте файлы внутри снимка и
расшифровывайте каталог до `verify`/`restore`. Встроенное шифрование копий
(R2c) отменено; за шифрование отвечает оператор.

### Границы

- **Point-in-time recovery нет:** восстановление возможно только к точке
  консистентности снимка.
- Внешние cron / systemd timer остаются вариантом. Online-копию можно
  запускать внешним планировщиком без остановки сервера; остановка
  нужна только offline-CLI-команде `backup --to`. Пример обёртки для systemd: один `.service` с
  `ExecStartPre=systemctl stop shamir-server`,
  `ExecStart=shamir-server ... backup --to ...`,
  `ExecStopPost=systemctl start shamir-server`; либо wrapper-скрипт
  `stop → backup → restart`. Наивный cron, запускающий `backup --to` против
  ещё живого сервера, получит отказ — так и задумано.

**Restore.** `shamir-server --config db.ktav restore --from <to>/<YYYYMMDD_HHMMSS>`
— только при остановленном сервере (проверяется блокировка `data_dir`;
`--force` её пропускает). Команда сверяет `manifest.json` (sha256 каждого
файла), атомарно подменяет `data_dir` (прежний сохраняется как
`.pre_restore_backup_<timestamp>`) и инвалидирует все resumption-тикеты.
Счётчики lockout не сбрасываются. Не восстанавливай простым `cp -r`: так
пропускаются проверка манифеста и инвалидация тикетов.

**Проверка целостности.** `shamir-server --config db.ktav doctor [--apply]`
(offline) — диагностика индексов и счётчиков, с `--apply` — перестройка.

### Durability (fsync) гарантии backup/restore (F-12, #802)

Backup и restore теперь явно fsync'ируют каждый скопированный файл, manifest и
каталог после каждого atomic rename, чтобы данные реально достигали stable
storage до возвращения успеха (а не оставались в OS page cache):

- **`copy_dir_recursive`** (общий для `backup()` и `restore()`'s step 3):
  после `fs::copy` файл переоткрывается на запись и вызывается `sync_all()`.
  Ошибка файлового sync'а **пропагируется** как `BackupError::Io` /
  `RestoreError::Io` — корректность содержимого файла IS the point (по
  прецеденту `segment_meta.rs`, где файловый `sync_all` тоже жёстко
  протагируется).
- **`write_manifest`**: `fs::write` заменён на `File::create` + `write_all` +
  `sync_all` — manifest durable до того, как `backup()` вернёт успех.
- **`restore()` step 5**: после КАЖДОГО успешного `fs::rename` (оба
  swap-rename, rollback-rename и single fresh-target rename) вызывается
  `fsync_dir(parent)` — fsync содержащего каталога, чтобы directory-entry
  update самого rename'а был durable. Это закрывает классический gap «you
  fsync'd the file but not the directory» на ext4/xfs. Ошибка каталогового
  fsync **только логируется**, не пропагируется (по прецеденту
  `wal_segment.rs::fsync_parent_dir` — деградация power-loss window, но не
  corruption); `RestoreError` не получил нового варианта.

**Windows / non-unix caveat:** `fsync_dir` — documented no-op (этот workspace
уже решил в `wal_segment.rs`'s `#[cfg(not(unix))]` stub, что Windows не требует
этой конкретной directory-entry durability guarantee). Истинное power-loss
crash-тестирование непортативно и не покрывается unit-тестом; регрессионный
guard — interrupted-copy тест в `tests/restore_tests.rs`, проверяющий ближайший
портативный прокси. См. `KNOWN_LIMITATIONS.md` §9.

### Штамп формата и обновление версии (format epoch)

В корне `data_dir` сервер хранит `format.stamp`: эпоху формата данных, сведения о создателе штампа и версию последнего открывавшего каталог бинарника. Штамп создаётся автоматически при первом старте; пустой каталог или каталог без штампа может быть «усыновлён» базовой эпохой с WARN в логе. До открытия хранилищ сервер сверяет эпоху: несовместимая версия не откроет данные молча.

**Процедура обновления версии.** Сервер должен оставаться остановленным до завершения format migration:

1. Останови сервер и дождись завершения процесса.
2. До любого вызова `format-migrate` сними backup:
   `shamir-server --config db.ktav backup --to /backups`.
   Команда создаст каталог снимка с фактической UTC-меткой, например
   `/backups/20261116_143000/`. Используй именно выведенный путь-корень
   снимка (каталог, содержащий `manifest.json`), а не `/backups` и не
   предполагаемое время. Backup содержит штамп; `cache/` в него не входит.
3. Установи новый бинарник, но пока не запускай сервер.
4. Необязательный просмотр плана:
   `shamir-server --config db.ktav format-migrate --from-backup /backups/YYYYMMDD_HHMMSS --plan`.
   Без `--yes` команда по умолчанию также только печатает план. `--plan`
   остаётся планом и при наличии `--yes`: он не берёт блокировку, не создаёт
   и не меняет файлы и их mtime, в том числе из-за логирования. План показывает
   цепочку, состояние журнала, результат проверки backup и оставшиеся отказы.
5. Выполни format migration с тем же фактическим путём снимка:
   `shamir-server --config db.ktav format-migrate --from-backup /backups/YYYYMMDD_HHMMSS --yes`.
   По умолчанию backup должен быть не старше 24 часов; окно можно изменить
   через `--max-backup-age-hours N`. `--yes` подтверждает выполнение и нужен
   также для старого adopted-штампа или данных без штампа.
6. Только после успешного завершения, пока сервер всё ещё остановлен, выполни
   `shamir-server --config db.ktav doctor` для отдельной диагностики; затем
   запусти сервер. Не запускай сервер, если format migration завершилась
   отказом или прервалась.

**Ручное восстановление повреждённого штампа.** Удаляй `format.stamp` и
`format.stamp.tmp` ТОЛЬКО после сверки данных с backup: каталог после этого
считается данным базовой эпохи и будет «усыновлён» при следующем старте.

**Пре-release данные** (до первого релиза с штампами) перемещаются только
логическим экспортом/импортом — обновление таких каталогов на месте не
поддерживается.

**Что проверяет backup gate.** По умолчанию `--from-backup` должен указывать на
снимок с корректным `manifest.json` и проверяемыми контрольными суммами. Его
копия `format.stamp` должна совпадать с исходным каталогом по эпохе и
`created_by`, а имена верхнего уровня снимка должны покрывать имена живого
каталога. Время `created_at` не должно быть в будущем и должно попадать в
окно `--max-backup-age-hours` (по умолчанию 24 часа). Пустой снимок и снимок
без manifest-backed пользовательских данных отвергаются; строгая проверка
покрытия может также отвергнуть пустой каталог, отсутствующий в manifest.
Для покрытия не учитываются только `cache/`, `.shamir.lock` и
`format_migration.journal`.

`--i-have-a-backup` явно обходит проверку манифеста, идентичности и свежести
и сопровождается предупреждением; используй его только если сам независимо
проверил пригодный снимок до format migration. Для данных без штампа проверка
по копии штампа невозможна, поэтому stampless base требует такого явного
override (и `--yes` для выполнения). Старый `adopted`-штамп тоже требует
`--yes` и сопровождается громким предупреждением. Проверка идентичности —
не математическое доказательство: другой каталог той же эпохи с тем же
`created_by` и теми же именами верхнего уровня может пройти gate. Это
ограничение Q9; привязка по UUID каталога отложена.

**Отказы и их смысл.** Старшая эпоха, новый layout, повреждённый штамп или
оборванный временный штамп остаются отказами: `format-migrate` их не исправляет;
восстанови проверенный backup либо используй соответствующую версию для
ручного восстановления. Более старая эпоха, для которой есть полная
поддерживаемая цепочка, — штатный случай для этой команды. Данные без штампа трактуются как базовая эпоха и
требуют явного подтверждения. Если эпоха уже текущая, команда сообщает
«nothing to do» и завершается с кодом 0. Если команда не может взять
`.shamir.lock`, сервер или другая обслуживающая команда ещё работает:
останови её и повтори запуск. Не обходи эту проверку.

**Сбой и продолжение.** Пока не завершены все шаги и проверка целостности,
штамп остаётся старым, поэтому обычный запуск сервера продолжит отказывать.
После сбоя оставь `format_migration.journal` на месте и повтори команду тем же
бинарником и той же цепочкой модулей; исходный backup до миграции можно снова
передать через `--from-backup`, только пока он укладывается в окно свежести.
Журнал позволяет продолжить с нужного шага, включая повтор незавершённого
шага. Не снимай backup частично преобразованного каталога, не удаляй и не
редактируй журнал. Повреждённая запись в середине журнала, чужая цепочка или
другая версия бинарника не возобновляются: восстанови исходный backup до
миграции. Штамп продвигается только после последнего шага и заключительной
проверки целостности.

Проверка целостности перед записью нового штампа ограничена существующим
read-only `TablesRegistry::open`: она декодирует `wire_tables.mpack`; отсутствие
или пустое содержимое реестра допускается. Это не проверка здоровья таблиц,
индексов или WAL и не полный integrity scan. Переиспользовать `doctor` до
обновления штампа нельзя: старую эпоху блокирует format gate, а сам `doctor`
открывает engine. Поэтому после успешной миграции отдельно запускается
остановленный `doctor`; не считай проверку перед штампом эквивалентом `doctor`.

**Откат — только восстановление.** In-place отката формата нет. Останови
сервер, восстанови снимок, снятый до начала миграции, например
`shamir-server --config db.ktav restore --from /backups/YYYYMMDD_HHMMSS`,
затем используй старый бинарник, соответствующий восстановленной эпохе.
Restore переносит штамп вместе со снимком; восстановленный старый формат при
необходимости снова потребует явной format migration.

Если обновление на месте невозможно, запасной путь R3 — логический export
старым бинарником и import новым; см. раздел «Logical dump: offline export
and import» ниже.

**Windows:** гарантии durability штампа опираются на fsync; на Windows
directory-flush отсутствует — при потере питания возможна потеря самой
последней записи штампа (каталог тогда трактуется как без штампа, что
безопасно).

**Backup/restore и штамп.** `backup` копирует `format.stamp` байт-в-байт
(он входит в манифест с SHA-256), `restore` переносит его вместе со снимком
и сам штамп не пишет. `cache/` — производные данные: он исключён из штампа
и snapshot.

**Референсный тест совместимости.** В репозитории лежит коммит-фикстура
`reference_epoch_1` — каталог данных, записанный реальным релизным бинарником
эпохи 1 (генератор `scripts/runners/make_reference_datadir.py`, CI-workflow
`reference-datadir.yml`). Интеграционный тест `shamir-server` при каждом прогоне
CI копирует эту фикстуру, проверяет штамп, запускает `format-migrate`
(сегодня — «nothing to do» с байт-в-байт неизменным содержимым) и открывает
копию свежим сервером, сравнивая содержимое семантически: строки и схемы
таблиц, метаданные индексов каждого вида, функции (включая `call_signature`),
принципалы/права, восстановление WAL. Когда генератор меняется, CI дополнительно
генерирует каталог заново и сверяет свежий прогон с закоммиченной фикстурой.
Тем самым путь «старая эпоха → текущая» всегда остаётся исполнимым и
проверенным — ещё до первого реального изменения формата.

## Logical dump: offline export and import

Логический дамп — это переносимый interchange-артефакт, а не физический backup:
он хранит логическую схему и выбранное состояние одной базы в каноническом
named-MessagePack стриме с контрольными суммами. Это плоскость для переноса
между машинами и для fallback «старый бинарник export → новый бинарник import»,
когда in-place format migration невозможна. История, удалённые записи,
replication-конфигурация (`profiles`/`publications`/`subscriptions`), токены и
секреты в дамп не попадают никогда; импортированная база получает новую,
независимую lineage.

**Stored shapes, не исходные типы.** Storage codec сохраняет `Dec`/`Big` как
строки и `Set` как список, поэтому дамп фиксирует то, что реально хранится:
сохранённая строка возвращается как `str`, сохранённый список — как `list`
(рекурсивно внутри вложенных map/list). Round trip байт-в-байт воспроизводит
сохранённые байты; `Dec`/`Big` приходят обратно как `str`, `Set` — как `list`.
Импорт принимает все теги value envelope (`dec`, `big`, `set` включительно),
поэтому будущий экспортёр с явной типизацией не потребует изменений импорта.

**TTL.** Per-table `buffer.ttl_ms` — это настройка memory buffer'а, а не
срок жизни записи: длительность переносится verbatim в конфигурации таблицы,
содержимое buffer'а пересоздаётся при импорте. Per-record expiry в системе нет,
сроки не переводятся и записи как «истёкшие» не отбрасываются.

### logical-export

Offline-команда: сервер не запускается, `data_dir` берётся из `--config` и
открывается с эксклюзивной блокировкой (recovery завершается до freeze).

```bash
shamir-server --config db.ktav logical-export --db app --output /backup/app.dump
# сужение области видимости:
shamir-server --config db.ktav logical-export --db app --repo archive --output /backup/archive.dump
shamir-server --config db.ktav logical-export --db app --repo archive --table events --output /backup/events.dump
# только определения (без записей):
shamir-server --config db.ktav logical-export --db app --schema-only --output /backup/app-schema.dump
```

| Flag | Meaning |
|---|---|
| `--db <name>` | Обязательный. Имя базы; внутренняя `__system__` запрещена. |
| `--output <path>` | Обязательный. Путь артефакта; этапные файлы пишутся рядом и публикуются атомарно после проверки. |
| `--repo <name>` | Только указанный repository (по умолчанию — все). |
| `--table <name>` | Требует `--repo`. Только указанная таблица. |
| `--schema-only` | Опустить секцию записей; все выбранные определения и все консервативные ограничения всё равно проверяются. |
| `--include-artifacts` | Зарезервировано; всегда явный отказ («guest functions/modules are not implemented»), никогда не принимается молча. |

Экспорт выполняется под maintenance-freeze: после холодной инициализации и
проверки каталога/движка захватывается snapshot-читатель, страницы пишутся через
ограниченный внешний merge sort (raw key bytes упорядочиваются на диске, не в
памяти), затем файл sync'ается, закрывается, полностью верифицируется и только
потом атомарно публикуется (same-volume no-replace commit). Незавершённые
этапные файлы при ошибке/отмене удаляются. Если после успешной публикации
Unix directory fsync упал с ошибкой, команда сообщает «published a verified
artifact … final file retained» — это durability-ошибка с готовым файлом, не
неизданная попытка: не удаляй и не blindly повторяй.

### logical-import

Импортирует проверенный дамп в **новый** серверный data directory: цель должна
отсутствовать или быть пустой; слияние и перезапись невозможны.

```bash
shamir-server --config new.ktav logical-import --input /backup/app.dump --target /data/restored
# переименовать базу во всех ссылках:
shamir-server --config new.ktav logical-import --input /backup/app.dump --target /data/restored --as-db app2
# требовать schema-only артефакт (зеркалит экспортный --schema-only):
shamir-server --config new.ktav logical-import --input /backup/app-schema.dump --target /data/restored --schema-only
```

| Flag | Meaning |
|---|---|
| `--input <path>` | Обязательный. Путь артефакта. |
| `--target <dir>` | Обязательный. Новый data directory: отсутствующий или пустой. |
| `--schema-only` | Принимать (и только) schema-only артефакт; несовпадение scope — отказ. |
| `--as-db <name>` | Переименовать импортируемую базу во всех ссылках: имя `DatabaseItem`, scope записей, ACL resource refs и строки владения в каталоге. |

Отмена — через процессный сигнал (Ctrl-C), отдельного флага нет: операция
join'ится, а не отсоединяется; до публикации этап отбрасывается, после
успешной публикации сообщается успех. Прогресс — на stderr, итог — на stdout.

### Security: идентичности и ACL

В дамп попадают только логические имена владельцев и их kind, никогда —
учётные данные: salt/stored/server key/KDF-параметры, токены, ticket-счётчики,
roles, superuser/replicator-состояние и user ID не экспортируются. Импортированные
identity создаются **disabled и passwordless** (`insert_disabled_import`):
авторизация для них закрыта, как для неизвестного пользователя, но имя
резолвится для маппинга владения. Права никому не выдаются неявно: ни grants,
ни administrator-статус. Системное владение остаётся null — требует явного
решения оператора, а не молчаливой привилегии. ACL-владелец, которого нет среди
экспортированных identity, — типизированный отказ, никогда молчаливый `System`
grant. Уже существующие chown/chgrp работают с импортированными владельцами как
обычно.

### Что отказывается сегодня и почему

Fail-closed: неподдерживаемое содержимое вызывает отказ экспорта/импорта,
никогда не пропускается молча и не превращается в «пустой успех».

* **Индексы** всех семейств (regular/unique, sorted, full-text, functional,
  vector) — дескрипторы не реализованы; данные/определения не теряются молча.
* **Схемы таблиц** (stored schemas, schema validators, transforms, FK-биндинги) —
  некорневые `schema_version`/`schema` не переносятся.
* **Функции, validators, function folders** — любая строка этих семейств в
  глобальном каталоге вызывает отказ даже если выбранная база на них не
  ссылается: анализ замыкания ссылок — будущая работа.
* **Группы** — group identities, membership, group ACL и расширенные grants —
  будущая работа; group-метаданные в repository/table/ACL-элементах — отказ.
* **Guest modules** — `--include-artifacts` всегда отказ; артефакт с
  guest-artifact scope на импорте отклоняется.
* Также отказываются: in-memory/hybrid repositories, non-default durability
  overrides, non-default buffer sizing/flusher (кроме `ttl_ms`), quarantined
  (`resync_required`) и rename-in-doubt repositories, application-schema
  version в заголовке артефакта, required features, unknown поля каталога.

### Ограничения и бюджеты

Это бюджеты (format limits), а не измеренные результаты — больших датасетов
acceptance не прогонялся. Фрейм ≤ 16 MiB, агрегированные метаданные ≤ 64 MiB,
артефакт ≤ 1 TiB (файловый preflight отвергает превышение до чтения), глубина
вложенности 64, до 65 536 элементов на контейнер (один `List`/`Set`/`Map` или
инвентарь секции). Дополнительные scope-лимиты: 8 MiB на кодирование выбранного
определения, до 4096 ACL и identity, до 1024 байт на логическое имя,
2048 узлов / 512 KiB на запись (ключ/бинарные байты), 1 MiB на сортировочную
запись. Целевые бюджеты CI (не measurements): 64 MiB датасет — export/import/verify
≤ 60 s, index build/validation ≤ 180 s; 1 GiB — ≤ 600 s; incremental RSS
≤ 256 MiB над baseline открытой базы.

### Lock, stage и восстановление после сбоя (import)

Всё держится **рядом с целью**, не внутри неё — публикация это один same-volume
rename на отсутствующее имя:

* `<target>.import.lock` — эксклюзивный create-new lock, берётся **до** осмотра
  цели и держится весь запуск. Существующий lock (конкурентный или от
  сбойнувшей попытки) — типизированный отказ. Снимай его только убедившись,
  что процесс-владелец мёртв; не обходи проверку.
* `<parent>/.logical-import-<unique>/` — этапный каталог. Устаревшие этапы
  сбойнувших попыток удаляются только под удерживаемым lock, перед созданием
  нового этапа.

После сбоя/отмены до публикации этап удаляется целиком; записи не
резюмируются — повтор всегда начинает свежий этап. Загрузка, flush всех
buffer'ов (включая TTL), закрытие движка, повторное открытие и валидация из
свежего open'а, запись receipt'а и только затем — rename. Переименование на
Windows ограниченно повторяется для transient sharing-ошибок (straggler
handles), при этом появившаяся между попытками цель немедленно отменяет retry.

### Idempotent retry и receipts

Успешный импорт кладёт в опубликованный каталог
`.logical-import-receipt.json`: SHA-256 всего артефакта, имя исходной базы,
эффективное имя (после `--as-db`), `schema_only`, format epoch, generator.
Повторный запуск против уже импортированной цели принимается идемпотентно
**только** при совпадении digest и всех effective options **и** при
прохождении integrity check (читаемый format stamp, движок переоткрывается,
содержимое по-прежнему равно артефакту). Любое несовпадение — типизированный
отказ; перезаписи нет. Цель без receipt'а или с повреждённым receipt'ом —
отказ, а не «переимпорт». Обычная цель для повторной попытки — crash/power-loss
после публикации, но до того, как успех был сообщён оператору.

### Verify-only workflow

Отдельной CLI-подкоманды верификации логического дампа сегодня нет — верификатор
живёт в библиотеке: `shamir_sync::replication_seed::verify_dump_file(path)`
проверяет magic, версию, required features, каноничность кодирования, лимиты,
контрольные суммы фреймов/секций, счётчики, trailer prefix digest и EOF **без
открытия базы**. Экспорт всегда верифицирует артефакт до публикации; импорт
прогоняет полный verify pass в preflight до любого изменения диска. Для
операторской проверки артефакта используй это API (или дождись CLI-обёртки):
коррупция, усечение, trailing bytes, несовпадение инвентаря — отказ до
какой-либо публикации.

### Пример запроса (builder, не wire-JSON)

Проверка результата после импорта — обычным клиентом и query builder'ом
(builder-примеры не являются спецификацией wire-протокола; никакого ручного
serde_json):

```ts
const repos = ddl.listRepos('app2');           // после --as-db
const tables = ddl.listTables('app2', 'archive');
const described = ddl.describeRepo('app2.archive');
```

## 9. Capacity planning

Оценки ниже выведены из структур в коде, а не измерены — сверяй с `/metrics` своей инсталляции (источник: `docs/dev-artifacts/ops/CAPACITY_PLANNING.md`, частично устарел). Кратко:

|| Подсистема | Память |
||---|---|
|| Idle процесс | ~50 MB |
|| Активная сессия | ~2 KB |
|| Активное соединение | ~8 KB |
|| Argon2 peak | `argon2_concurrent_max × memory_kb × 1.05` |
|| Steady-state | `50 MB + sessions × 2 KB + connections × 8 KB` |

10 000 сессий → ~134 MB (без Argon2 spike).

|| Операция | CPU |
||---|---|
|| Argon2id verify | ~50 ms (defaults) |
|| TLS handshake | ~5 ms |
|| Post-auth request | ~20 µs |
|| Batch query (cold) | ~1 ms / query |

## 10. Prometheus + Grafana

`deploy/README.md` содержит готовую конфигурацию Prometheus:

```yaml
scrape_configs:
  - job_name: shamir-db
    static_configs:
      - targets: ['127.0.0.1:9090']
```

И примеры alert-правил:
* `auth_attempts_total{result="bad_proof"}` > threshold → brute-force alert.
* `shamir_tx_aborted_ssi_total` growing → contention hot-spot.
* `increase(shamir_visibility_ack_total{result="pending_timeout"}[5m])` > 0 →
  commits are answering Pending (bounded mode): a writer version is unsettled —
  correlate with `shamir_gate_oldest_blocker_*` before restarting anything.
* `increase(shamir_visibility_pending_total{reason="reopen_required"}[5m])` > 0 →
  the repository needs reopen/recovery attention, not client retries.
* `increase(shamir_visibility_read_wait_timeouts_total[5m])` > 0 → AsOf reads
  are waiting past `read_visibility_timeout_ms`: check the blocking writer.
* `shamir_visibility_ack_waiters` near the connection limit → acknowledgement
  parking is consuming connection slots; switch the service to
  `visibility_ack_mode = "bounded"` (connection-resource liveness).
* `increase(shamir_visibility_reconciliation_total{result="pending_timeout"}[15m])`
  high → callers reconcile repeatedly against a stalled version; investigate
  `shamir_gate_unsettled_versions` / orphan-replay counters.
* Expiry of any of these waits also emits a rate-limited WARN log (at most one
  per repository per 5 s) with `target`, `visible_through`, `blocking_version`
  and `elapsed_ms` — alert on the metric, read the log for the position.

## Что важно знать уже сейчас (дозированно)

* **Один бинарник — все платформы.** `service install` генерирует
  платформенный юнит автоматически. Не нужен отдельный пакет.
* **Observability включена по умолчанию** на `127.0.0.1:9090` (loopback).
  Отключается только явным `addr: ""`; публичный адрес требует
  `allow_public_metrics: true` и внешней аутентификации.
* **Graceful shutdown — bounded.** 30 s deadline и общий бюджет репозиториев
  остаются неизменными. Ошибка flush/clean horizon или истечение deadline —
  unclean shutdown: exit code **70**, один ERROR с именами неуспешных
  репозиториев и счётчик `shutdown_unclean_total`. Clean требует quiesce,
  drain, W = allocation high-water и сохранённого J. При следующем старте
  cold reconciliation восстанавливает выводимые v3 journal events из
  сохранившегося WAL; совпадающие события остаются без изменений,
  несовпадающие bytes и любые I/O/decode ошибки запрещают open. v3 entries
  с event header, из которых не выводится событие, gap не создают. Для
  неизвестных legacy entries или версий с отсутствующими evidence может
  остаться минимальный консервативный gap; уже существующие gap не
  очищаются. Journal и recovery floor flush'ятся до продвижения J к H, а J
  сохраняется до serving. Поэтому сам unclean shutdown не означает
  replication gap или reseed; OS reclaim'ает сокеты.
* **Single-instance guard.** Файловая блокировка (`fs4`) на `data_dir`.
  Два процесса на одних данных → второй не стартует.
* **Logging — non-blocking.** Никогда не блокирует горячий путь. Overflow
  — lossy (лучше потерять лог, чем запрос).

## Application schema migrations (Rust client)

Schema changes ship as an **ordered, checksummed migration set compiled into
your application**, applied through `shamir_client::schema_migration`. There
is **no down or rollback API** and **no automatic retry**: after any uncertain
apply you ask the server what happened instead of resending. Bodies are built
**only** from the typed DDL builders (`shamir-query-builder`) — hand-written
wire JSON has no canonical form and no checksum the server can verify.
Version 1 admits **DDL only — no DML**.

```rust
use shamir_client::builder::{ddl, SchemaMigration};
use shamir_client::schema_migration::{
    ApplicationMigrationSet, MigrationError, PrefixCoverage, SchemaMigrationTarget,
    UnknownApplyResolution,
};

let m1 = SchemaMigration::new("add-audit-table", 1, "add audit table")
    .op(ddl::create_table("audit").repo("app").build())? // build() yields a typed BatchOp
    .build()?; // validates against the admitted DDL subset
let m2 = SchemaMigration::new("add-audit-archive", 2, "add audit archive")
    .op(ddl::create_table("audit_archive").repo("app").if_not_exists().build())?
    .build()?;
let set = ApplicationMigrationSet::new(vec![m1, m2])?;
```

`ApplicationMigrationSet::new` validates the set **before anything can be
sent**: ordinals contiguous from exactly 1, unique ids, typed errors. A gap or
malformed body is a **local** rejection — zero requests. Each body has
versioned canonical bytes and a SHA-256 checksum (cached by the set); the
server **recomputes** the checksum from the submitted typed body, so an
authored raw-JSON body or a client-reported checksum is never proof. A body
is immutable once applied: changing it changes its checksum — drift, refused
before any apply. Correct a released migration with a new forward migration,
never by editing the old body.

#### Functional indexes in migrations

`create_index` migrations admit functional indexes only with
`index_type = "functional"` and a `functional_op` of exactly
`lower | upper | trim | length` — the built-ins the server's index backend
constructs directly (`write_domain_index_admitted.rs`). Functional options on
any other (or omitted) index type, a functional index without an explicit op,
a name outside the allowlist, and ANY supplied `functional_args` — including
`[]` — are refused locally, before the journal intent is written. The typed
refusal is `SchemaMigrationError::UnsafeOperation` from the operation builder;
when the op goes through a migration-set builder, that error surfaces wrapped
as `MigrationError::InvalidSet(SchemaMigrationError::UnsafeOperation(…))`. `functional_args`
are refused because the server builds the index expression from the indexed
field and the op alone and never reads them. Note the index semantics:
`length` counts UTF-8 bytes of a string (element count of a list), not
Unicode characters.

### Target identity and permissions

`SchemaMigrationTarget { database_identity }` carries the **known stable
database identity**, supplied separately from the connection name;
`MigrationStatus::reported_target` echoes the serving identity on
`schema_migration_identity_mismatch`, which is the supported discovery path
for a wrong identity. Permissions (design §5.2): all three operations need
`Read` on the database — the preflight read before a write — while `plan` and
`apply` additionally need `Manage`, and apply still passes every underlying
operation's normal DDL authorization (that server-side `Manage` separation is
the server's, not the client's). The journal, fence generation and stored
bodies live in the server's durable system catalogue and survive restart;
`status` and `plan` perform **no migration journal or DDL changes** — they
are read-only with respect to migrations (the server's own legacy identity
backfill/persistence tail may run, but it never migrates anything on their
behalf).

### `status`, `plan`, `apply_next`

```rust
let target = SchemaMigrationTarget { database_identity };
client.status(db, target, &set).await?;      // by-value target, borrowed set
client.plan(db, target, &set).await?;        // read-only with respect to migrations
client.apply_next(db, target, &set).await?;  // applies exactly one next ordinal
```

`status` compares the **entire ordered applied prefix** against your local
set: the server's status wire reports every applied migration in ordinal
order (`applied`: `id` / `ordinal` / `checksum` per entry) plus the durable
`fence_generation`, so a client can prove its full local prefix, not just
the head. `plan` verifies the submitted set on the server against the same
journal.

`apply_next` itself reads `status` first and refuses **before sending the
APPLY** when the journal drifts from your set, when your set is shorter
than the applied journal, when the applied ordinals have a gap, when the
prefix is unverified, or when nothing is left to apply. The only zero
network round trip is a locally invalid set construction — every journal
refusal costs one read-only `status`. Otherwise it submits exactly **one**
next ordinal with the expected predecessor id and the fence generation taken
**directly from `status`'s `fence_generation` field** — never derived or
guessed. Concretely: on an empty journal the status reports
`fence_generation: 0`, the apply acquires generation **1**, and after the
fence is released the next status reports `fence_generation: 2` — the
expected generation of the next fresh apply. The generation an apply
acquired is not reused as the next expectation — the next expectation is
always the fresh `fence_generation` of the following status, a distinct
number read from the wire, not inferred from ordinals. When the set is
already fully applied, a repeat call applies nothing and returns
`NothingToApply`.

### Lost response: resolve, never resend

A transport error or timeout **after** an apply was sent returns
`MigrationError::UnknownOutcome { attempt, .. }` — the client never resends
on its own. The opaque `UnknownApply` token binds the database, the target
identity and the exact id / ordinal / canonical checksum of the in-flight
body; the resolver correlates the returned status against your set locally —
it does not ask the server "by id". Reconnect, then resolve through the
targeted status helper:

```rust
// `reconnected_client: &Client` — an already connected client to the SAME
// server and database as the original apply.
match client.apply_next(db, target, &set).await {
    Err(MigrationError::UnknownOutcome { attempt, .. }) => {
        // one read-only status query; the apply is never resent
        let resolution = reconnected_client
            .resolve_unknown_apply(&attempt, &set)
            .await?;
        match resolution {
            // journal proves the attempt applied with the same
            // id/ordinal/checksum — anywhere in the verified reported
            // `applied` list, even when a LATER migration already applied:
            // success, never resend
            UnknownApplyResolution::Applied { .. } => {}
            // attempt IS the unresolved head: operator reconciliation or
            // the identical-body resume below; the server's reason is in
            // `reason`
            UnknownApplyResolution::InDoubt { .. } => {}
            // absence AND the predecessor are both established: safe to
            // call `apply_next` again
            UnknownApplyResolution::ReadyToApply => {}
            // head overtook the attempt without proving it, or an
            // unresolved event for a DIFFERENT migration: no false
            // success; reconcile out of band
            UnknownApplyResolution::EvidenceUnavailable { .. }
            | UnknownApplyResolution::UnmatchedUnresolved { .. } => {}
        }
    }
    Err(MigrationError::UnsupportedServer { .. }) => { /* upgrade the server */ }
    other => { /* §5.3 stable codes mapped onto MigrationError, message kept */ }
}
```

The server's stable codes (`schema_migration_in_progress`, `_in_doubt`,
`_checksum_mismatch`, `_gap`, `_predecessor_mismatch`, `_follower_read_only`,
`_durability_unavailable`, `_postcondition_failed`, `_identity_mismatch`,
`schema_migration_operation_deferred:`) map onto typed variants without
losing the server message.

`resolve_unknown_apply` checks the attempted id / ordinal / canonical
checksum against the **whole verified reported `applied` list**, not only
the head: the proof survives even when a later migration has already
applied past the attempt. The safe-to-reapply verdict (`ReadyToApply`) is
issued **only** when both the absence of the attempt and its predecessor
are established — a proven, drift-free prefix at the attempt's predecessor,
no unresolved event, and the attempt's ordinal being the set's next local
ordinal. All correlation happens locally against your compiled set; the
apply is never replayed on the strength of a status answer alone.

**Resume after in-doubt** uses the *same* `apply_next`: the identical stored
body, expected predecessor, and the fence generation reported for the
unresolved migration in `status` (the unresolved entry's own generation) —
the server refuses to take over a still-running owner, then acquires a new
generation when it fences the old owner and reconciles. The server
re-checks the checksum of the submitted body and concurrent contention.
An in-doubt diagnostic alone does not prove the outcome, and the client
cannot confirm an in-flight local attempt from its own side — only the
server's answer can. Do **not** assume a refused apply has succeeded. Recovery is an operator decision after reading the diagnostics,
or a restore from backup — never a body edit or hand-run DDL, which would
break checksum identity and make every later apply refuse.

### Status wire and old peers

The public status result carries two additive fields (shared contract
`crates/query/shamir-query-types/src/admin/types/schema_migration/protocol/status.rs`):

* `fence_generation: u64` — the highest durable fence generation of the
  journal (`0` when the journal holds no event): the
  `expected_fence_generation` of the next fresh apply.
* `applied: Vec<SchemaMigrationAppliedEntry>` — every applied migration in
  ordinal order, each with `id`, `ordinal` and the 32-byte canonical
  `checksum`.

The existing fields (`last_applied_id`/`_ordinal`/`_checksum`,
`checksum_validation`, the unresolved `in_flight` entry with its
`in_doubt_reason` evidence) are unchanged; a typed wire error surface stays
separate from the public `MigrationStatus::server_error`.
On the client, `PrefixCoverage` now reports full-prefix coverage:
`PrefixCoverage::Verified` when every reported entry matches your set by
ordered id / ordinal / checksum, and `PrefixCoverage::Drift` when any entry
differs; the existing variants for an empty journal and for a set shorter
than the applied journal remain, with new coverage for an unverifiable
ordinal (`Unverified { ordinal }`) and a reported gap
(`Gap { expected, actual }`).

A peer old enough to answer `status` but omit `applied` and
`fence_generation` while the journal head is non-empty is not silently
treated as an empty journal: the client returns
`MigrationError::UnsupportedServer` — upgrade the server. There is no
guessing of fence generations, no per-id server lookup, and **no fallback
to unjournaled DDL on any path**.

```rust
// Sequential applies: each call reads status again before applying.
let s0 = client.status(db, target, &set).await?;
assert_eq!(s0.coverage, PrefixCoverage::Empty);        // empty journal, fence 0
client.apply_next(db, target, &set).await?;            // acquires generation 1, releases it
let s1 = client.status(db, target, &set).await?;
assert_eq!(s1.coverage, PrefixCoverage::Verified { ordinal: 1 });
assert_eq!(s1.fence_generation, 2);                    // next expected
client.apply_next(db, target, &set).await?;            // acquires generation 3
// Fully applied: a repeat call applies nothing and returns NothingToApply.
```

### Порядок эксплуатации: backup → status → plan → apply

Перед применением любой миграции снимите резервную копию (см. раздел
«Restore до/вместо обновления схемы» ниже) — это единственный путь назад: у миграций **нет**
down/rollback. Restore выполняйте только из **верифицированного полного**
snapshot (манифест проверен, копирование и seal завершены); усечённый или
непроверенный снимок для отката миграции непригоден. Дальше строгий порядок:

```rust
let s = client.status(db, target, &set).await?;    // только чтение: сверка префикса и fence_generation
let plan = client.plan(db, target, &set).await?;   // только чтение: серверная сверка plan до apply
// Осмотрите s (и план) ПЕРЕД apply: проверенный префикс, отсутствие in-flight,
// следующий ординал. Ничего не применяйте вслепую.
if s.journal_checksum_verified() && s.in_flight.is_none() && s.next_local_ordinal.is_some() {
    client.apply_next(db, target, &set).await?;    // ровно один следующий ординал
}
```

`status` и `plan` не пишут ни журнал миграций, ни DDL; `apply_next` сам
читает `status` и берёт `fence_generation` прямо из его ответа — не
выводите поколение сами и не кэшируйте его между запусками.

### Maintenance fence: что блокируется и что видит писатель

Пока apply миграции **фактически идёт** — живой раннер владеет
maintenance fence этой базы, — движок отклоняет: **любой батч**, содержащий
запись, DDL
или вызов хранимой процедуры (`CallOp` — гостевой код может писать через
прямые интринсики), включая вложенные `Batch`/`ForEach`; отклонение —
целиком, со стабильным кодом `schema_migration_in_progress`. **Чтения
проходят**, как и сами операции `ApplicationSchema` (status/plan/apply) —
apply проверяется отдельно по durable-журналу. DDL-обработчики самой
миграции отказов не получают.

Писатель видит `BatchError::QueryError` с сообщением вида
«an application-schema migration holds the maintenance fence for database
'…'; writes, DDL and stored-procedure calls are refused until it
completes» и указанным кодом. Fence живёт столько, сколько длится apply
**при успешном исходе**: durable-поколение снимается лишь после durable
записи `applied`-события в журнал. Если apply уже владел fence и начал
исполнение, но завершился неуспешно или был прерван, обычные записи,
DDL и вызовы процедур этой базы остаются под отказами с кодом
`schema_migration_in_doubt`, пока миграция не будет безопасно возобновлена
тем же телом или пока не будет выполнен restore из верифицированного
backup с повторным открытием базы. Отказ **до** захвата fence (admission:
дрейф, чужое поколение, живой владелец и т.п.) ни нового fence, ни
in-doubt-состояния не создаёт и существующее не снимает: база остаётся в том
состоянии, в котором была.

**При перезапуске сервера.** Если после перезапуска журнал базы содержит
валидную незавершённую историю, принадлежащую этой базе (fence durably
занят, исход попытки не доказан), база открывается в состоянии in-doubt
с **известным** состоянием: `status` возвращает незавершённую запись
`in_flight` и, когда известно, причину и индекс неуверенной операции.
Пока миграция не возобновлена, писатели/DDL/`CallOp` этой базы получают
отказы с кодом `schema_migration_in_doubt`; перезапуск сам по себе
незавершённый fence не снимает.

**Повреждённый журнал при открытии (старт прерывается целиком).** Если
обычная валидация истории при открытии обнаруживает повреждение
durable-журнала миграций — как отнесённое к конкретной базе (у записи
пригодный record key с identity базы, но содержимое повреждено:
несовпадающие или невалидные метаданные, тип или payload записи,
недостающие события, расхождение последовательности или поколения,
отсутствующая control-запись при непустой истории этой базы), так и
неотносимое ни к одной базе (запись без record key / identity либо с
противоречащими друг другу ключом и identity, или повреждение, из-за
которого нельзя прочитать сам конверт записи), — **открытие всего
ShamirDb/сервера прерывается** с отказом того же типизированного кода
`schema_migration_durability_unavailable`; стабильный код виден
оператору в **диагностике старта**, а не как wire-ответ на запрос.
Повреждённая запись не отбрасывается и не «чинится» на лету.
Восстановление: полный restore каталога данных (`data_dir`: журнал
живёт в системном хранилище, и схема, данные и журнал должны быть
восстановлены согласованно из одной верифицированной копии; системное
хранилище отдельно от репозиториев не восстанавливают) и повторный
запуск; никогда не угадывайте, какая запись «лишняя», и не правьте
каталог вручную — офлайн-хирургии не предусмотрено.

**Изоляция при запросе (рантайм), не при старте.** Если повреждённое
событие/тело или несогласованный раздел журнала маршрутизируется на
одну базу (у конверта записи пригодный ключ с identity), отказ
получают `status`/`plan`/`apply` **только этой базы** — с тем же кодом
`schema_migration_durability_unavailable`; базы, чьи разделы журнала
прочитались без ошибок, продолжают обслуживаться. Конверт без
пригодного ключа маршрутизации не относится ни к одной базе, поэтому
отказать могут `status`/`plan`/`apply` **всех** баз. В рантайме тот же код возвращают
`status`/`plan`/`apply` и при обычных отказах системного хранилища в
момент запроса — нечитаемость, отказ записи или синхронизации,
недоступность раздела, — поэтому сам по себе этот код в рантайме **не**
означает повреждение и **не** является основанием немедленно
восстанавливать backup: сначала диагностируйте и восстановите
доступность хранилища, затем заново прочитайте `status` и только по его
результату решайте следующий шаг. Слепые повторы apply недопустимы;
если durable-намерения (события журнала) уже были записаны до отказа,
незавершённый run может вместо этого отчитаться
`schema_migration_in_doubt` — тогда действуйте по процедуре in-doubt
ниже, а не как при повреждении журнала.

**Жизненный цикл базы, затронутой миграцией.** Пока apply выполняется
(включая время до подтверждения долговременного захвата fence) или при
in-doubt-состоянии базы (незавершённая валидная история), `RENAME` и
`DROP` этой базы, включая `CASCADE`, отказываются
со стабильным кодом `schema_migration_in_doubt`. Этот отказ —
**исключение из области действия fence**, который ограничен
обслуживающей базой: проверка lifecycle выполняется по
**целевой** базе, поэтому она срабатывает и когда запрос отправлен через
другую базу. Во время активного apply действие оператора — **ждать**
завершения (тот же код `schema_migration_in_doubt`); обычные операции
из самой обслуживающей базы в это время отклоняются с кодом
`schema_migration_in_progress`. `CREATE DATABASE IF NOT EXISTS` по уже
занятому имени, отправленный **через другую, здоровую базу**, —
идемпотентный exists/no-op: он не пересоздаёт базу и не считается
ошибкой; если же запрос отправлен **через саму обслуживаемую базу**,
любая запись/DDL в ней уже отклонены обычным write/DDL fence.
Живой fence снимается только **успешным** завершением apply;
при неуспешном или прерванном apply действует in-doubt-состояние, как
описано выше. Снять его можно только resume тем же телом
(идентичным сохранённым телом незавершённой миграции) или restore из
верифицированного backup с повторным открытием базы.
Офлайн-хирургии каталога не предусмотрено; обходить fence или применять
миграции вслепую нельзя.

Fence действует в области **обслуживаемой** базы: batch-entry fence
осматривает всё дерево операций сессии этой базы — вложенные `Batch`/
`ForEach` с DML, DDL и `CallOp` — и отклоняет пакет целиком. Явные
**кросс-базовые административные операции из другой базы** этим fence
не покрыты: например, привязка или отвязка валидатора (bind/unbind),
отправленная в другую базу, адресует таблицу целевой базы напрямую и
fence целевой базы не блокируется. **Никогда не выполняйте такие
операции во время идущей миграции и не адресуйте базу с in-doubt-состоянием** —
они обойдут и живой fence, и защиту in-doubt. Права: сервер требует
**администратора** для административных операций (привязка и отвязка
валидатора — административные операции и не входят в упрощённые
исключения серверного шлюза), а стандартные проверки доступа —
администратору нужны подходящие права на **обе** базы: пакет
авторизуется в **обслуживаемой** базе — сначала `Read` на саму базу,
затем все пары `(действие, ресурс)` по всему дереву запросов — а
операция привязки/отвязки валидатора дополнительно требует `Write` на
**целевой таблице** (`Action::Write` по пути `db.repo.table`); отдельной
проверки «администратора целевой базы» (`Manage`/`Root`) в этом пути
нет — не полагайтесь на неё в этих путях.

### Follower / read-replica

Apply **и любые другие application-schema операции — `status` и `plan` тоже** —
разрешены только на primary: у follower нет консистентного вида
журнал+схема, поэтому серверный шлюз отказывает **все три** операции со
стабильным кодом `schema_migration_follower_read_only` ещё до входа в
движок. Режим узла опускается в движок один раз при запуске сервера;
отдельная проверка на стороне движка страхует только **apply**, если
серверный шлюз когда-нибудь обойдён, — `status` и `plan` такой второй
проверки не имеют.

### Checksum drift: только вперёд

Тело применённой миграции **неизменяемо**: сервер пересчитывает
канонический checksum из присланных типизированных тел, и любое изменение
уже применённого тела меняет его checksum — `status` завершится успешно
и вернёт диагностическую `PrefixCoverage::Drift` (с ординалом расхождения),
а локальный `MigrationError::Drift` возвращает **только `apply_next`** —
он отказывает ещё до отправки APPLY; серверные `plan` и apply тоже
отказывают с кодом `schema_migration_checksum_mismatch` — `plan` отвергает
изменённое тело уже применённого ординала без всякой записи. Это локальный
типизированный вариант клиента, отдельный от wire-кода. Исправление — всегда **новая
миграция вперёд** с новым ординалом; правка старого тела запрещена.
Обратите внимание: ручной DDL **меняет фактическую схему, но не меняет
checksum тела в журнале**, поэтому такой дрейф `status` сам не
обнаруживает — сверяется journal body, а не физическая схема. Восстановление согласия между данными и журналом при
дрейфе — повторный restore из консистентного backup (см. ниже).

### In-doubt: что читать из status и кто reconcile

Если сервер не может доказать исход попытки apply, `status` возвращает
незавершённую запись `in_flight` с полями `migration_id`, `ordinal`,
`operation_index` (индекс неуверенной операции внутри тела, когда
известен), `fence_generation`; причина in-doubt — в поле `in_flight.in_doubt_reason`
(запись `in_flight`), а **не** в статусном коде верхнего уровня — читайте
причину и индекс операции именно оттуда, не гадайте.

Дальше два пути:

* **Сервер reconcile сам** при повторном apply **точно того же тела**:
  resume использует тот же `apply_next` (идентичное сохранённое тело,
  ожидаемый predecessor и поколение fence, сообщённое status для
  незавершённой миграции). Сервер отказывает живому владельцу, берёт
  новое поколение, закрывает журнал для прежнего владельца и сверяет
  каждую операцию из её записанного intent (create/rename/alter/drop/check,
  bind/unbind — по binding-специфичному evidence): завершившуюся операцию
  сверка доказывает без повторного исполнения; операции, которую сверка не
  может доказать или безопасно завершить, отклоняется — «все
  идентичные resumes безопасны» не гарантируется; при невоспроизводимой
  истории сверка останавливается с отказом. Отдельной «reconcile»-операции в первом
  релизе нет.
* **Вмешательство оператора** требуется, когда reconciliation не может
  доказать или безопасно завершить эффект (например, постусловие операции
  не выполнилось — `schema_migration_postcondition_failed`, либо
  `EvidenceUnavailable`/`UnmatchedUnresolved` от резолвера: голова журнала
  обогнала попытку, не доказав её). Если apply был отправлен и исход его
  неизвестен (`MigrationError::UnknownOutcome { attempt, .. }`), сначала
  вызовите `resolve_unknown_apply(&attempt, &set)` — один read-only
  `status` с типизированным вердиктом (`Applied`, `InDoubt`,
  `ReadyToApply`, `EvidenceUnavailable`, `UnmatchedUnresolved`);
  вмешательство — только по его результату. Действия оператора — только
  диагностика по status/plan/логам и restore из консистентного backup;
  **не** правьте тела и не выполняйте DDL руками. Не считайте отказанный
  apply случившимся: исход доказывает только ответ сервера.

### Restore до/вместо обновления схемы

Журнал миграций живёт в durable-системном каталоге и восстанавливается
вместе с backup. Применённый префикс **восстановленного** журнала —
авторитет: ординалы, которых в нём больше нет, могут быть применены
заново **дословно** (то же самое тело) — это не нарушает
неизменяемость, потому что журнал-авторитет их не содержит, и повторный
verbatim-apply применённого ординала завершается как no-op/успех.
Дословность при этом обязательна и на сервере: серверный `plan` (как и
apply) отвергает подмену тела уже применённого в журнале-авторитете
ординала кодом `schema_migration_checksum_mismatch`.
Восстановление mid-flight-снимка воспроизводит то же незавершённое
состояние; поколение fence после restore без владельца — следующий apply
берёт новое поколение и делает сверку, а не продолжает старого владельца.
Состояние «данные откатились, журнал нет» (или наоборот) — это
Corruption: дрейф тел журнала против множества следующий `status`
сообщит как `PrefixCoverage::Drift` (диагностика — сама команда не
падает), но **смешанный
backup-дрейф (физическая схема
расползлась с журналом при совпадающих телах) `status` не обязан
обнаруживать** — он сверяет тела журнала, не физическую схему; при
подозрении на такой дрейф — restore из согласия и верифицированная
проверка данных, а не вслепую apply.

Логический import существует, но он переносит только логические данные и
schema: журнал application-schema миграций R1c и durable-идентичность базы
через него не переносятся. Смешивание каталогов через import/restore в этом
релизе даёт несогласованное состояние; проектное ограничение будущего R3 —
отказывать при конфликте журнала или идентичности
(`schema_migration_identity_mismatch`).

### Допущенные виды операций (версия 1)

Версия 1 допускает **только DDL, без DML**. Закрытый набор видов операций
тела миграции (`model/operation.rs` в `shamir-query-types`):

* таблицы: `CreateTable`, `DropTable`, `RenameTable`;
* индексы: `CreateIndex`, `DropIndex`, `RenameIndex`;
* схема таблицы: `SetTableSchema`, `AddSchemaRule`, `RemoveSchemaRule`;
* функции: `CreateFunction`, `DropFunction`, `RenameFunction`,
  `CheckFunction`, `AlterFunction`;
* валидаторы: `CreateValidator`, `DropValidator`, `RenameValidator`,
  `BindValidator`, `UnbindValidator`.

Каждое тело валидируется локально при построении множества и повторно на
сервере при apply; недопущенная операция — типизированный отказ ещё до
сети. Функции и валидаторы серверно-глобальны: миграция, заменяющая
регистрацию, затрагивает и другие базы этого сервера, а такой apply
сериализуется на весь сервер.

Допущение в модели не гарантирует исполнения на рантайме: операции,
требующие HMAC по канонической политике, отказываются ещё в admission с
`hmac_required`; каждая операция также проходит обычную авторизацию цели
(`BindValidator` обязан целиться в ограждённую базу). `BindValidator`
допущен моделью, но на рантайме **всегда откладывается** с
`schema_migration_operation_deferred:` — зафиксировать существующие
строки до бинда безопасно нельзя. `AlterFunction` меняет **только**
режим optimize (замены тела/кода нет).

<!-- ORCHESTRATOR FOLLOW-UP (functional-index admission fix): limits of the
     functional-index subset are under review on branch rel-r1c-fix-funcidx
     (narrowing to `lower`/`upper`/`trim`/`length` without `functional_args`).
     Not merged into master at the time of writing; this guide intentionally
     does not enumerate the current limits. When the fix lands, state here:
     the built-in function allowlist and the rule on `functional_args`. -->

### Таблица стабильных кодов ошибок

| Код | Значение | Действие оператора |
|---|---|---|
| `schema_migration_in_progress` | Миграция владеет maintenance fence этой базы | Ничего: подождите завершения apply; писатели повторят попытку |
| `schema_migration_in_doubt` | Исход миграции не доказан (незавершённая валидная история). Для `RENAME` и `DROP` целевой базы этот же код означает, что для неё действует fence или in-doubt-состояние | Если apply ещё выполняется, дождитесь его завершения. Прочитайте `status` (причина и `operation_index`, если известны) и следуйте процедуре возобновления тем же телом; если reconcile не доказывает исход — restore из верифицированного backup |
| `schema_migration_checksum_mismatch` | Тело применённой миграции изменилось (drift) | Forward-миграция вперёд; тела не править; при дрейфе данных — restore |
| `schema_migration_gap` | В применённых ординалах дыра | Локальная ошибка множества; пересоберите set, проверьте ординалы |
| `schema_migration_predecessor_mismatch` | Ожидаемый predecessor не совпал с журналом | `status`, сверить префикс; apply не повторять вслепую |
| `schema_migration_follower_read_only` | Любая application-schema операция (status/plan/apply) на follower/replica | Выполнять на primary |
| `schema_migration_durability_unavailable` | При открытии: повреждение durable-журнала миграций, обнаруженное валидацией истории при старте — как отнесённое к базе, так и неатрибутируемое — прерывает открытие всего ShamirDb/сервера; код виден в диагностике старта, не как wire-ответ. В рантайме: тот же код от `status`/`plan`/`apply` при повреждении, маршрутизируемом на одну базу (конверт с пригодным ключом), при неатрибутируемом конверте (могут отказать все читатели), а также при обычных отказах чтения/записи/синхронизации хранилища — сам по себе он не означает повреждение | При открытии: полный restore `data_dir` из верифицированного консистентного backup (журнал, схема и данные восстанавливаются вместе) и повторный запуск; каталог вручную не править. В рантайме: не спешите с restore — диагностируйте доступность хранилища, затем заново прочитайте `status`; apply не повторять вслепую; при незавершённых durable-намерениях состояние может вместо этого быть in-doubt |
| `schema_migration_postcondition_failed` | Постусловие операции не подтвердилось | Вмешательство оператора: диагностика, restore при расхождении |
| `schema_migration_identity_mismatch` | Присланная целевая идентичность базы не совпала с обслуживаемой базой | Проверьте, в ту ли базу подключены (`reported_target`); import/restore смешанных состояний каталога |
| `schema_migration_operation_deferred:` | Операция отложена сервером (см. сообщение) | Читать серверное сообщение, действовать по нему |

### Старый сервер / новый клиент

Сервер без R1c отвечает отказом на неизвестную типизированную операцию —
клиент возвращает `MigrationError::UnsupportedServer { message }`:
обновите сервер. Отдельный случай: пир отвечает на `status`, но опускает
`applied`/`fence_generation` при непустой голове журнала — это тоже
`UnsupportedServer`, а не «пустой журнал». Никакого fallback на DDL без
журнала не существует ни на одном пути.

### Что добавит административный шелл (позже)

Шелл (release-1 R5, проект
[docs/dev-artifacts/design/admin-shell.md](../../dev-artifacts/design/admin-shell.md))
получит те же команды apply/status/check поверх этого протокола; R1c
поставляет только клиентские API.

## Поиск и отмена зависшего запроса

Откройте **второе аутентифицированное SCRAM-соединение**, даже если отменяете
запрос того же пользователя: занятое соединение не примет команду отмены.
В обработчике `CancelRequest` не занимает слот запроса, не расходует
байтовый бюджет ответа и не подпадает под заданный вызывающим лимит
результата.
При этом [транспортная диспетчеризация](../../../crates/net/shamir-connect/src/server/infra/dispatch.rs#L139-L152)
по-прежнему проверяет частоту запросов аутентифицированного сеанса до
вызова обработчика и при превышении возвращает `rate_limited`.
Для своих запросов достаточно аутентификации; для просмотра и отмены
запросов других пользователей нужно право `ActivityView:Manage` (для
просмотра используйте `ActivityScope::All`).

На управляющем соединении запросите список активных запросов. Сверьте базу,
сеанс, вид операции и время выполнения, прежде чем выбрать строку. Если
нужной строки нет на первой странице, передайте `page.continuation` в
следующий `ListActivityReq`. Для отмены берите серверный `request_exec_id`,
а не клиентский `rid`.

```rust
use shamir_client::{Client, ClientError};
use shamir_query_types::admin::activity::{
    ActivityKind, ActivityPage, ActivityRow, ActivityScope, CancelOutcome, ListActivityReq,
};

async fn list_own_requests(control: &Client, db: &str) -> Result<ActivityPage, ClientError> {
    control
        .list_activity(
            db,
            ListActivityReq {
                scope: ActivityScope::Own,
                kind: ActivityKind::Requests,
                limit: Some(64),
                continuation: None,
            },
        )
        .await
}

// selected — проверенная оператором строка из page.rows.
async fn cancel_selected_request(
    control: &Client,
    db: &str,
    selected: &ActivityRow,
) -> Result<Option<CancelOutcome>, ClientError> {
    match selected {
        ActivityRow::Request { request_exec_id, .. } => {
            let id = *request_exec_id;
            control.cancel_request(db, id).await.map(Some)
        }
        _ => Ok(None),
    }
}
```

| Исход `CancelRequest` | Что означает и что делать |
|---|---|
| `cancelled` | Отмена принята. Целевой запрос остановится в контрольной точке или проснётся из поддерживаемого ожидания. Его клиент получит обычную ошибку с кодом `cancelled`. |
| `already_finished` | Запрос уже завершился, был отменён ранее или идентификатор неизвестен. Проверьте ответ исходного запроса; этот исход не говорит, чем тот завершился. |
| `already_committing` | Запрос уже защищён от отмены: он вошёл в фазу фиксации или дошёл до запуска `Call` либо записывающей административной операции. Дождитесь результата исходного запроса. |
| `not_cancellable` | Найденный запрос не допускает отмены. `CancelRequest` его не остановит. |

Отмена кооперативная: сервер проверяет её в контрольных точках и может
разбудить запрос в следующих ожиданиях, пока запрос не защищён от отмены:

| Статус | Какое ожидание можно прервать |
|---|---|
| `waiting_lock` | Ожидание блокировки строки, мьютекса записи в уникальный индекс до фиксации или мьютекса воспроизведения журнала (drainer). |
| `waiting_visibility` | Ожидание подтверждения видимости версии при чтении. |
| `waiting_budget` | Ожидание свободной ёмкости байтового бюджета. |

В drainer прерывается только ожидание мьютекса. Если drainer уже получил
мьютекс и начал проход воспроизведения с изменением данных, он сначала
завершит этот проход: его асинхронная операция не прерывается. Правила
ожидания видимости и байтового бюджета не изменились.

Само наличие `Call` или записывающей административной операции в пакете
не защищает запрос от отмены при допуске. Непосредственно перед первым
фактическим запуском такой операции сервер выполняет тот же атомарный
переход (`CAS`) в `committing`, что и при входе в фазу долговечной фиксации.
Если отмена выиграла гонку, операция не начнётся; если выиграл переход в
`committing`, отмена вернёт `already_committing`. После перехода отмена не
включается заново до конца пакета, даже между операциями или фиксациями.
Уже выполняющийся вызов WASM принудительно не прерывается. Бюджет инструкций
(fuel) ограничивает код гостя, но не ограничивает время ввода-вывода хоста.

Интерактивные `TxBegin` и `TxExecute`, отдельная отмена транзакций
(`CancelTx`) и работа курсоров не входят в область действия `CancelRequest`.
У отдельной команды протокола `TxCommit` пока нет собственного дескриптора
в реестре запросов: `CancelRequest` её не отменяет, шаг 3 ещё не реализован.

На реплике (follower) применение потока репликации не регистрируется в
списке запросов и не отменяется. Незарегистрированный ID даст
`already_finished`, а не `not_cancellable`. Свои обычные пакеты на реплике
можно отменять по тем же правилам.

Если аудит включён, для отказов `already_committing` и `not_cancellable`
сервер пытается записать в аудит идентификатор целевого запроса и исход
попытки. На сервер допускается не более 256 незавершённых заданий такого
аудита. Ответ на отмену не ждёт свободного места или записи аудита и
возвращает тот же типизированный `CancelOutcome`, даже если задание
отброшено. При насыщении сервер учитывает отброшенные задания и пишет
предупреждение при первом отброшенном задании. Эпизод насыщения заканчивается,
когда остаётся меньше 256 незавершённых заданий. Новые задания после начала
остановки сервера также учитываются как отброшенные. Перед финальным
сбросом буферов аудита сервер ждёт завершения принятых заданий до 3 секунд;
если время истекло, пишет в журнал число ещё не завершённых заданий.
Ответ на отмену не подтверждает долговечную запись аудита.

## Куда дальше

||| Упёрся в… | Поднимайся на |
||---|---|---|
|| «нужна децентрализация / P2P» | [Этаж 8 — Interconnect](./08-interconnect.md) |

### `[transactions]` — visibility policy

| Setting | Default | Meaning |
|---|---:|---|
| `visibility_ack_mode` | `"strict"` | Strict by default; `"bounded"` is accepted at startup and returns committed receipts |
| `visibility_ack_timeout_ms` | `5000` | Default Bounded commit budget; no effect on Strict. At most `max_visibility_wait_ms` |
| `max_visibility_wait_ms` | `30000` | Maximum explicit Bounded commit/reconciliation wait, including nested overrides; excessive requests fail before writes, never clamp |
| `allow_strict_visibility_wait` | `true` | Permit explicit Strict commit/reconciliation waits; does not forbid the configured Strict default when the commit override is absent |
| `read_visibility_timeout_ms` | `5000` | Allocated-version AsOf wait, capped by remaining execution budget |

All millisecond values accept zero (immediate predicate check, never infinity)
and must fit `0..=4294967295`. Strict waits intentionally retain indefinite-wait
risk. Bounded applies to explicit and implicit DML batches, including nested
Batch/ForEach and multi-repository batches. Inspect every write alias's `receipt`,
even when row results are suppressed. Receipt-less Call/admin writes fail closed.
For connection-resource liveness, prefer Bounded and handle Pending as committed,
not a retry signal. Dependent read aliases report `visibility_dependency_pending`
in their `value`, with the awaited position and `skipped: true`, without executing
the read. Data-flow consumers also stop; unrelated operations inside nested
bodies and across ForEach iterations continue. Same-connection order is not a fence.
The budget excludes inner commit (including asynchronous index-visibility durability-error tail
awaits), admission and network backpressure; post-ack soft pressure is not a
hard memory admission guarantee. Reconcile Pending with WaitForVisibility and
open a new snapshot for reads. Materialization is independent of visibility.
Protocol v4 gating is current; query-v3 requests are rejected. See [PR1c migration](./PR1C-MIGRATION.md). Supported SDK adoption remains VIS-5 work.
Ktav uses `transactions: { visibility_ack_mode: strict ... }` with the same keys.

VIS-4c exposes this policy as metrics: acknowledgement duration histograms
(`shamir_visibility_ack_wait_seconds_*`, labelled `mode` × `result`), pending
counters by `reason` and write `kind`, AsOf read-timeout and
`WaitForVisibility` reconciliation counters, and the `shamir_visibility_ack_waiters`
gauge — see `/metrics` above. Structured WARN logs fire at every wait expiry,
rate-limited per repository. `shamir_tx_committed_total` keeps counting
commits: Pending acknowledgement is a committed outcome, not an abort.
