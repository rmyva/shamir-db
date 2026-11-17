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

Версии, ожидающие GC, уже экспортируются как `shamir_gate_pending_versions` —
отдельная серия `shamir_gc_pending_versions` не создаётся.

`process_*` опрашиваются каждые 5 секунд (crate `metrics-process`).

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

Одноразовый snapshot всего `data_dir` — модель **stop-and-copy** (v1):
**сервер должен быть остановлен перед запуском `backup`**, онлайн/живого
режима резервного копирования сейчас нет.

```bash
# 1) Остановить сервер (systemctl stop shamir-server / SIGTERM — дождаться graceful shutdown)
# 2) Запустить backup — data_dir берётся из --config, снимок пишется в <to>/<UTC-timestamp>/
shamir-server --config db.ktav backup --to /backup/shamir
# 3) Перезапустить сервер
```

Команда рекурсивно копирует **весь** `data_dir` (хранилище, WAL, реестры таблиц,
audit-лог и его ротации, TLS PEM) в каталог `<to>/YYYYMMDD_HHMMSS/`; существующий
целевой каталог перезаписываться отказывается. Источник (`data_dir`) задаётся
файлом `--config`, отдельного флага источника нет; реальная точка входа —
`crates/server/shamir-server/src/main.rs` (`Subcmd::Backup { to }` → `backup::backup`).

> ⚠ **Нельзя запускать `backup` против живого (запущенного) сервера.**
> Никакой блокировки или quiesce-механизма нет: фразы вроде «блокирует данные
> на время snapshot» в старых версиях этого руководства были **неверны**. Защита
> от torn-write здесь — **только** отсутствие конкурирующего writer'а благодаря
> остановленному серверу. Копия, снятая на фоне работающего сервера (mid-WAL-write
> / mid-SST-flush), может быть повреждена и **молчаливо непригодна к
> восстановлению** — дефект всплывёт только при restore.

In-process online-snapshot без downtime — запланированное улучшение **P2**,
пока не реализовано
(см. комментарий-модуля `crates/server/shamir-server/src/backup.rs`, «Future enhancement
(P2)»).

**Scheduled backup** — cron / systemd timer, но задание **обязано** сначала
остановить сервер, затем снять копию, затем поднять его обратно. Пример обёртки
для systemd: один `.service` с `ExecStartPre=systemctl stop shamir-server`,
`ExecStart=shamir-server ... backup --to ...`, `ExecStopPost=systemctl start
shamir-server`; либо wrapper-скрипт `stop → backup → restart`. Наивный cron,
запускающий `backup` против всё ещё живого сервера, — в точности тот риск,
которого избегает это правило.

**Restore.** `shamir-server --config db.ktav restore --from <to>/<YYYYMMDD_HHMMSS>`
— только при остановленном сервере (проверяется блокировка `data_dir`;
`--force` её пропускает). Команда сверяет `manifest.json` (sha256 каждого
файла), атомарно подменяет `data_dir` (прежний сохраняется как
`.pre_restore_backup_<timestamp>`) и инвалидирует все resumption-тикеты.
Счётчики lockout не сбрасываются. Не восстанавливай простым `cp -r`: так
пропускаются проверка манифеста и инвалидация тикетов.

**Point-in-time recovery нет:** восстановление возможно только к моменту снимка.

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

Если обновление на месте невозможно, предусмотренный для R3 запасной путь —
логический export старым бинарником и import новым. Этот fallback относится
к R3 и здесь пока не предоставлен.

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
