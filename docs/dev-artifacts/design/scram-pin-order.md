# Дизайн: проверка pinned-идентичности сервера ДО отправки client_proof (SCRAM-PIN-ORDER)

Задача: #1988. Дата: 2026-11-26. Раунд 3 (последний исполнительный): один
wire-формат (§4.1), один rollout (§7), одно identity-состояние на соединение
(§3.2), ровно две гарантии (§2.3/§2.5). Раунды 1-2 COMMITTED
(`3d11ed105` — состояние раунда 2). Статус: дизайн; независимое ревью
проведено (раунд 4, §11: 6 P1 и 5 P2 учтены в тексте); шаг 1 (§7) —
аддитивный и может идти до шага 2, провод подключается только после
этого ревью.

## 0. Резюме

Сегодня клиент отправляет SCRAM `client_proof` **раньше**, чем проверяет
pinned-идентичность сервера: pin (`SHA256(server_pub_key)`) проверяется только
в `process_auth_ok` после того, как proof ушёл в сеть
(`crates/net/shamir-connect/src/client/handshake.rs:262-277`), а публичный ключ
сервера физически появляется на проводе только в `auth_ok`
(`crates/net/shamir-connect/src/server/auth/handshake.rs:74-105`).

Решение: сервер доказывает идентичность **в самом challenge** — подписью
Ed25519 над транскриптом challenge (оба nonce, salt, KDF,
transport/binding, exporter); клиент проверяет pin и подпись **до** запуска
Argon2id и до вычисления proof. Раскладка challenge — **позиционный массив
ровно из 9 элементов** (единственное нормативное определение — §4.1);
расширение идёт вместе с **бампом handshake-версии до 2**. Rollout (§7):
сервер говорит на двух версиях во время перехода, защита от downgrade —
клиентская: v2-клиент не имеет кодового пути к старому потоку.
Rotation (§3.2): одно identity-сnapshot'а на соединение, ин-банд recovery
сохранён, переиспользуется существующая цепочка
`build_rotation_proof_input` (`crates/net/shamir-connect/src/server/ops/rotation.rs:318-320`).

Нормативные гарантии — ровно две (§2.3): **G1** (аутентификация
подписанта; все пять SCRAM-профилей, но только для клиента с НАСТРОЕННЫМ
и ПЕРЕДАННЫМ pin) и **G2** (peer authentication; только TCP+TLS и native
WSS с реальным локальным TLS-exporter'ом, binding 0x01). Прецедент «доказать идентичность до раскрытия секрета»:
exporter-bound resume-путь (`resume_proof_message`,
`crates/net/shamir-connect/src/common/user/identity.rs:56-66`,
RV-CLIENT-DE75ADDD).

---

## 1. Текущий поток, сообщение за сообщением

### 1.1. Формат сообщений и общие поля

Полный SCRAM идёт четырьмя фреймами поверх транспортного фреймера
(`shamir_transport_tcp::framing::{read_frame, write_frame}`, вызовы в
`crates/client/shamir-client/src/client/handshake/scram.rs:71,76,129,134`).
Размер pre-auth фрейма ограничен `MAX_PRE_AUTH_FRAME` (использование:
`scram.rs:8`; серверный комментарий HIGH-1:
`crates/server/shamir-sync/src/sync/connection/handshake.rs:162-164`).

Wire-структуры клиента (`crates/client/shamir-client/src/client/handshake/wire.rs`):

- `WireAuthInit` (стр. 7-14): `user`, `client_nonce[32]`, `binding_mode u8`, `version u8`.
- `WireChallenge` (стр. 17-26): `salt[16]`, `memory_kb u32`, `time u32`,
  `parallelism u32`, `argon2_version u8`, `server_nonce[32]`.
  **Поля идентичности сервера в challenge НЕТ.**
- `WireClientProof` (стр. 29-32): `client_proof[32]`.
- `WireAuthOk` (стр. 80-105): `server_signature`, `server_pub_key`,
  `identity_sig`, `session_id`, `expires_at_ns` + опциональные поля.

**Раскладка challenge на проводе — позиционный массив, НЕ именованный map.**
Сервер кодирует `wire::Challenge` через `rmp_serde::to_vec` → array из 6
элементов в порядке объявления полей:
`[salt, memory_kb, time, parallelism, argon2_version, server_nonce]`
(кодирование: `crates/server/shamir-sync/src/sync/connection/handshake.rs:264-283`).
TypeScript-клиент читает по индексам:
`CH_SALT=0, CH_MEMORY_KB=1, CH_TIME=2, CH_PARALLELISM=3, CH_ARGON2_VERSION=4,
CH_SERVER_NONCE=5` (`crates/client/shamir-client-ts/src/core/protocol/protocol.ts:38-43,144-160`).
Нормативное определение v2 — ровно одно место, §4.1.

Домен подписи идентичности (`crates/net/shamir-connect/src/common/user/identity.rs:26-54`):

```text
identity_input = "SHAMIR-IDENTITY-v1"      // IDENTITY_V1
              || SHA256(server_pub_key)
              || u8(transport_kind) || u8(binding_mode)
              || tls_exporter_or_zeros(32)
              || auth_message
              || session_id(32) || u64_be(expires_at_ns)
```

`auth_message` (`crates/net/shamir-connect/src/common/messages/auth_message.rs`;
TS-зеркало `crates/client/shamir-client-ts/src/core/protocol/scram.ts:129-170`)
конкатенирует username, оба nonce, salt, KDF, transport/binding, exporter и
версию протокола.

### 1.2. Шаги полного SCRAM (все SCRAM-транспорты одинаковы)

| # | Кто → кому | Сообщение | Файл:строки (клиент) | Файл:строки (сервер) | Что клиент уже проверил |
|---|---|---|---|---|---|
| 0 | — | TCP/TLS-хендшейк, (WSS: upgrade), IPC-коннект | `dial_*` в `crates/client/shamir-client/src/client/transport/` | `accept_loops.rs:129,197,262,380,488` | native TCP+TLS: приложение-аутентифицированный verifier — CA-chain и hostname **не** проверяются, идентичность привязана к exporter-SCRAM (`dial.rs:38-54`; `shamir-transport-tcp/src/tls.rs:191-202`, комментарий «Intentionally skip CA-chain and hostname validation»); browser-WSS: TLS-сертификат против `opts.trust` ДО SCRAM (`entry_ws.rs:183-190`); IPC/plain: ничего |
| 1 | клиент → сервер | `auth_init` | `scram.rs:63-73`; `handshake.rs:187-194` | decode `sync/handshake.rs:165-192`; версия `:172-179`; `ServerHandshake::new` `shamir-connect/server/auth/handshake.rs:123-173` | ничего (builder требует pin или `accept_new_host`, `handshake.rs:149-154`) |
| 2 | сервер → клиент | `challenge` (6-элементный массив) | чтение `scram.rs:76-102`; `process_challenge` `handshake.rs:203-242` | `hs.challenge()` `shamir-connect/server/auth/handshake.rs:186-196`; запись `sync/handshake.rs:264-283` | KDF-лимиты (`scram.rs:99-102`); **pin НЕ проверяется — ключа сервера ещё нет** |
| 3 | клиент (локально) | Argon2id + proof | `spawn_blocking` `scram.rs:104-123`; `common/primitives/scram.rs:51-77` | — | — |
| 4 | клиент → сервер | `client_proof` | `scram.rs:125-131` | чтение `sync/handshake.rs:285-310` | **proof уже на проводе** |
| 5 | сервер → клиент | `auth_ok` | чтение `scram.rs:133-138`; `process_auth_ok` `handshake.rs:252-308` | `verify_proof` `shamir-connect/server/auth/handshake.rs:204-259`; ветки `sync/handshake.rs:349-412` | (1) SCRAM mutual auth `handshake.rs:262-266`; (2) **pin** `handshake.rs:268-277`; (3) Ed25519 strict `handshake.rs:279-295`; (4) TOFU-захват `handshake.rs:297-302` |

Ключевой факт, подтверждённый кодом: pin проверяется на шаге 5, **после**
отправки proof на шаге 4, на всех SCRAM-транспортах. Все SCRAM-пути клиента
ведут в один обмен `run_scram` (`scram.rs:43-145`; вызовы: `entry.rs:182,217`,
`entry_ws.rs:120`).

### 1.3. Транспорты, несущие полный SCRAM

Перечисление путей приёма — `AcceptPath` в
`crates/server/shamir-server/src/engine/server/server_launcher/support/helpers.rs:109-122`:

1. **TCP + TLS, `binding_mode = 0x01` (tls_exporter)** — `AcceptPath::TcpTlsExporter`;
   exporter передаётся в `handle_connection` (`accept_loops.rs:129,380,488`).
2. **TCP plain (loopback-only), `binding_mode = 0x00`** — `AcceptPath::TcpPlain`;
   exporter = 32 нуля (`accept_loops.rs:197`).
3. **IPC: Unix domain socket / Windows named pipe, `binding_mode = 0x00`** —
   `handle_connection(ctx, IPC_PEER_ADDR, framer, [0u8; 32])`
   (`accept_loops.rs:262`); один транспортный тег `TransportKind::Unix` на обе
   ОС (`scram.ts:28-31`). TLS нет вообще.
4. **WebSocket поверх TLS (нативный WSS), `binding_mode = 0x01`** —
   `AcceptPath::WsNative` (`accept_loops.rs:269,380`).
5. **WSS browser-профиль, `binding_mode = 0x02` (tls_no_export)** —
   `AcceptPath::WsBrowser` (`accept_loops.rs:488`); exporter принудительно
   нули (`BROWSER_CHANNEL_BINDING`, `accept_loops.rs:467-468`); до SCRAM
   клиент проверяет TLS-сертификат против `opts.trust` (`entry_ws.rs:183-190`).

### 1.4. Пути БЕЗ полного SCRAM-пина-до-proof и вне скоупа

- **HTTP password-to-token login** (`/shamir/v1/auth/{begin,finish}`) —
  **отдельный протокол** со своим proof: клиент вычисляет password-derived
  `client_proof` и отправляет его в finish-запросе
  (`docs/guide-docs/client-server-protocol-spec/TOKEN_LOGIN_HTTP.md:50`,
  key schedule `:99`). Вне скоупа не потому, что proof не существует, а
  потому что это другой wire-протокол. Его pin-история: идентификация хоста —
  WebPKI TLS (cert/hostname), опциональный **out-of-band listener pin**
  обязан совпасть, если сконфигурирован; listener_id связывает transcript,
  но не идентичность; **live relay остаётся возможным**
  (`TOKEN_LOGIN_HTTP.md:42,194`) — здесь не меняется.
- **Native WSS-token** (`GET /shamir/v1/ws`) — тот же token-протокол.
- **Session resumption (ticket resume)** — не полный SCRAM. Preamble
  (`WireResumeProofInit` → `WireResumeProofOk`, подпись
  `RESUME_PROOF_V1 || client_nonce || exporter`,
  `crates/client/shamir-client/src/client/handshake/wire.rs:44-61`,
  `identity.rs:56-66`) есть только на exporter-bound путях: TCP+TLS resume
  (`resume.rs:136`) и native WSS resume (`entry_ws.rs:149-162`).
  **Browser-profile WSS resume, plain-TCP resume и IPC resume отправляют
  билет первым фреймом без preamble** (`entry_ws.rs:223-226`, `:270-288`
  `resume_plain` → `resume_wire_exchange_unbound`, `:310-314`). Существующее
  состояние; этот документ его не меняет (зафиксировано как_known,
  отдельный task, вне 5 шагов §7).

### 1.5. Серверная сторона подключения

Полный поток — `run_handshake`
(`crates/server/shamir-sync/src/sync/connection/handshake.rs:154-…`):
decode `auth_init` (165-192), версия (172), lockout pre-check (198-219),
`ServerHandshake::new` (239-250), challenge write (264-283), proof read
(285-310), `verify_proof` (349-359), ветки (361-412). `handle_connection`
живёт в этом же крейте и переиспользуется всеми accept-петлями
(`crates/server/shamir-server/src/lib.rs:31,42`).

Identity-ключ сервера: `ServerIdentityState` + `Ed25519Keypair` в
`build_ctx` (`helpers.rs:143-189`, стр. 163). `identity_sig` для `auth_ok`
вычисляется в `verify_proof` безусловно, даже при неверном proof
(`shamir-connect/server/auth/handshake.rs:219-241`). **Финальная подпись
остаётся; этот дизайн ДОБАВЛЯЕТ подпись challenge** (§3.1) — никакой
«перенос» не подразумевается. На соединение сервер видит два носителя ключа:
boot-time keypair copy в `ConnectionContext`
(`crates/server/shamir-sync/src/sync/connection/session/connection_context.rs:82-94`)
и живое `ServerIdentityState`, которое читает
`rotation_in_progress_payload` и может вернуть `None` вне окна
(`shamir-connect/server/auth/handshake.rs:261-281`). Что меняется для
единого snapshot'а — §3.2, шаг 2 rollout'а.

### 1.6. Rotation-инфраструктура сервера (для §3.2)

`crates/net/shamir-connect/src/server/ops/rotation.rs`:
`ServerIdentityState` хранит current + previous + `rotation_until_ns`
(`:203-228`, `try_finalize`), `RotationOutcome { old_pub, new_pub,
transition_until_ns, new_version }` (`:231-244`). Подписанные конструкты от
**previous**-ключа: `build_identity_rotation_event` (`:251-281`) и
`build_rotation_in_progress_payload` (`:301-328`) — `identity_sig_previous`
над byte-exact `identity_input` и `rotation_proof` над
`(previous_pub, current_pub, transition_until_ns)` (`build_rotation_proof_input`,
стр. 318-320). Wire-форма: `RotationInProgressPayload` / `RotationInProgressWire`
(`crates/net/shamir-connect/src/common/messages/wire.rs:133-200`).
Клиентская обработка: `verify_rotation_in_progress`
(`crates/net/shamir-connect/src/client/rotation.rs:107-158`, окно `:145-151`)
и путь `ServerIdentityChanged` + `rotation_in_progress` →
`verify_rotation_recovery` → `IdentityRotationPending`
(`crates/client/shamir-client/src/client/handshake/auth_ok.rs:67-91`).

---

## 2. Формулировка угрозы

### 2.1. Что роуг-эндпоинт узнаёт СЕГОДНЯ до какой-либо проверки идентичности

Завершив транспортный хендшейк и отправив challenge, атакующий получает:

- `client_proof[32]` — `client_key XOR HMAC(stored_key, auth_message)`
  (`common/primitives/scram.rs:70-77`) — **байты, производные от пароля**;
- `salt[16]` и полный набор KDF-параметров (реальных или fake-blob —
  неотличимо и неважно);
- оба nonce, username, transport/binding, exporter **своего** соединения —
  полный `auth_message`.

TLS-exporter binding привязывает proof к соединению; атакующий атакует
**своё** соединение: `auth_message` известен, проверка кандидата локальна.

### 2.2. Стоимость офлайн-атаки при параметрах, которые клиент принимает СЕГОДНЯ

Floor, который сегодня принимает клиент: `KDF_MIN_MEMORY_KB = 19_456` КиБ
(19 МиБ), `KDF_MIN_TIME = 2`, `KDF_MIN_PARALLELISM = 1`
(`crates/net/shamir-connect/src/common/model/types.rs:130-134`); те же
нижние границы в TS-валидаторе (`scram.ts:58-67`). Честные серверные
дефолты выше — `131_072` КиБ / 4 / 1 (`kdf_params.rs:24-29`), но
роуг-сервер не обязан их соблюдать.

Проверка одного кандидата: один Argon2id на 19 МиБ × 2 прохода → HMAC →
SHA256 → сравнение воспроизведённого proof с перехваченным.
**Арифметическое** (не хронометрическое) сравнение с дефолтами:
`19_456 × 2 = 38_912` против `131_072 × 4 = 524_288` единиц
память×проходы — примерно **в 13.5 раза дешевле**. Комментарии кода
«~50ms-2s» относятся к дефолтам (`handshake.rs:235`, `scram.rs:104-105`) и
на floor не переносятся. Атака офлайн и параллелизуемо; утверждения о
конкретном GPU/ASIC-ускорении не делаются. Salt в текущем challenge не
аутентифицирован — роуг может выдать один salt всем жертвам; подпись в
варианте A покрывает salt и KDF и закрывает это.

### 2.3. Нормативные гарантии — РОВНО ДВЕ

> **G1 (signer authentication; все транспорты).** Клиент не отправляет ни
> одного байта, производного от пароля, если challenge не несёт валидной
> подписи — по verify_strict — над транскриптом, содержащим nonces этого
> соединения, salt и KDF-параметры, сделанной ключом, который либо
> напрямую хешируется в pin клиента, либо авторизован верифицированной
> rotation-цепочкой от pinned-ключа (§3.2).
>
> **G2 (peer authentication; только binding 0x01, реальный exporter в
> транскрипте).** Подписант является также непосредственным TLS-peer'ом
> клиента, поэтому relay не может переиспользовать ответ.
>
> Для plain TCP, IPC и browser-profile WSS **G2 НЕ выполняется**: live
> relay между клиентом и настоящим сервером может переслать настоящий
> подписанный challenge и получить proof. На этих транспортах изменение
> даёт только G1.

Любая формулировка, утверждающая больше, — ошибка документа. G1 не
утверждает, что ответ получен от непосредственного peer'а; G2 не
утверждается там, где exporter нулевой.

### 2.4. Честные границы: что изменение НЕ закрывает

- **TOFU (`accept_new_host = true`)** — новые поля обязательны и для
  TOFU-клиентов (§4.2): подпись проверяется всегда, pin предлагается к
  сохранению только из challenge-ключа. Роуг-сервер, ответивший первым,
  станет «пин-ed» — фундаментальное ограничение TOFU.
- **HTTP-token и WSS-token пути** — отдельные протоколы (§1.4).
- **Атакующий = настоящий сервер** — получает всё легитимно.
- **Компрометация `previous_priv`** — ограничения `AUTH_PROTOCOL.md §6.5`
  остаются; rotation-цепочка наследует те же допущения.
- **Compromised client machine** — вне модели угроз транспорта.

### 2.5. Гарантии по транспортам (НОРМАТИВНАЯ таблица, согласована с §2.3)

| Транспорт | binding | exporter | G1 signer auth | G2 peer auth | Что relay/rogue всё ещё может |
|---|---|---|---|---|---|
| TCP+TLS | 0x01 | реальный | да | **да** | ничего от пароля; DoS |
| Native WSS | 0x01 | реальный | да | **да** | ничего от пароля; DoS |
| TCP plain | 0x00 | нули | да | **нет** | live relay: переслать настоящий challenge настоящему серверу и получить валидный proof (офлайн-атака против настоящих salt/KDF); loopback-политика ограничивает доступ |
| IPC (unix/pipe) | 0x00 | нули | да | **нет** | то же; доверие = OS-граница хоста (`TRANSPORT_UNIX.md`) |
| Browser WSS | 0x02 | нули | да | **нет** | то же; TLS-terminating proxy может ретранслировать. Проверка TLS-сертификата — отдельный слой, разный у трёх клиентов и к G2 не относится: Rust browser-профиль сверяет сертификат с `opts.trust` до SCRAM (`entry_ws.rs:183-189,206-220`); TS в браузере — политика сертификатов браузера/ОС; TS на Node — `rejectUnauthorized` (по умолчанию true, можно отключить; `platform/node.ts:124-131`) |
| HTTP/WSS token | — | — | вне скоупа | нет (`TOKEN_LOGIN_HTTP.md:42,194`) | как сегодня |

**Область таблицы.** Строки «да» в столбце G1 и формулировки «ничего от
пароля» относятся к клиенту с НАСТРОЕННЫМ pin (прямым или через
rotation-цепочку). Клиент без pin (trust-on-first-use, §2.4) на первом
соединении не получает G1 ни на одном транспорте: он принимает ключ из
challenge и предлагает его к сохранению; защита начинается со второго
соединения и только если сохранённый pin затем ПЕРЕДАН клиенту (настроен).
Привязка к exporter'у на первом соединении лишь связывает подпись
предъявленного ключа с этим каналом; доверенным ключ от этого не
становится.

Механика G2 (направление: подписывает сервер, проверяет клиент). Сервер
подписывает транскрипт с exporter'ом **своего** TLS-соединения (с relay);
клиент проверяет подпись против exporter'а **своего** соединения (тоже с
relay). Соединения разные, exporter'ы разные: пересланный настоящий
challenge не проходит проверку на клиенте → proof не уходит. На 0x00/0x02
exporter нулевой с обеих сторон, транскрипт совпадает → relay пересылает
настоящий challenge, клиент проверяет его успешно и proof уходит
настоящему серверу (только G1).

---

## 3. Варианты и выбор

### 3.1. Вариант A — сервер доказывает идентичность в challenge (ВЫБРАН)

**Сообщение.** Challenge v2 — позиционный массив **ровно из 9 элементов**
(§4.1): к шести существующим добавляются `server_pub_key`, `challenge_sig`
и `challenge_rotation`.

**Байт-раскладка подписи** (новый домен-тег `SHAMIR-CHALLENGE-ID-v1`):

```text
challenge_identity_input =
    "SHAMIR-CHALLENGE-ID-v1"
 || SHA256(server_pub_key)          // 32 — ключ входит хешем (зеркалит IDENTITY_V1)
 || client_nonce(32)                // привязка к соединению, anti-replay
 || server_nonce(32)
 || salt(16)                        // ПО РЕШЕНИЮ ВЛАДЕЛЬЦА: salt в транскрипте
 || u32_be(memory_kb) || u32_be(time) || u32_be(parallelism) || u8(argon2_version)
 || u8(transport_kind) || u8(binding_mode)
 || tls_exporter_or_zeros(32)
```

Сервер подписывает в `challenge()`/новом `challenge_signed()`
(`shamir-connect/server/auth/handshake.rs:186-196`) через `sign_identity`
(`common/user/identity.rs:69-71`). **Стоимость**: ДОБАВЛЕННАЯ подпись —
`identity_sig` для `auth_ok` остаётся и по-прежнему вычисляется в
`verify_proof` безусловно (`shamir-connect/server/auth/handshake.rs:227-241`).
Итого +1 Ed25519 на хендшейк (µs на фоне Argon2id-этапа), +0 round trips.

**Wire-рост** (арифметика MessagePack для энкодера, который реально
пишет этот код, не замер): `rmp_serde` пишет `#[serde(with = "serde_bytes")]`
поля через `write_bin` (`rmp-serde-1.3.1/src/encode.rs:653-654`), а он
выбирает `Bin8` для полезной нагрузки до 255 байт (`rmp-0.8.15/src/encode/bin.rs:15-30,41-44`):
маркер `0xC4` + u8-длина + данные. Базовый 6-элементный fixarray (маркер
`0x96`, 1 байт) превращается в `0x99`; рост заголовка 0 байт (обе формы —
1 байт). Новые элементы: 32-байтовый ключ = 2 + 32 = **34 байта**;
64-байтовая подпись = 2 + 64 = **66 байт**; элемент 8 вне окна (пустой bin) =
`0xC4 0x00` = **2 байта**. Итого вне rotation-окна: **+102 байта**. Внутри
окна элемент 8 = `0xC4` + u8-длина + `rmp_serde::to_vec(ChallengeRotationWire)`;
`ChallengeRotationWire` (§4.1) кодируется fixarray (1) + 32-байтовый ключ
(34) + u64 (энкодер выбирает наименьшую форму: для наносекундных значений
выше 2^32 это `0xCF` + 8 = 9 байт) + 64-байтовая подпись (66) = **110
байт**, элемент 8 = 2 + 110 = **112 байт**; итого внутри окна **+212 байт**.
Всё внутри `MAX_PRE_AUTH_FRAME`. Цифры привязаны к форме, которую выбирает
энкодер; декодер принимает любую валидную форму (§4.1).

**Проверка клиентом** — `verify_challenge_identity` рядом с
`process_challenge` (`shamir-connect/src/client/handshake.rs`), вызывается
из `run_scram` **между** чтением challenge (`scram.rs:76-102`) и созданием
`password_buf`/`spawn_blocking` (`scram.rs:109-112`): пароль ещё не
скопирован, Argon2id не начат. Правило подписанта (нормативное, детально
в §3.2): (1) verify_strict подписи; (2) либо
`constant_time_eq(sha256(server_pub_key), pinned_hash)`, либо валидная
rotation-цепочка; любая ошибка → `Error::ServerIdentityChanged`, disconnect.

Для TOFU (раунд 4, окончательно): подпись проверяется всегда; ДО proof
ключ challenge остаётся приватным для handshake — никакой callback и
никакая публикация pin до proof не происходит. Для КАЖДОГО v2-handshake
(не только TOFU) сразу после `auth_ok`, до любого recovery-разбора,
обязательно `auth_ok.server_pub_key == challenge.server_pub_key`;
расхождение терминально (`ServerIdentityChanged` без перехода в
recovery-ветку `auth_ok.rs:67-94`: pin mismatch с payload там считается
восстанавливаемым, а continuity-сбой — нет). Pin TOFU публикуется только
ПОСЛЕ успешного финального SCRAM и проверки финальной подписи идентичности
(сегодняшний инвариант Rust: неверная финальная подпись — ноль callback'ов,
`rv_handshake_0fc58d93_tofu_pin_after_identity_verify.rs:123-140`).
Проваленный handshake не публикует pin.

**В `auth_ok` ничего не убирается.**

**Downgrade-попытки.** (a) роуг шлёт 6-элементный challenge после v2 hello —
нарушение правила «ровно 9» → типизированная ошибка раскладки, proof не
отправляется; (b) чужой ключ — pin mismatch; (c) переигровка подписи —
подпись покрывает оба nonce; (d) подпись своим ключом — не равен pin и не
авторизован цепочкой; (e) версия — v2-клиент не имеет fallback-ветки (§4.2);
(f) relay — на 0x01 отказ клиента (G2), на 0x00/0x02 только G1 (§2.5).

**Версия в подписи challenge не входит (решение оркестратора, раунд 4).**
Подпись существует только в v2; её формат назван доменным тегом
`SHAMIR-CHALLENGE-ID-v1` (тег версионирует конструкцию, а не handshake;
будущая раскладка получит новый тег). Версия hello привязана иначе:
v2-клиент не принимает ничего, кроме 9 элементов (§4.1), и версия входит в
`auth_message` (§4.5), а значит в client proof, server signature и обе
финальные подписи идентичности. Подмена версии в hello (v2→v1) приводит
к 6-элементному challenge и отказу клиента, не к ослаблению. Поэтому
вектор `challenge_identity_v2` версию не содержит и не пересчитывается.

**Прецедент в коде**: exporter-bound resume (`wire.rs:44-61`,
`identity.rs:56-66`, RV-CLIENT-DE75ADDD).

### 3.2. Плановая rotation: ОДИН identity-снапшот на соединение, ин-банд recovery сохранён

**Проблема раунда 2.** Финальный verifier использует boot-time keypair copy
(`connection_context.rs:82-94`), а `rotation_in_progress_payload` читает
**живое** состояние и может вернуть `None` вне окна
(`shamir-connect/server/auth/handshake.rs:261-281`) — подпись challenge,
rotation-элемент и `auth_ok`-подпись могли бы разойтись через proof wait.

**Решение (решение владельца): сервер делает ОДИН identity-снапшот на
соединение в момент принятия hello** и использует этот единственный
snapshot для: (i) ключа подписи challenge, (ii) rotation-элемента
(challenge_rotation), (iii) ключа подписи `auth_ok.identity_sig` и (iv)
содержимого `auth_ok.rotation_in_progress`. Реализация (шаг 2 rollout'а,
§7):

- **Владение (раунд 4: владелец — `run_handshake`, не `ConnectionContext`).**
  `ConnectionContext` общий для всех соединений слушателя (его клонируют в
  accept-цикле, `accept_loops.rs:92-95`), поэтому ни поля snapshot'а, ни
  его создания там быть не может. Snapshot — значение, которое
  `run_handshake` создаёт один раз сразу после принятия полного
  SCRAM-hello (рядом с чтением первой рамки, `sync/handshake.rs:766+`) и
  передаёт по ссылке в `challenge_signed`, `verify_proof`,
  `rotation_in_progress_payload` И в `auth_ok_hints`
  (`session/scram/auth_ok_hints.rs:6-21`). Из общего состояния идентичности
  он берёт копию под одним чтением (одно атомарное чтение снимка
  состояния, не несколько независимых). Подписывающие способности живут
  только на время handshake и отбрасываются вместе со snapshot'ом после
  `auth_ok` (долго они не хранятся).
- **Начальный билет.** Сегодня выдача начального resume-билета читает
  версию ключа идентичности из ЖИВОГО состояния
  (`sync/handshake.rs:645-680`). Шаг 2 берёт её из snapshot'а: билет
  помечается версией ключа snapshot'а, и неизменный resume-гейт отвергает
  билет старого ключа.
- **Содержимое.** Всё, что нужно трём подписям и payload'у, вместе:
  `IdentitySnapshot { current_signer, previous_signer: Option<_>,
  transition_until_ns: Option<u64>, rotation_proof: Option<[u8;64]> }`.
  `current_signer` и `previous_signer` — способность подписывать
  (в процессе уже хранимая пара ключей или её подписывающий хэндл), не
  только публичный ключ: существующая финальная подпись
  `identity_sig_previous` и подпись цепочки делаются ПРИВАТНЫМ ключом
  предыдущей идентичности (`server/ops/rotation.rs:305-326`).
  `rotation_proof` подписывает `build_rotation_proof_input(previous_pub,
  current_pub, transition_until_ns)`, без данных соединения. **Сегодня
  готового proof нет**: подпись выполняется на каждый `auth_ok`
  (`server/ops/rotation.rs:301-326`). Шаг 2 вводит его кэш в ТОМ ЖЕ
  опубликованном состоянии идентичности, где лежат подписанты и дедлайн:
  proof строится при `rotate` И при rehydration состояния из сохранённого
  перекрытия (`server_meta.rs:550-562,764-780`, запуск
  `launch.rs:571-579`) и сбрасывается при финализации. Предыдущий
  ПРИВАТНЫЙ ключ при перекрытии доступен (`rotate` публикует старую пару
  как `previous`, сохранённое перекрытие хранит оба seed'а; путь
  restart-overlap e2e `auth_ok_hints_e2e.rs:96-137`). Граница проверки:
  живого вызова ротации в production-коде этого среза нет — доказательство
  даёт автомат состояний плюс путь сохранения/перезапуска.
- **Что меняется в существующем коде.** `identity_keypair_for_verify`
  (`connection_context.rs:86-94`, boot-time копия) и
  `rotation_in_progress_payload` (`shamir-connect/server/auth/handshake.rs:261-281`,
  проверка `state.rotation_in_progress(now_ns)`) читают snapshot, а не
  живое состояние и не boot-time копию. Вне окна payload ведёт себя как
  сегодня (`None`).
- **Окно истекло, пока шло ожидание proof.** Snapshot фиксирует окно как
  оно было при hello. Клиент проверяет границы окна дважды: до proof
  (правило ниже) и в `auth_ok` существующим кодом
  (`client/handshake/auth_ok.rs:67-94` снова берёт время). Исход
  отличается от сегодняшнего и фиксируется здесь: сегодня истёкшее
  серверное окно может опустить payload, и клиент получает
  `ServerIdentityChanged` (`server/auth/handshake.rs:261-281`); с
  замороженным payload существующая клиентская проверка дедлайна
  (`client/rotation.rs:145-151`) даёт `ServerSignatureInvalid`. Оба
  исхода фатальны; новых вариантов ошибок это не добавляет, оракул §6.3
  фиксирует именно `ServerSignatureInvalid`.

**Клиентское правило (нормативное, полное).** Пусть `K` — ключ подписи
challenge. Клиент принимает ровно один из двух исходов:
1. **Прямой pin**: `SHA256(K) == pinned_hash` → идентичность доказана;
2. **Rotation-цепочка**: `SHA256(K) != pinned_hash`, но элемент 8 содержит
   chain, где `previous_pub` удовлетворяет `SHA256(previous_pub) ==
   pinned_hash` И `verify_strict(previous_pub, rotation_proof)` валидна над
   `build_rotation_proof_input(previous_pub, K, transition_until_ns)` И
   deadline в окне — **те же две проверки, что использует существующий
   клиент**, цитата `crates/net/shamir-connect/src/client/rotation.rs:145-151`:
   ```rust
   // 4. Window sanity.
   if payload.transition_until_ns <= now_ns {
       return Err(Error::ServerSignatureInvalid);
   }
   if payload.transition_until_ns > now_ns.saturating_add(ROTATION_MAX_TRANSITION_NS) {
       return Err(Error::ServerSignatureInvalid);
   }
   ```
   (`ROTATION_MAX_TRANSITION_NS` — существующая константа верхнего предела;
   переиспользуется, не изобретается новая.)
Иначе — `Error::ServerIdentityChanged` **до** Argon2id и proof.

При исходе 2 клиент продолжает полный поток. Цепочка доказывает, что
текущий подписант авторизован ранее закреплённым ключом (G1); она не
доказывает, что получатель proof — непосредственный peer клиента: на
транспортах без exporter это по-прежнему только G1 (§2.3, §2.5). В `auth_ok` клиент получает
`rotation_in_progress` как сегодня → `verify_rotation_recovery` →
`IdentityRotationPending` (`auth_ok.rs:67-94`) без изменений. Принятие
цепочки в challenge — НЕ автоматическое восстановление; три исхода
сегодняшнего кода называются явно: (1) библиотека Rust возвращает
`IdentityRotationPending`; (2) проверенная в репозитории оболочка
отображает его в жёсткий отказ по pin
(`shell/connect/scram.rs:263-268`); (3) TypeScript и после принятия
цепочки в challenge отвергает старый pin в `auth_ok`
(`session/open.ts:157-164`). Политика «интерактивный prompt или
`--accept-rotation`» — текст спецификации `AUTH_PROTOCOL.md §6.5`, а не
поставляемый путь оболочки. Вне окна pin mismatch фатален до proof — как и
сегодня вне окна (orphan, пропустивший окно, делает out-of-band re-pin).

`identity_sig_previous` из auth_ok-payload в challenge **не переносится**:
он подписывает `identity_input`, содержащий `session_id`/`expires_at_ns`
(`shamir-connect/server/auth/handshake.rs:272-281`), которых на шаге
challenge ещё не существует.

### 3.3. Вариант B — «ключ уже доступен раньше auth_ok, меняем только порядок»

Опровергнут кодом: `ChallengeView` содержит ровно `salt, kdf_params,
server_nonce` (`shamir-connect/server/auth/handshake.rs:64-72`) — ключа
до `auth_ok` нет. Доставить ключ — это и есть вариант A. Спецификация:
pin-check после proof (`AUTH_PROTOCOL.md:333-338`, §11.2.4 стр. 602).

### 3.4. Вариант C — проверка на транспортном уровне

Отвергнут: (а) IPC и plain не имеют TLS — pin приложения единственная
проверка идентичности (`helpers.rs:112-116`,
`docs/dev-artifacts/design/admin-shell.md:156`); (б) browser-профиль уже
совмещает CA-корни с pin (`admin-shell.md:190-192`); (в) pin определён как
`SHA256(server_ed25519_pub)` в raw-кодировке (`AUTH_PROTOCOL.md:355`);
переопределение ломает профили оболочки
(`docs/guide-docs/guide/07-operations.md:3473-3475`).

### 3.5. Итог

A с rotation-цепочкой и снапшотом (§3.2): 0 round trips, прецедент,
закрытие salt-подмены, сохранённый recovery, G1/G2 без преувеличений.

---

## 4. Совместимость и downgrade

### 4.1. НОРМАТИВНАЯ раскладка challenge v2 (единственное определение; все
другие разделы ссылаются сюда)

**Было (v1, сегодня)** — array ровно из 6 элементов:

| индекс | поле | тип/длина |
|---|---|---|
| 0 | salt | bin 16 |
| 1 | memory_kb | uint (u32) |
| 2 | time | uint (u32) |
| 3 | parallelism | uint (u32) |
| 4 | argon2_version | uint (u8) |
| 5 | server_nonce | bin 32 |

**Стало (v2)** — array **ровно из 9 элементов**:

| индекс | имя | MessagePack тип и точная длина | кто производит | кто проверяет |
|---|---|---|---|---|
| 0 | salt | `bin` со значением 16 байт (форма маркера — любая валидная) | сервер из `ChallengeView.salt` | клиент: длина 16, входит в auth_message и в подпись |
| 1 | memory_kb | `uint` (значение u32) | сервер | клиент: валидация KDF-лимитов |
| 2 | time | `uint` (u32) | сервер | клиент |
| 3 | parallelism | `uint` (u32) | сервер | клиент |
| 4 | argon2_version | `uint` (u8) | сервер | клиент: `== 0x13` |
| 5 | server_nonce | `bin` со значением 32 байта (энкодер пишет `Bin8`: 34 байта) | сервер | клиент: длина 32 |
| 6 | server_pub_key | `bin` со значением 32 байта (34 байта) | сервер, из снапшота §3.2 | клиент: pin/chain-правило §3.2 |
| 7 | challenge_sig | `bin` со значением 64 байта (66 байт) | сервер, Ed25519 over §3.1 | клиент: `ed25519_verify_strict` |
| 8 | challenge_rotation | `bin`: значение длины 0 (пустой bin) вне rotation-окна; иначе значение — РОВНО одна полная внутренняя раскладка `ChallengeRotationWire` (ниже) без хвоста | сервер, из снапшота | клиент: форму проверяет ВСЕГДА, цепочку — только если прямой pin не совпал (§3.2) |

**Размеры 34/66/2 и 110/112 — только размеры, которые ПИШЕТ энкодер**
(`rmp_serde::to_vec`, `Bin8`; арифметика по исходникам энкодера, не замер),
для справки о росте кадра (§3.1). Декодер не требует ни этих байтовых
длин, ни наименьшей формы: «точная длина» в таблице — длина **значения**.

**Правила декодера (нормативные, одинаковые в Rust и TypeScript).**
1. Корень — именно MessagePack array (не map и не скаляр) ровно из 9
   элементов, и это ОДНА полная внешняя раскладка без байтов после неё.
2. Элементы 0, 5, 6, 7, 8 — bin (не array из чисел); принимается любой
   валидный маркер `bin8/bin16/bin32` для значения нужной длины (длины по
   таблице); более широкий маркер при том же значении принимается.
3. Элементы 1-4 — целые; принимаются маркеры positive fixint, `uint8`,
   `uint16`, `uint32`, `uint64` (0x00-0x7F, 0xCC-0xCF); знаковые маркеры
   (0xD0-0xD3, negative fixint) и нецелые типы — `ChallengeLayout`.
   Декодирование ориентировано на МАРКЕР: диапазон значения проверяется
   ДО сужения (элементы 1-3 ≤ `u32::MAX`, элемент 4 ≤ `u8::MAX`; иначе
   `ChallengeLayout`), и только потом значение превращается в `u32`/`u8`
   (в TypeScript — в `number`; декодер возвращает `bigint` для любого
   `uint64`-маркера, в том числе когда значение влезает в u32, и без
   проверки диапазона приводить его к `number` нельзя).
4. Непустой элемент 8 разбирается как ОДНА полная внутренняя тройка
   `ChallengeRotationWire` (ниже): ровно 3 элемента, байтов после неё нет.
   Форма внутренней раскладки проверяется и при ПРЯМОМ совпадении pin;
   цепочка (подпись, привязка к pin, окно) верифицируется только при
   несовпадении pin (§3.2).
5. `transition_until_ns` (uint64) остаётся 64-битным: в TypeScript — как
   `bigint` (путь `core/protocol/framing.ts:214-219`), с окном сравнивается
   в `bigint`, не через `number`; в Rust — `u64`.
6. Различаем три класса: НЕКОРРЕКТНАЯ раскладка (всё выше) —
   `ChallengeLayout` (§4.3), proof не отправляется; корректная раскладка с
   KDF вне политики клиента — существующая ошибка отказа KDF
   (`kdf_rejection_tests.rs`), не `ChallengeLayout`; корректная раскладка с
   недействительной цепочкой/подписью — `ServerIdentityChanged`.
7. Сегодняшние декодеры этим требованиям НЕ удовлетворяют: Rust
   декодирует структуру через `rmp_serde` (не валидатор «только
   array/bin», `rmp-serde-1.3.1/src/decode.rs:558-605,1171-1187`,
   `serde_bytes-0.11.19/src/de.rs:153-185`), TS приводит корень и слоты KDF
   приведением типов (`protocol.ts:147-160`). Шаги 3 и 4 §7 пишут
   явный marker-aware валидатор в каждом языке.
8. **Общие побайтовые вектора принятия и отказа** (генерирует reference
   шага 1, лежат рядом с `challenge_identity_v2.*`): ПРИНЯТЬ — более
   широкие маркеры для bin и uint при тех же значениях, `uint64`-маркер для
   KDF-значения, влезающего в u32, дедлайн с маркером любой ширины;
   ОТКАЗАТЬ (`ChallengeLayout`) — корень map; 8 и 10 элементов; array из
   чисел вместо bin; знаковый маркер; KDF-значение > `u32::MAX`; хвост
   после внешнего array; хвост после внутренней тройки; внутренний array
   не из 3 элементов; bin неверной длины.

**`ChallengeRotationWire`** — новая структура, позиционная
(`rmp_serde::to_vec` → array ровно из 3 элементов), поля переиспользуют
точные типы существующего `RotationInProgressPayload`
(`crates/net/shamir-connect/src/common/messages/wire.rs:133-160`),
ничего нового не изобретено; `identity_sig_previous` исключён, т.к.
подписывает `identity_input` с `session_id`/`expires_at_ns`, которых на
шаге challenge нет (`shamir-connect/server/auth/handshake.rs:272-281`):

| внутренний индекс | поле | тип/длина | источник |
|---|---|---|---|
| 0 | previous_pub | `bin` значением 32 байта | `RotationInProgressPayload.previous_pub` (`wire.rs:145-148`) |
| 1 | transition_until_ns | `uint` (u64, значение из `RotationInProgressPayload.transition_until_ns`) | `ops/rotation.rs:310-312` |
| 2 | rotation_proof | `bin` значением 64 байта | Ed25519 by PREVIOUS priv over `build_rotation_proof_input(previous_pub, current_pub, transition_until_ns)` — **тот же конструкт**, `ops/rotation.rs:318-320`; подпись не зависит от соединения, поэтому может быть вычислена один раз при старте окна и храниться в состоянии идентичности (§3.2) |

Внутренний размер при наличии окна: fixarray (1) + 32-байтовый ключ (34) +
u64 в форме `0xCF` (9) + 64-байтовая подпись (66) = **110 байт** — размер,
который ПИШЕТ энкодер для наносекундных значений выше 2^32, а не
требование к декодеру (правила 2-4 выше). Внутренний массив — ровно 3
элемента: другая длина или тип — `ChallengeLayout`.

**Правило точной длины (нормативное).** Challenge array в v2 обязан иметь
**ровно 9 элементов**. Меньше или больше — типизированная ошибка протокола
на клиенте (§4.3, `ChallengeLayout`), proof не отправляется. Rust и
TypeScript применяют правило одинаково: Rust — явная проверка длины
декодированного массива/структуры до `process_challenge`; TypeScript —
явная проверка `chArr.length === 9` в `runHandshake` перед чтением
индексов. (Фон: `rmp-serde 1.3.1` сам даёт `LengthMismatch` на лишних
элементах структуры — `decode.rs:566-574` — но правило фиксируется явно,
не как побочный эффект энкодера.)

**Общий фиксчур-вектор: правило генерации** (не рукописный hex): фиксированные
входы `{domain_tag="SHAMIR-CHALLENGE-ID-v1", client_nonce=0xAA×32,
server_nonce=0xBB×32, salt=0xCC×16, kdf={19456,2,1,0x13}, transport=0x01,
binding=0x01, exporter=0x00×32, pub/priv — фиксированная тестовая Ed25519
пара}` (зеркалит константы `protocol.ts:41-43`); reference-реализация Rust
вычисляет `challenge_identity_input` и подпись ОДИН раз; hex транскрипта и
подписи записывается в `crates/net/shamir-connect/test-vectors/challenge_identity_v2.toml`
и `.json`. TS-тест сверяет оба значения. Vector — артефакт шага 1 (§7),
не этого документа.

### 4.2. Политика (решения владельца)

Новые поля **обязательны безусловно**, TOFU включительно; **fallback на
старый поток у v2-клиента отсутствует** — ни ветки, ни флага; защита от
downgrade — **клиентская** (§7, шаг 2 держит v1 на сервере, но v2-клиент
никогда её не принимает). Salt и KDF в подписанном транскрипте.
TypeScript/browser в том же релизе, что Rust-ядро.

### 4.3. Смешанные версии: публичные исходы (ЕДИНАЯ таблица)

| пара | wire-наблюдаемое | типизированная ошибка нового кода |
|---|---|---|
| новый клиент (v2 hello) → сервер, принявший только v1 | сервер закрывает соединение после `auth_init`, challenge-фрейм не приходит (наблюдаемое сегодня поведение reject-ветки `sync/handshake.rs:172-179` → `handle_connection` shutdown `:893-907,924-932` без application-ошибки) | Rust: `Error::HandshakeVersionMismatch` (сообщение: «сервер может говорить другую версию хендшейка»); TS: `HandshakeVersionMismatchError`. Это ПОДСКАЗКА о совместимости, не диагноз: версионный отказ и блокировка до challenge выглядят одинаково. Ошибка выдаётся только при ЧИСТОМ закрытии (EOF / WS close) на границе challenge; таймаут, ошибка ввода-вывода и обрыв кадра сохраняют свои сегодняшние ошибки (в TS сегодня чистое закрытие и прочие ошибки приёма идут одним путём `framing.ts:240-253,278-311` — шаг 4 их разделяет; `tcp/framing.rs:49-62,80-105`) |
| новый клиент (v2 hello) → сервер ответил 6-элементным challenge | 6-элементный фрейм дошёл | Rust: `Error::ChallengeLayout`; TS: `ChallengeLayoutError`; **proof не отправляется** |
| старый клиент (v1 hello) → новый сервер ВО ВРЕМЯ rollout (§7 шаги 2-4) | старый 6-элементный challenge, старый поток завершается | legacy-код: наблюдаемое поведение, не обещание |
| старый клиент (v1 hello) → новый сервер ПОСЛЕ шага 5 | сервер закрывает соединение | legacy-клиент сообщает transport failure — наблюдаемое поведение, не обещание |
| Rust vs TypeScript | семантика идентична; имена ошибок: Rust `shamir_connect::common::error::Error::{HandshakeVersionMismatch, ChallengeLayout}` (`common/model/error.rs:30` — рядом с `UnsupportedVersion`, `:84`); TS — новые классы в `crates/client/shamir-client-ts/src/core/support/errors.ts` по образцу `ServerIdentityChangedError` (`:169-188`); `shamir-client` оборачивает в `ClientError::handshake_with`. Оболочка (`shell/connect/scram.rs:334-360`) отображает только `UnsupportedVersion`: новые варианты без явного отображения попадут в «ошибка аутентификации», поэтому отображение добавляется в шаге 3б (§5) | — |

Во время rollout сервер принимает обе версии: v1 hello → старый
6-элементный challenge и старый поток; v2 hello → v2 challenge (§7 шаг 2).
После шага 5 v1-приём удалён.

**Отличие от раунда 2**: правило «ровно 9 элементов» устраняет
двусмысленность «6-элементный = ошибка декодирования ИЛИ
ServerIdentityChanged» — ровно один исход `ChallengeLayout`; «version-2-OK»
как отдельный сценарий удалён — смешанные пары покрывает эта таблица.

### 4.4. Session resumption

Полный SCRAM меняется; resume — нет. Отсутствие preamble на
browser/plain/IPC resume — существующая дыра, зафиксированная как известная
(§1.4), вне 5 шагов §7.

### 4.5. Версия в транскриптах и в allowlist (иначе стороны разойдутся в proof)

Версия handshake входит в SCRAM `auth_message` и в финальные подписи.
Сегодня она зашита как v1 в обеих реализациях, а ядро сервера отвергает
всё, кроме v1, независимо от транспортного allowlist. Нормативное правило:
**на handshake существует ОДНО неизменяемое значение версии — из
принятого hello, а не константа; один и тот же байт идёт в hello и во все
транскрипты.** Единственный построитель `auth_message` сервера
(`server/auth/handshake.rs:211-239,261-300`) кормит ТРИ потребителя:
проверку client proof, подпись текущего ключа идентичности и подпись
recovery предыдущего ключа; все три наследуют версионный байт. Транскрипт
`auth_message` остаётся побайтно прежним, кроме этого последнего байта
версии. Две вещи версии НЕ касаются и не меняются: формат
rotation-proof (`common/runtime/rotation.rs:31-42`), TLS-exporter label и
WS-субпротокол (`transport-ws/src/server.rs:27`) — это не поля версии
hello. Подпись challenge версии не содержит (§3.1, решение оркестратора).
Следствия, обязательные для шагов §7:

- Ядро сервера (`shamir-connect/server/auth/handshake.rs:135-141`)
  принимает `{1, 2}` на шаге 2 и `{2}` на шаге 5; построители транскрипта
  сервера (`:290-300`) принимают `version` параметром и получают его из
  принятого hello. Транспортный гейт — `check_handshake_proto`
  (`sync/connection/handshake.rs:165-179,232-237`) — пересылает
  `init.version` в ядро.
- **Производитель hello на клиенте.** `ClientHandshake::auth_init` сегодня
  эмитит v1 независимо от литерала версии в транскрипте
  (`shamir-connect/client/handshake.rs:185-194`), а транспорт Rust-клиента
  копирует результат без изменений (`shamir-client/.../scram.rs:63-71`).
  Версия параметризуется в `auth_init` ТОЙ ЖЕ величиной, что и построители
  транскрипта клиента; значения по умолчанию, которое можно забыть
  переключить, нет.
- Построители клиента (`shamir-connect/client/handshake.rs:223-233`)
  принимают `version` параметром; v2-клиент передаёт 2 и ничего другого.
- TypeScript: построители в `core/protocol/scram.ts:154-169` принимают
  версию явным параметром (сегодня `SUPPORTED_VERSION` используется как
  значение по умолчанию); `runHandshake` (`protocol.ts:133-173`) строит
  hello и транскрипты из ОДНОГО явного значения версии, не из default'а
  построителя. Поднятие `SUPPORTED_VERSION` до 2 происходит на шаге 4
  одним изменением вместе с использованием параметра, не раньше: иначе
  TS и Rust разойдутся в proof или сервер отвергнет v2 до challenge.
- Вектора (§4.1) содержат пары для обеих версий `auth_message`: транскрипт
  при версии 1 не меняется (регресс v1-клиентов на шагах 2-4), транскрипт
  при версии 2 фиксируется впервые (генерирует reference шага 2, не шага 1:
  в шаге 1 `auth_message` не меняется).

---

## 5. Список изменений по компонентам (полный inventory call-site)

| Файл | Изменение | Этап (§7) |
|---|---|---|
| `crates/net/shamir-connect/src/common/user/identity.rs` | домен-тег `CHALLENGE_ID_V1`, `build/verify_challenge_identity_input` | 1 |
| `crates/net/shamir-connect/src/common/domain_tags.rs` | константа `CHALLENGE_ID_V1` | 1 |
| `crates/net/shamir-connect/src/common/model/error.rs` | варианты `HandshakeVersionMismatch`, `ChallengeLayout` (рядом с `UnsupportedVersion`, стр. 84); ОБЯЗАТЕЛЬНО классифицировать оба в исчерпывающем `Error::to_wire` (стр. 137-179) и в его оракуле (`tests/types_tests/error_tests.rs:44`): это локальные диагностики клиента, а не новые серверные wire-ответы | 1 |
| `crates/net/shamir-connect/src/server/auth/handshake.rs` | `ChallengeView` += `server_pub_key`, `challenge_sig`, `challenge_rotation`; подпись в `challenge()`; `rotation_in_progress_payload` (стр. 261-281) читает snapshot | 1 (типы), 2 (snapshot-переключение) |
| `crates/net/shamir-connect/src/server/ops/rotation.rs` | экспорт chain-конструкта (переиспользование `build_rotation_proof_input`, стр. 318-320) | 1 |
| `crates/net/shamir-connect/src/server/ops/rotation.rs`, `crates/server/shamir-server/src/engine/server_meta.rs`, `.../server_launcher/launch.rs` | готовый `rotation_proof` в опубликованном состоянии идентичности: строится при `rotate` И при rehydration из сохранённого перекрытия (`server_meta.rs:550-562,764-780`, `launch.rs:571-579`), сбрасывается при финализации (§3.2) | 2 |
| `crates/net/shamir-connect/src/client/handshake.rs` | `ServerChallenge` += поля; `verify_challenge_identity`; правило §3.2; хранение challenge-ключа | 1 |
| `crates/net/shamir-connect/src/client/rotation.rs` | переиспользование window-проверки (стр. 145-151), pub-видимость | 1 |
| `crates/server/shamir-sync/src/sync/connection/session/connection_context.rs` | БЕЗ нового поля: контекст общий для слушателя (§3.2). Меняется только `identity_keypair_for_verify` (стр. 86-94): boot-time копия заменяется чтением snapshot'а, который ей передаёт `run_handshake` | 2 |
| `crates/server/shamir-sync/src/sync/connection/handshake.rs` | `run_handshake` захватывает snapshot сразу после принятия полного SCRAM-hello; dual-version провод: v1 → 6-элементный challenge (стр. 264-283), v2 → 9-элементный; метка версии ключа snapshot'а на начальном билете (стр. 645-680) | 2 |
| `crates/server/shamir-sync/src/sync/connection/session/scram/auth_ok_hints.rs` | получает snapshot (стр. 6-21) | 2 |
| `crates/server/shamir-sync/src/version.rs` | шаг 2: `SUPPORTED = &[1,2]`; шаг 5: `&[2]`, `CURRENT = 2` (стр. 31-38) | 2, 5 |
| `crates/net/shamir-transport-tcp/-ipc/-ws` | без изменений; e2e этапа 3 | 3 |
| `crates/client/shamir-client/src/client/handshake/scram.rs` | v2 hello (версия параметром `auth_init`, §4.5); marker-aware валидатор раскладки (§4.1 п.1-8); проверка между стр. 102 и 109 | 3б |
| `crates/client/shamir-client/src/client/handshake/wire.rs` | `WireChallenge` v2; замапленные ошибки | 3б |
| `crates/client/shamir-client/src/client/handshake/auth_ok.rs` | сверка challenge/auth_ok ключей для КАЖДОГО v2-handshake ДО recovery-разбора (терминальный сбой continuity, не в ветку `:67-94`); rotation-путь `:67-94` сохраняется | 3б |
| `crates/server/shamir-server/shell/connect/scram.rs` | явное отображение `HandshakeVersionMismatch` и `ChallengeLayout` (сегодня отображается только `UnsupportedVersion`, стр. 334-360; новый вариант без отображения попадёт в «ошибка аутентификации»); отображение `IdentityRotationPending` на жёсткий отказ (стр. 263-268) остаётся как есть | 3б |
| Rust prod-вызовы `run_scram`: `entry.rs:182,217`, `entry_ws.rs:120` | без изменений call-site: pin в `ScramRequest.trusted_pin` (`scram.rs:55-58`) из `ConnectOptions.trusted_pin` (`session/options.rs:29-32,176`) | 3б |
| Rust test-вызов `run_scram`: `tests/wire/scram_binding_tests.rs:186` | параметризуется версией: до флипа продакшен-клиента остаётся v1, после — v2 | 3а, 3б |
| Node binding: `crates/client/shamir-client-node/src/client.rs:166` (`connect_host`) и `connect_variants/{connect_ws,connect_ws_browser,connect_local,connect_plain}.rs` | без изменений: pin течёт через `core_opts.trusted_pin` (`client.rs:48`, опции `options/*.rs`); верифицируются e2e | 3б |
| Admin shell: `crates/server/shamir-server/shell/connect/scram.rs` | без изменений: pin валидируется в `prepare` (стр. 103-106) и присваивается в обоих dial-путях — `accept_new_host: false, trusted_pin: Some(self.pin)` (стр. 199-200, 231-232); e2e роуг-фикстурой; отображение новых ошибок — строка выше | 3б |
| TS prod: `core/protocol/protocol.ts::runHandshake` (стр. 116-183) | v2 hello (`SUPPORTED_VERSION` → 2); проверка `chArr.length === 9` перед индексами; pin-политика (БЕЗ callback до proof) и `ChallengeLayout`/`HandshakeVersionMismatch` ошибки; проверка между decode (стр. 147-160) и `computeClientProof` (стр. 174); hello и транскрипты берут ОДНО явное значение версии (§4.5); явный marker-aware валидатор раскладки (§4.1: сегодня корень и слоты KDF приводятся типами); разделение чистого закрытия и прочих ошибок приёма (`framing.ts:240-253,278-311`) | 4б |
| TS prod: `core/protocol/session/open.ts` | `openSession` (стр. 199-227) передаёт `policy.trustedPin` **в** `runHandshake` (стр. 218); **TOFU (раунд 4)**: при `trusted_pin == None` challenge-ключ остаётся приватным для handshake, callback ДО proof НЕ вызывается; ключ challenge сверяется с `auth_ok.server_pub_key` для каждого v2-handshake до recovery-разбора, pin публикуется только после финальной проверки идентичности (`open.ts:183-191,218-239`; сегодняшний порядок «pin после проверки» сохраняется) | 4б |
| TS test: `core/protocol/__tests__/protocol.test.ts` (обёртка `runHandshake`, стр. 291+), `session/__tests__/binding.test.ts` (стр. 129-162), `session/__tests__/tls-binding.test.ts` (стр. 124,152,207) | параметризация фикстур версией и 9-элементным challenge за двухверсионным сервером (4а); `session/__tests__/identity.test.ts:208-233`, `core/client/__tests__/connect-local.test.ts:151-153` — в том же перечне | 4а |
| TS `core/protocol/scram.ts` | `verifyChallengeIdentity` (зеркало §3.1), константа `CHALLENGE_ID_V1`, версия явным параметром; `SUPPORTED_VERSION = 2` | 4б |
| TS `core/support/errors.ts` | `HandshakeVersionMismatchError`, `ChallengeLayoutError` по образцу `ServerIdentityChangedError` (стр. 169-188) | 4б |
| `docs/guide-docs/client-server-protocol-spec/AUTH_PROTOCOL.md` | §5.2 v2-раскладка §4.1; §5.3 порядок; §6.5 chain в challenge; §11.2.4 | 5 |
| `.../SECURITY_MODEL.md` | G1/G2 §2.3 + таблица §2.5 | 5 |
| `.../TRANSPORT_TCP.md`, `TRANSPORT_UNIX.md`, `TRANSPORT_WS.md` | диаграммы v2 + dual-version окно rollout'а | 5 |
| `docs/guide-docs/guide/07-operations.md` | pin до Argon2id (стр. 3473-3475); поведение при mismatch | 5 |
| `docs/dev-artifacts/design/admin-shell.md` | `HANDSHAKE:252-301` порядок | 5 |

---

## 6. Тестовые оракулы (дизайн; пишутся на этапах реализации)

### 6.1. Роуг-сервер: ни байта приложения от пароля

Фикстура в `crates/net/shamir-connect/tests/auth/` (рядом с
`integration_full_auth.rs`, `rv_handshake_0fc58d93_tofu_pin_after_identity_verify.rs`):
роуг с чужим identity-ключом завершает транспорт честно и пишет challenge.
**Метрика — application payload-байты, полученные фикстурой после записи
challenge, частичные записи включительно**; трафик закрытия (WS close
frame, TCP FIN/RST) учитывается отдельно и в метрику не входит. Ожидание:
множество payload-байтов пусто; клиент — `ServerIdentityChanged`,
`DerivedKeys::derive == 0`. **Дискриминирует**: регрессию порядка; proof
при pin mismatch.

### 6.2. Негативные пары challenge (ровно один ожидаемый исход на строку)

| # | challenge роуга | ожидание нового клиента | что дискриминирует |
|---|---|---|---|
| N1 | чужой ключ + **валидная** подпись этого ключа, без chain | `ServerIdentityChanged` до Argon2id | отсутствие сравнения pin |
| N2 | **правильный** ключ + **испорченная** подпись | `ServerIdentityChanged` до Argon2id | отсутствие проверки подписи |
| N3 | 6-элементный (v1-байты) после v2 hello | `ChallengeLayout`, proof не отправляется | fallback на старый поток |
| N4-N7 | правильный ключ, подпись валидна над транскриптом, в котором ровно ОДНО поле заменено: N4 nonce (клиентский или серверный), N5 salt, N6 один из KDF-параметров, N7 transport/binding | `ServerIdentityChanged` до Argon2id | проверка подписи над неполным транскриптом (поле выпало из входа подписи) |
| N8 | challenge подписан для ДРУГОГО локального exporter'а (клиент держит ненулевой exporter, подпись сделана над иным) | `ServerIdentityChanged`, ноль вызовов KDF, ноль байт proof | проверка challenge-подписи с нулевым exporter'ом вместо локального |
| P1 | тот же challenge, подписанный верно, при ненулевом локальном exporter'е | успех: KDF вызван, proof отправлен | отказ на любом ненулевом exporter'е |

(Вариант «без chain-блоба при несовпадении pin» уже покрыт N1 — пустой
элемент 8 = нет цепочки.) N8 и P1 требуются в ОБОИХ production-путях
клиента (Rust и TS) — существующий TS-тест привязки
(`session/__tests__/tls-binding.test.ts:152-189`) проверяет позднее
proof, а не раннюю подпись challenge. Раскладочные вектора (§4.1 п.8)
прогоняются через те же клиентские пути. Шаг 1 (без провода) тестирует
только помощники; байтовый оракул транспорта живёт в шагах 3б и 4б.

### 6.3. Rotation-оракулы (позитив + три негатива) — НОВОЕ в раунде 3

- **Позитив**: клиент с **previous**-pin против сервера **внутри** окна
  rotation: chain в challenge верифицирован → proof отправлен → в `auth_ok`
  `rotation_in_progress` → `IdentityRotationPending`
  (`auth_ok.rs:67-91`); **ни одного proof-байта не уходит до верификации
  chain** (та же метрика §6.1). **Дискриминирует**: ветку §3.2-исход-2.
- **Негативы** (все → `ServerIdentityChanged` до proof):
  (a) deadline истёк (`transition_until_ns <= now`) — правило `rotation.rs:146-148`;
  (b) deadline за верхним пределом (`> now + ROTATION_MAX_TRANSITION_NS`)
  — `rotation.rs:149-151`;
  (c1) подпись chain испорчена (ключ `previous_pub` верный, подпись нет);
  (c2) подпись chain валидна, но `SHA256(previous_pub) != pin` («чужой
  якорь») — `rotation.rs:116-119,131-143`. Два отдельных случая: один и
  тот же исход не должен прятать два разных пропущенных контроля.
- **Порядок «проверка до отправки»**: положительный сценарий сам порядок
  не доказывает (реализация, проверяющая ПОСЛЕ отправки, даст тот же
  итог). Порядок доказывают негативы: N1, N2 и негативы (a)-(c) измеряют
  payload-байты фикстуры и требуют, чтобы множество было пустым.
- **Окно истекает между challenge и proof**: фикстура отдаёт цепочку с
  дедлайном, который истекает до `auth_ok` (управляемые часы клиента);
  ожидание — `ServerSignatureInvalid` из существующей клиентской проверки
  дедлайна (`client/rotation.rs:145-151`; с замороженным payload §3.2, не
  `ServerIdentityChanged`), без новых вариантов ошибок; дискриминирует
  двойную проверку окна (§3.2).
- **Форма внутренней тройки при ПРЯМОМ совпадении pin** (§4.1 п.4):
  валидный challenge с malformed элементом 8 (хвост после тройки, не 3
  элемента) при совпавшем pin → `ChallengeLayout`, а не успех.
- **Continuity** (§3.2): для каждого v2-handshake `auth_ok.server_pub_key`
  равен challenge-ключу: (i) ключи совпали → успех; (ii) финальный ключ
  изменён → терминальный сбой БЕЗ перехода в recovery-ветку; (iii)
  финальная подпись идентичности неверна → ошибка, ноль callback'ов TOFU,
  pin не опубликован.

### 6.4. Смешанные пары на реальных байтах

- новый клиент против v1-байтов challenge → N3 (один исход);
- **legacy TypeScript decoder против v2-байтов** — ОТДЕЛЬНО от серверного
  reject: фикстура подаёт 9-элементный массив старому индексному декодеру
  (`protocol.ts:147-160`) и фиксирует его наблюдаемое поведение (лишние
  индексы игнорируются молча) — это документирует, почему защита старого
  TS — серверный version-gate (`sync/handshake.rs:172-179`), а не decoder;
- старый клиент с `version=1` против нового сервера: во время rollout —
  старый поток; после шага 5 — закрытие соединения (§4.3, наблюдаемое);
- **закрытие до challenge**: фикстура-сервер ЧИСТО закрывает соединение
  после `auth_init`; новый Rust- и TS-клиент выдают `HandshakeVersionMismatch`
  (подсказка о возможной несовместимости, не диагноз) и не отправляют
  ничего ещё; таймаут, ошибка ввода-вывода и обрыв кадра на той же границе
  сохраняют свои прежние ошибки; оболочка отображает новый вариант
  явно, не как «ошибку аутентификации» (§5, шаг 3б);
- **прямой legacy Rust-декодер против v2-байтов**: фикстура подаёт
  9-элементный массив декодеру `wire::Challenge` текущей раскладки и
  фиксирует `LengthMismatch` (`rmp-serde-1.3.1/src/decode.rs:566-574`):
  старый декодер ТОЖЕ отвергает v2-байты, а совместимый обмен ему
  обеспечивает серверный version-gate (v1 hello → 6 элементов); старый
  TS-декодер, наоборот, молча игнорирует лишние слоты (индексное чтение);
- **вектора обеих версий** (§4.5): транскрипт при версии 1 не меняется,
  при версии 2 совпадает побайтно в Rust и TypeScript.

Каждый исход — именованный типизированный результат; «known incompatibility»
как исход отсутствует.

### 6.5. Round trips: 4 фрейма приложения

Подсчёт application frames полного v2-хендшейка — ровно 4. Формулировки
«wall-clock latency не изменилась» и «существующие e2e остались зелёными»
оракулами НЕ являются.

### 6.6. TOFU-захват pin привязан к challenge-ключу

Pin из `pin_callback` равен `SHA256(challenge.server_pub_key)`; сверка с
`auth_ok.server_pub_key` обязательна для каждого v2-handshake и
предшествует recovery-разбору (Rust: `handshake.rs:297-302`,
`auth_ok.rs:67-94`; TS: `verifyAuthOkIdentity`, §5). Публикация pin
происходит ТОЛЬКО после финальной проверки идентичности; до proof
никакого callback'а нет (§3.2, раунд 4). Тесты: ключи совпали, финальный
ключ изменён, финальная подпись неверна; проваленный handshake не
публикует pin.

---

## 7. Rollout (5 шагов, 7 PR; master говорит на каждом шаге)

Матрица совместимости полного SCRAM (`версии, принимаемые сервером ←
версия, которую шлют Rust / TS клиенты`):

| после шага | сервер принимает | Rust-клиент шлёт | TS-клиент шлёт |
|---|---|---|---|
| 1 | {1} | 1 | 1 |
| 2 | {1, 2} | 1 (плюс test-only v2-клиент) | 1 |
| 3 | {1, 2} | 2 | 1 |
| 4 | {1, 2} | 2 | 2 |
| 5 | {2} | 2 | 2 |

Старые серверы v2 не принимают никогда; старые клиенты перестают работать
после шага 5 (§4.3, наблюдаемое поведение, не обещание).

| # | Содержание | Файлы | Приёмка | Размер | Почему master интероперабелен |
|---|---|---|---|---|---|
| 1 | Ядро `shamir-connect`, АДДИТИВНО: домен-тег, sign/verify challenge-identity, типы `ChallengeView`/`ServerChallenge`/`ChallengeRotationWire`, варианты ошибок (с классификацией в `Error::to_wire`, §5), вектора §4.1 (транскрипт + раскладочные принять/отказать). Поля добавляются к существующим типам ТОЛЬКО вместе с обновлением ВСЕХ существующих литералов в этом же PR (инвентарь поиском по `ServerChallenge`/`ChallengeView` во всех зависимых крейтах, включая `kdf_rejection_tests.rs`, `integration_full_auth.rs`, `rv_handshake_0fc58d93_*`); `auth_message` НЕ меняется — **без изменения поведения** | `identity.rs`, `domain_tags.rs`, `model/error.rs`, `server/auth/handshake.rs`, `server/ops/rotation.rs`, `client/handshake.rs`, `client/rotation.rs`, тесты и вектора | unit-тесты помощников (§6.2 N1/N2/N4-N7 и раскладочные вектора на уровне помощников, §6.3 без провода); `cargo check`/`clippy` для `shamir-connect` И каждого зависимого крейта, где найден литерал; существующий v1-поток не затронут | M | новые типы не подключены к проводу |
| 2 | Сервер говорит на ДВУХ версиях: `run_handshake` берёт snapshot сразу после принятия полного SCRAM-hello (§3.2: подписанты, готовый `rotation_proof` из состояния идентичности, строящийся при `rotate` и rehydration, метка версии ключа на начальном билете, snapshot в `auth_ok_hints`); v1 hello → старый 6-элементный challenge, v2 hello → 9-элементный; транспортный `SUPPORTED = &[1,2]` И ядро `server/auth/handshake.rs:135-141` принимает `{1,2}`; построители транскрипта сервера принимают версию параметром (§4.5); вектора `auth_message` при версиях 1 и 2 | `version.rs`, `sync/connection/handshake.rs`, `auth_ok_hints.rs`, `connection_context.rs` (только чтение snapshot'а), `shamir-connect/server/{auth/handshake.rs,ops/rotation.rs}`, `server_meta.rs`, `launch.rs` | ОБА полных обмена: v1-e2e зелёные, v2-e2e (test-only v2-клиент) зелёные; оракулы 6.4 (серверная часть), 6.5; тест rehydration готового proof; тест билета старого ключа | M | v1-клиенты (текущий master-код) продолжают старый поток; v2-ветка изолирована версией hello |
| 3а | Миграция Rust-фикстур ЗА двухверсионным сервером, без изменения production: сырые тестовые пиры, пиры KDF и `auth_ok` (`shamir-client/src/tests/auth/kdf_rejection_tests.rs:65-86`, `auth_ok_hints_tests.rs:145-173`), `tests/wire/scram_binding_tests.rs:186`, шины `handshake_e2e*` — каждый пир явно выбирает v1 или 9-элементный v2; значение версии hello по умолчанию остаётся 1 | тесты `shamir-client`, `shamir-connect`, `shamir-e2e` | прежний набор зелёный; новые пиры v2 зелёные против сервера шага 2 | S | production не менялся |
| 3б | Rust-клиент переходит на v2 (production): v2 hello (версия параметром `auth_init`), marker-aware валидатор раскладки (§4.1), pre-Argon2id pin/chain-проверка в `run_scram` между стр. 102 и 109, continuity `auth_ok`↔challenge (§3.2), ошибки `HandshakeVersionMismatch`/`ChallengeLayout` и их отображение оболочкой | `shamir-client/src/client/handshake/{scram.rs,wire.rs,auth_ok.rs}`, `shamir-connect/client/handshake.rs`, `shell/connect/scram.rs` | оракулы 6.1, 6.2 (N1-N8, P1), 6.3, continuity на клиенте; production v2 УСПЕХ на каждом Rust-пути (TCP, IPC, WS, Node, оболочка) e2e; байтовый оракул транспорта | M | сервер шага 2 принимает v2; v1-клиенты (TS) обслуживаются |
| 4а | Миграция TS-фикстур ЗА двухверсионным сервером, без изменения production: `protocol.test.ts`, `binding.test.ts`, `tls-binding.test.ts`, `identity.test.ts:208-233`, `connect-local.test.ts:151-153` — явная версия и 9-элементный challenge в фикстурах | TS `__tests__` | прежний набор зелёный | S | production не менялся |
| 4б | TypeScript переходит на v2 (production): `runHandshake` (v2 hello и транскрипты из ОДНОГО значения версии, marker-aware валидатор раскладки с `bigint` для u64, pin-политика БЕЗ callback до proof, новые ошибки, разделение чистого закрытия и прочих ошибок приёма), `scram.ts` (версия параметром, `SUPPORTED_VERSION = 2`, `verifyChallengeIdentity`), `open.ts` (continuity и публикация pin после финальной проверки), общие вектора §4.1. TS проверяет цепочку в challenge; `verify_rotation_recovery` в `auth_ok` TS по-прежнему не реализует и при несовпадении ключа выдаёт `ServerIdentityChanged`, как сегодня: известный предел | `core/protocol/{protocol.ts,scram.ts,framing.ts}`, `core/protocol/session/open.ts`, `core/support/errors.ts` | TS-вектора, N1-N8/P1-эквиваленты, continuity, production v2 УСПЕХ против сервера шага 2 зелёные в CI | M | сервер шага 2 обслуживает и v1; порядок 3→4 сохраняет master говорящим на обеих версиях |
| 5 | Сервер удаляет v1: транспортный `SUPPORTED = &[2]`, `CURRENT = 2`, ядро `server/auth/handshake.rs:135-141` принимает `{2}`, старая ветка провода удалена; тесты, которые ЯВНО принимают v1 (`tests/auth/integration_full_auth.rs:80-108`, `shamir-server/tests/e2e/transport/ipc_e2e.rs:154-192`, `src/tests/boot/config/version_tests.rs:6-20`), переписываются на ОТКАЗ v1; спецификации и гайды | `version.rs`, `sync/connection/handshake.rs`, `shamir-connect/server/auth/handshake.rs`, перечисленные тесты, docs (таблица §5) | e2e только v2; v1-отказ зафиксирован наблюдением (§4.3 строка 4) | S | к этому шагу все клиенты master говорят v2 |

Правило: каждый шаг оставляет master с клиентом и сервером, которые
говорят друг с другом (dual-version сервер шага 2 — единственная точка,
где обе версии живут одновременно; удаление v1 — последний шаг).
Миграция фикстур (3а, 4а) идёт ДО флипа production в том же шаге, а не
откладывается на шаг 5.

## 8. Не сделано, риски, открытые вопросы

**Установлено чтением:** `rmp-serde 1.3.1` `LengthMismatch` на лишних
элементах (`decode.rs:566-574`); позиционность challenge; TS-индексный
декодер; конструкты цепочки (`ops/rotation.rs:318-320`); window-проверки
(`client/rotation.rs:145-151`); keypair-copy vs live state
(`connection_context.rs:82-94`, `server/auth/handshake.rs:261-281`).

**INFERRED / вне досягаемости чтения:**

1. Вектор challenge-identity уже сгенерирован независимым reference
   (RFC 8032, шаг 1 в работе); раскладочные вектора принять/отказать
   (§4.1 п.8) и вектора `auth_message` при версии 2 (§4.5) ещё не
   сгенерированы.
2. Арифметика размеров (§3.1, §4.1) проверена по исходникам энкодера
   (`rmp-serde 1.3.1`, `rmp 0.8.15`), а не измерена на байтах фикстур;
   нормативно декодер принимает любую валидную форму.
3. Точный текст ошибки TS при коротком массиве — неважно за правилом
   «ровно 9» (явная проверка длины).
4. Хронометраж Argon2id на floor — бенчи запрещены; сравнение §2.2
   арифметическое.

**Известные пределы (решено, не открытые вопросы):**

1. Rotation-политика (a) §3.2, snapshot-механика и исход «нет chain при
   несовпадении pin вне окна → `ServerIdentityChanged` до proof» решены
   владельцем поручением «решать в сторону совершенства»; альтернатива
   (b) (удалить ин-банд recovery) отвергнута.
2. Полное восстановление rotation в TypeScript (`verify_rotation_recovery`
   в `auth_ok`) сегодня отсутствует и остаётся пределом: TS проверяет
   цепочку в challenge и при несовпадении ключа в `auth_ok` выдаёт
   `ServerIdentityChanged` (§7, шаг 4б). Принятие цепочки — не автоматическое
   восстановление ни в одном клиенте (три исхода в §3.2). Следующая задача,
   не часть шагов.
3. Resume без exporter-preamble на browser/plain/IPC (§4.4) остаётся
   известной дырой.
4. Клиент без pin (TOFU) не получает G1 на первом соединении (§2.5).

## 9. Правки раунда 3 (traceability)

P1-A → §4.1 (одна нормативная таблица, 9 элементов, точные типы/длины,
`ChallengeRotationWire`, правило «ровно 9», правило генерации фиксчура);
все упоминания «8 элементов» устранены (grep «8-элементн» по документу —
0 совпадений). P1-B → §4.3 (единая таблица исходов, новые варианты ошибок
`HandshakeVersionMismatch`/`ChallengeLayout` с домами `model/error.rs:30`
и `core/support/errors.ts:169-188` как образец), §6.2 (один исход на
строку, «version-2-OK» удалён), §6.4 (legacy-TS отдельно от серверного
reject), §7 (5-шаговый dual-version rollout с обоснованием
интероперабельности). P1-C → §1.5, §3.2 (snapshot, клиентское правило с
цитатой `rotation.rs:145-151`, судьба `rotation_in_progress_payload`),
§6.3 (позитив + негативы a/b/c). P1-D → §2.3 (ровно G1/G2, одно предложение
про relay на 0x00/0x02), §2.5 (таблица согласована), §3.1 (правило
подписанта через «или chain»). P2-1 → §5 (тестовые вызовы TS и Rust,
Node TCP entry, TOFU-callback путь, shell `:199-200,231-232`). P2-2 →
§1.2 строка 0 (app-authenticated verifier без CA/hostname —
`dial.rs:38-54`, `tls.rs:191-202`), §2.5 строка browser. P2-3 → §2.2
(13.5x, арифметика), §3.1 (wire-рост с MessagePack-overhead и rotation
элементом; «перенос» удалён из §1.5 и §3.1 — финальная подпись остаётся).
P2-4 — подтверждённое раундом 2 не повторяется.

## 10. Правки оркестратора после ревью раунда 3

Вместо четвёртого раунда исполнителя оркестратор исправил четыре P1 ревью:
размеры (§3.1, §4.1: `Bin8`, +102/+212 байт, 110/112 байт, правила
декодера, `bigint` для u64 в TS); версия в транскриптах и в allowlist ядра
(§4.5, §7 шаги 2-5); владение snapshot'ом и подписывающая способность
(§3.2); область гарантий G1 для клиента без pin и роль rotation-цепочки
(§2.5, §3.2). Закрыты P2 по оракулам (§6.3, §6.4) и P3 (решённые вопросы
вынесены из «открытых», §8). Независимое ревью документа после этих
правок проведено и привело к §11.

## 11. Правки по независимому ревью (раунд 4)

Ревьюер (read-only, вердикт FAIL, удержать шаги 2-5; шаг 1 может идти как
аддитивный) нашёл 6 P1, 5 P2, 2 P3; все учтены в тексте, ничего не
отложено.

- **P1-1 формат (§4.1):** 110/112 — размеры энкодера, не требование;
  правила 1-8 (одна внешняя раскладка, одна внутренняя тройка без хвоста,
  marker-aware целые и bin, диапазон до сужения, `bigint` для u64, три
  класса ошибок, раскладочные вектора принять/отказать).
- **P1-2 snapshot (§3.2, §5, §7):** владелец — `run_handshake`, не
  `ConnectionContext`; готовый `rotation_proof` вводится в состояние
  идентичности (rotate и rehydration), сбрасывается при финализации;
  `auth_ok_hints` и метка версии ключа на начальном билете; исход
  истёкшего окна — `ServerSignatureInvalid`.
- **P1-3 версия (§3.1, §4.5):** одно неизменяемое значение на handshake
  для hello и всех транскриптов; `auth_init` параметризуется; три
  потребителя `auth_message`; подпись challenge версию не содержит
  (решение оркестратора: тег версионирует конструкцию; вектор не
  пересчитывается).
- **P1-4 TOFU (§3.2, §5, §6.6, §7):** callback до proof отменён;
  continuity `auth_ok`↔challenge для каждого v2-handshake до recovery;
  публикация pin только после финальной проверки.
- **P1-5 rollout (§7):** матрица совместимости; шаг 1 аддитивен
  (литералы обновляются в том же PR); шаги 3 и 4 разделены на 3а/3б и
  4а/4б (миграция фикстур за двухверсионным сервером, затем флип
  production); шаг 5 переписывает тесты, принимающие v1.
- **P1-6 оракулы (§6.2, §6.3):** N4-N8 и P1 (exporter) в обоих
  production-путях; (c) разделён на (c1)/(c2); форма внутренней тройки при
  прямом pin; continuity-оракулы.
- **P2:** области G1/G2 (§0, §2.5, §3.1); сертификаты Rust/Node/браузер
  (§2.5); закрытие до challenge — подсказка, отображение оболочки
  (§4.3, §5); три исхода принятия цепочки (§3.2); классификация новых
  ошибок в `to_wire` (§5).
- **P3:** формулировка про старый Rust-декодер (§6.4); «проверено по
  исходникам, не измерено» (§8); ревью предшествует провод-подключению
  (шаг 2), а не уже идущему аддитивному шагу 1 (шапка).
