בְּשֵׁם יהוה הָרַחֲמָן וְהַחַנּוּן

# Матрица «рукопожатие × транспорт»

Какие способы войти (рукопожатия) на каких каналах связи (транспортах) поддерживает **сервер** и какие из них реализованы в каждом **клиенте**. Это единственное место с таблицей: остальные документы ссылаются сюда. Дизайн и обоснование: [`T9 design`](../../dev-artifacts/audits/2026-11-04-t9-client-matrix/DESIGN.md).

Принцип: сервер знает только протокол, а не клиента. «Браузерные» варианты — профили протокола под ограничения среды (нет TLS exporter, нет своих заголовков у WebSocket); ими может пользоваться любой клиент. Каждый клиент должен поддерживать каждую ячейку, которую допускает протокол, кроме физически невозможных на его платформе.

## Две оси

**Рукопожатия**
- **Сессия** — SCRAM + Argon2id, подпись сервера Ed25519, привязка к TLS. Быстрый вариант: **resume** по билету, без пароля.
- **Токен** — `shk1.<id>.<secret>` в заголовке `Authorization` (браузерный WS: первым кадром).
- **Вход по паролю → токен** — HTTP-обмен `/shamir/v1/auth/begin` и `/finish`, выдаёт токен. Это способ получить токен, а не третье рукопожатие.

**Транспорты:** TCP+TLS, TCP без TLS (только loopback), WebSocket, HTTP, локальный IPC (Unix socket на Linux/macOS, Named Pipe на Windows). QUIC и mTLS в коде отсутствуют.

## Что поддерживает сервер

| Слушатель | binding_mode | Рукопожатие |
|---|---|---|
| `tcp`, `profile: tls_exporter` | 0x01 | сессия, resume |
| `tcp`, `profile: plain` (только loopback) | 0x00 | сессия, resume |
| `ws`, `profile: tls_exporter`, путь `/shamir/v1` | 0x01 | сессия, resume |
| `ws`, `profile: tls_no_export`, путь `/shamir/v1/browser`, Origin обязателен | 0x02 | сессия, resume |
| `unix`, `profile: plain` | 0x00 | сессия, resume |
| `http`: `POST /shamir/v1/request`, `GET /shamir/v1/ping` | — | токен |
| `http`: WS `/shamir/v1/ws` (заголовок Authorization) | — | токен |
| `http`: WS `/shamir/v1/ws/browser` (токен первым кадром) | — | токен |
| `http`: `/shamir/v1/auth/begin`, `/finish` | — | вход по паролю → токен |

Профиль принадлежит слушателю: клиент обязан прислать `binding_mode` своего слушателя.

**Вне протокола намеренно:** токен по TCP, по `kind: ws` и по IPC (граница безопасности — тип слушателя); сессия по HTTP (SCRAM — дуплексный обмен, привязанный к соединению). QUIC и mTLS не реализованы.

## Что умеют клиенты

Статусы: **да** — реализовано и доказано тестом; **нет** — пробел, не запланирован; **не требуется** — причина в примечании; **невозможно** — физическая причина платформы.

| # | Рукопожатие × транспорт | Rust `shamir-client` | napi `shamir-client-node` | TS в Node | TS в браузере |
|---|---|---|---|---|---|
| 1 | сессия × TCP+TLS | да | да | да (тест: crates/shamir-client-ts/src/__tests__/connection/session/e2e-native-transports.test.ts) | невозможно: нет TCP |
| 1b | сессия × TCP без TLS (loopback) | да (тест: crates/shamir-client/tests/matrix/plain_session.rs) | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | нет | невозможно |
| 2 | сессия × WS native (`tls_exporter`) | да (тест: crates/shamir-client/tests/matrix/ws_session.rs) | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да (тест: crates/shamir-client-ts/src/__tests__/connection/session/e2e-native-transports.test.ts) | невозможно: нет exporter |
| 3 | сессия × WS, браузерный профиль (`tls_no_export`) | нет (T9-8) | нет (T9-8) | да | да |
| 4 | сессия × IPC | да | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да (смена пароля по IPC: исправление S4) | невозможно |
| 6a | resume × TCP+TLS | да | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да (тест: crates/shamir-client-ts/src/__tests__/connection/session/e2e-native-transports.test.ts) | невозможно |
| 6b | resume × WS native | да (тест: crates/shamir-client/tests/matrix/ws_resume.rs) | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да (тест: crates/shamir-client-ts/src/__tests__/connection/session/e2e-native-transports.test.ts) | невозможно |
| 6c | resume × WS, браузерный профиль | нет (T9-8) | нет (T9-8) | да (без resume_proof) | план T9-8d (e2e написан; «да» после зелёного прогона) |
| 6d | resume × TCP без TLS | нет (T9-8) | нет (T9-8) | нет | невозможно |
| 6e | resume × IPC | нет | нет | нет | невозможно |
| 7 | токен × REST | да (тест: crates/shamir-client/tests/matrix/token/http.rs) | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да | да |
| 8 | токен × WS native (`/shamir/v1/ws`) | да (тест: crates/shamir-client/tests/matrix/token/ws.rs) | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да | невозможно: у WebSocket нет заголовка |
| 9 | токен × WS браузерный (`/shamir/v1/ws/browser`) | не требуется | не требуется | не требуется | да |
| 10 | вход по паролю → токен | да (тест: crates/shamir-client/tests/matrix/token/login.rs) | да (тест: tests/e2e/tests/21-connection-matrix.test.js) | да | да |

Примечания:
- Строка 9: нативному клиенту браузерный маршрут ничего не даёт, у него есть заголовок Authorization.
- Строки 3, 6c, 6d в столбцах Rust и napi (в Rust-ядре нет сессии с браузерным профилем и нет resume по голому TCP), строки 6d, 6e в остальных столбцах собраны в задачу T9-8 и пока не запланированы.
- Ячейка меняет статус на **да** только в PR, где лежит e2e, который её доказывает.
- Во всех сессиях TS-клиент проверяет личность сервера: подпись `identity_sig` (Ed25519) и пин `SHA256(pub_key)`. Передайте `trustedPin` — пин с прошлого соединения (`client.trustedPin()`); это рекомендованное укрепление. Несовпадение — типизированная ошибка `ServerIdentityChangedError`. Без пина первый сервер принимается (trust-on-first-use, `acceptNewHost`, по умолчанию включён), а его пин возвращается клиентом для хранения. Транспортное доверие (`rejectUnauthorized` по умолчанию true и опция `tls.ca`) остаётся без изменений.

Использование napi-клиента из Node: [`node-client.md`](./node-client.md).

## Ограничения REST

Через REST недоступны интерактивные транзакции (`TxBegin`/`TxExecute`/`TxCommit`/`TxRollback`), курсоры и подписки (`not_supported_over_rest`): для них нужен токенный WebSocket. Тело — только MessagePack, JSON не поддерживается. Подробности: [`token-api.md`](./token-api.md).
