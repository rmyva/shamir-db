בְּשֵׁם יהוה הָרַחֲמָן וְהַחַנּוּן

# Этаж 5 — Функции: WASM, the "M" (Modular)

**Когда подниматься:** логика переезжает в БД.

До этого этажа вся бизнес-логика жила в приложении — ShamirDB была
«глупым» хранилищем с фильтрами и индексами. Но чем сложнее продукт,
тем больше инвариантов хочется держать рядом с данными: валидация
записей, вычисляемые поля, инкапсуляция доступа. Этот этаж — о том,
как логика переезжает **в** базу.

## 1. Четыре вида функций

ShamirDB различает четыре макроса SDK:

|| Макрос | Сигнатура | Контекст | Для чего |
||---|---|---|---|
|| `#[shamir::function]` | `(ctx, batch, params) → Value` | полный (db, call, http, globals) | хранимая процедура с доступом к данным |
|| `#[shamir::procedure]` | `(ctx, params) → Value` | полный (db, call, http, globals) | процедура без batch-скрэтчпада |
|| `#[shamir::scalar]` | `(params) → Value` | **нет Ctx** — чистая | inline-функция в запросах (встроенный funclib) |
|| `#[shamir::validator]` | `(record, old_record, ctx) → Validation` | read-only ctx | валидация записи «до коммита» |

### `#[shamir::function]` — хранимая процедура

Пишешь обычный async Rust; макрос прячет WASM-ABI:

```rust
use shamir_sdk::prelude::*;

#[shamir_sdk::function]
pub async fn identity(_ctx: Ctx, _batch: Batch, params: Params) -> Result<Value> {
    let n = params.i64("n")?;
    Ok(Value::Int(n))
}
```

* `Ctx` — шлюз ко всему: `ctx.db()`, `ctx.call()`, `ctx.http_fetch()`,
  `ctx.global_get()` / `ctx.global_set()`.
* `Batch` — per-batch скрэтчпад: функции внутри одного батча обмениваются
  данными через `batch.put()` / `batch.get()`.
* `Params` — именованные параметры вызова.

<!-- TODO: verify batch scratchpad API surface matches shamir-sdk Batch docs — see examples/wasm-baseline -->

### `#[shamir::procedure]` — процедура с доступом к БД

```rust
use shamir_sdk::prelude::*;

#[shamir_sdk::procedure]
pub async fn list_all(ctx: Ctx, params: Params) -> Result<Value> {
    let table_name = params.str("table")?;
    let rows = ctx.db().table(table_name).query(None)?;
    Ok(Value::List(rows))
}
```

`procedure` = function без batch-скрэтчпада. Основной вид для «сделай
что-то с БД и верни результат».

### `#[shamir::scalar]` — чистая inline-функция

```rust
use shamir_sdk::prelude::*;

#[shamir_sdk::scalar]
pub async fn double(params: Params) -> Result<Value> {
    let n = params.i64("n")?;
    Ok(Value::Int(n * 2))
}
```

Нет `Ctx` — нет побочных эффектов. Вызывается движком в `$fn`-выражениях
при записи и в `WHERE`-фильтрах. Встроенная библиотека (`shamir-funclib`)
содержит ~120 таких скаляров: `math/abs`, `strings/lower`, `arrays/length`
и т.д.

### `#[shamir::validator]` — защита от плохих данных

```rust
use shamir_sdk::prelude::*;

#[shamir_sdk::validator]
pub async fn require_name(record: Value, _old: Option<Value>, _ctx: Ctx) -> Validation {
    match &record {
        Value::Map(entries) => {
            let name = entries.iter().find(|(k, _)| k == "name");
            match name {
                Some((_, Value::Str(s))) if !s.is_empty() => Validation::accept(),
                Some((_, Value::Str(_))) => Validation::reject("name", "name_empty"),
                _ => Validation::reject("name", "name_required"),
            }
        }
        _ => Validation::record_error("expected_map"),
    }
}
```

Валидатор запускается **до** записи — если он вернул ошибки, insert/update
отклоняется, строка не попадает в таблицу.

## 2. Ctx — контекст выполнения

`ctx` даёт доступ к трём мирам:

|| Метод | Возвращает | Для чего |
||---|---|---|
|| `ctx.db()` | `Db` | Чтение/запись таблиц: `db.table("users").query(None)`, `.get(key)`, `.insert(doc)` |
|| `ctx.call("fn", params)` | `Value` | Вызов другой зарегистрированной функции (function-calls-function) |
|| `ctx.http_fetch(req)` | `HttpResponse` | HTTP-эгресс (GET/POST, allowlist; выполняется через системный `curl`, он должен быть установлен на хосте) |
|| `ctx.http_get(url)` | `HttpResponse` | Удобная обёртка для GET |
|| `ctx.http_post(url, body)` | `HttpResponse` | Удобная обёртка для POST |
|| `ctx.global_get("key")` | `Option<Value>` | Процесс-level глобальная переменная |
|| `ctx.global_set("key", v)` | `()` | Записать глобальную переменную |

`Db`-хэндл привязан к дефолтному репозиторию; `Table` предоставляет
`.get(key)`, `.query(filter)`, `.insert(doc)`.

## 3. Компиляция и деплой

### Из исходника (Rust → WASM)

> ⚠ **Experimental, только для разработки.** Компиляция запускает `cargo` на
> хосте сервера и подключает `shamir-sdk` по абсолютному пути, с которым
> собран бинарник, — поэтому в релизных архивах и Docker-образе она не
> работает. По умолчанию доступна любому аутентифицированному пользователю
> (ресурс `compiler://`, mode `0o755`) — ограничь его в любой общей
> инсталляции. В проде загружай готовый `.wasm`.

ShamirDB умеет компилировать Rust-исходник в WASM на сервере:

```ts
import { ddl, Batch } from '@shamir/client';

await Batch.create('deploy')
  .add('fn', ddl.createFunction('list_users', {
    source: `use shamir_sdk::prelude::*;\n#[shamir_sdk::procedure]\npub async fn list_users(ctx: Ctx, _p: Params) -> Result<Value> {\n    let rows = ctx.db().table("users").query(None)?;\n    Ok(Value::List(rows))\n}`,
  }))
  .execute(client, 'my_app');
```

Движок scaffolds временный крейт, подставляет `shamir-sdk` по абсолютному
пути и запускает `cargo build --target wasm32-unknown-unknown --release`.
Нужен toolchain (`cargo` + target `wasm32-unknown-unknown`). Если его нет —
ошибка `ToolchainUnavailable`.

<!-- TODO: verify wasm-opt post-processing is always available or gracefully skipped — see function/compile.rs -->

### Из бинарника (pre-compiled WASM)

Собираешь локально, отправляешь base64:

```bash
cargo build --release --target wasm32-unknown-unknown -p my-function
```

```ts
await Batch.create('deploy-bin')
  .add('fn', ddl.createFunction('my_fn', { wasm: '<base64-encoded .wasm>' }))
  .execute(client, 'my_app');
```

Ответ: `results.fn.records[0].created_function === 'my_fn'`.

Toolchain на сервере **не нужен** — WASM уже скомпилирован.

### Замена (`replace: true`)

```ts
await Batch.create('replace-fn')
  .add('fn', ddl.createFunction('my_fn', { wasm: '<new-base64>', replace: true }))
  .execute(client, 'my_app');
```

Атомарная замена: старый WASM выгружается, новый регистрируется.
In-flight вызовы дорабатывают на старой версии.

### Уровни компиляции

Внутри движка каждая функция в каждый момент живёт на одном из двух уровней
компиляции. Быстрый уровень — однопроходный компилятор Winch: готов почти
мгновенно, но код получается медленнее. Оптимизированный — Cranelift:
компилируется дольше, код быстрее. Механика такая:

* **При создании** (`createFunction`, замена, восстановление после рестарта)
  компилируется только быстрый уровень — деплой не ждёт Cranelift.
* **После 64 вызовов** (порог по умолчанию) один фоновый job компилирует
  модуль на Cranelift и атомарно подменяет скомпилированный слот. Вызовы,
  начавшиеся до подмены, дорабатывают на старом уровне, новые идут на
  оптимизированном. Одновременно может работать не больше
  `max(1, ядра / 4)` таких фоновых компиляций.
* Если быстрый уровень модуль не принял — движок сразу компилирует его на
  Cranelift; для вызывающего это не ошибка. Если фоновый апгрейд упал —
  функция остаётся на быстром уровне (в журнале появится `warn`); повторная
  попытка одна, на 16-кратном пороге (1024 вызова при пороге 64).

Режим выбирается на стороне хоста — параметр `TierMode` конструктора
`WasmFunction` в Rust, а из DDL/OQL — поле `optimize` (см. ниже).

|| Режим | Что делает | Когда подходит |
||---|---|---|
|| `Auto` (по умолчанию) | быстрый уровень сейчас, апгрейд после порога | обычный деплой |
|| `Now` | оптимизированный уровень синхронно при создании | редко вызываемая, но тяжёлая функция |
|| `Never` | только быстрый уровень, апгрейд никогда | много мелких функций, важен холодный старт |

Текущий уровень видно из Rust: `WasmFunction::tier()` возвращает `Tier::Fast`
или `Tier::Optimized`.

**Честно об ограничениях.** Порог вызовов и размер фонового пула — опции
движка (`WasmEngineOptions::tier`: `enabled`, `hot_calls`, `background_jobs`),
а режим теперь задаётся и из DDL/OQL — полем `optimize` (ниже); эти две
настройки остаются параметрами движка сервера. Состояние уровня
не персистится: каждый старт заново компилирует функции на быстром уровне
(на оптимизированном — если быстрая компиляция не удалась или быстрого движка
нет), и апгрейды зарабатываются заново. Pulley —
не быстрый уровень, это переносимый интерпретатор для бенчмарков. На чистых
вычислениях быстрый уровень проигрывает нативному коду примерно в 1.8–6 раз,
оптимизированный — в 1.3–4 (замеры на hosted-раннере: это отношения, а не
абсолюты). А вызов БД из гостя стоит ~6 мкс накладных расходов шлюза
**независимо от уровня** — процедура, упёртая в базу, от апгрейда почти
ничего не выигрывает.

### Режим из DDL/OQL: `optimize`

Двухуровневая компиляция теперь задаётся и с провода: у `create_function`
и `check_function` есть необязательное поле `optimize`. Быстрый уровень —
Winch, компиляция почти мгновенная, но код медленнее; оптимизированный —
Cranelift, компиляция дольше, код быстрее. В режиме `auto` после порога
вызовов запускается фоновый апгрейд на Cranelift. Если быстрый уровень
модуль не принял — движок сразу компилирует его на Cranelift: в `auto`
это прозрачно, а с `optimize: 'never'` такой модуль отклоняется уже на
адмиссии.

|| Режим | Что делает |
||---|---|
|| `auto` | быстрый уровень сейчас, фоновый апгрейд после порога (по умолчанию) |
|| `now` | оптимизированный уровень синхронно при создании |
|| `never` | только быстрый уровень, апгрейд никогда |

Режим фиксируется при создании. `alter_function` пересобирает функцию и
сбрасывает её per-function счётчики вызовов.

`describe_function` возвращает по функции такую запись:

|| Поле | Что в нём |
||---|---|
|| `name` | имя функции |
|| `lang` | производный сервером: `wasm` / `rust` / `shamir` |
|| `origin` | заявка загрузившего: `rust-cargo`, `shamir-lang` или `external`. Только заявка — на валидацию, лимиты и права не влияет |
|| `abi` | номер ABI или `null` |
|| `hash` | 32 строчных hex-символа или `null` |
|| `size` | размер в байтах или `null` |
|| `optimize` | итоговый режим (resolved) |
|| `tier` | `null` или `{ tier: 'fast'|'optimized', mode, calls_observed, upgrade_state: 'idle'|'scheduled'|'failed'|'done'|'exhausted' }` |
|| `state` | `{ kind: 'loaded' }`, `{ kind: 'unloadable', reason }` или `{ kind: 'corrupt', expected, actual }` |

Строка, не прошедшая гейты загрузки, остаётся в каталоге и помечается
`unloadable` с причиной — вызовы такой функции падают с этой причиной.
Несовпадение хэша при загрузке или выгрузке помечает её `corrupt`.

```ts
const resp = await Batch.create('describe-fn')
  .add('d', ddl.describeFunction('my_fn'))
  .execute(client, 'my_app');
resp.results.d.records[0]; // { name, lang, origin, abi, hash, size, optimize, tier, state }
```

### Выгрузка: `export_function`

`export_function` возвращает ровно те байты, что были сохранены: без
рекомпиляции и нормализации. Для функции из исходника дополнительно
возвращается её `source`. Инвариант round-trip: `export(create(m)) == m`;
создание из выгруженных байт даёт тот же хэш.

Хэш идентичности — XXH3-128 по полным байтам модуля (seed 0), на проводе —
32 строчных hex-символа. Это **не** криптографический хэш: он для
идентичности и проверки порчи, без секретного seed и без обязательного
сравнения байт. Ни для lookup, ни для dedup, ни для кэширования, ни для
каких-либо решений по правам или `definer` его использовать нельзя —
поэтому второй прообраз против него безвреден по построению.

```ts
const resp = await Batch.create('export-fn')
  .add('e', ddl.exportFunction('my_fn'))
  .execute(client, 'my_app');
const { wasm, source, hash } = resp.results.e.records[0];
```

`create_function` принимает необязательный `lang: 'wasm' | 'rust' | 'shamir'`
(без него выводится: `wasm`, если прислан модуль `wasm`, `rust` — если
прислан `source`). `lang: 'shamir'` берёт поле `source` и компилирует
исходник встроенным компилятором языка `shamir` внутри сервера (внешний
тулчейн не нужен); вместе с полем `wasm` он отвергается. Компиляция идёт
под тем же ограничением одновременных сборок, что и остальные, с таймаутом
10 секунд, и итоговый модуль проходит ту же адмиссию. Ошибка компиляции
приходит одной детерминированной строкой: стабильный код, затем
`строка:столбец` и сообщение (несколько диагностик — по строке на каждую).
Там же задаётся необязательный `optimize` (см. выше).

```ts
await Batch.create('create-shamir-fn')
  .add('c', ddl.createFunction('five', {
    source: '@entry scalar five() -> i64 { return 5; }',
    lang: 'shamir',
  }))
  .execute(client, 'my_app');
```

Точка входа обязательна и одна: `@entry scalar` (чистое вычисление) или
`@entry validator` (получает `record` и `old_record`, возвращает
`Validation`). `@entry procedure` — процедура с доступом к БД: она может вызывать
`db_get(table, key)`, `db_insert(table, document)` и `db_query(table, filter)`
(ключ и фильтр — скаляр или запись из скаляров, для `db_query` допустим `null`
— «все строки»). Тип результата обязателен и берётся из контекста:
`let user: { id: i64, name: string }? = db_get("users", key);`,
`let rows: [{ id: i64 }] = db_query("users", null);`; без контекста компилятор
отвечает `T010`. Лишние поля ответа игнорируются, отсутствующее обязательное
поле или несовпадение типа — ошибка кодека (вид 3). Сервер компилирует
функции `lang = 'shamir'` на стадии процедур; скаляры и валидаторы
обращаться к БД не могут (`T007`).

Для нескольких операций за один вызов есть типизированный построитель
батча: `batch()` (нетранзакционный корень), `batch_tx()` (транзакционный
корень), `batch_id(b, id)`, `batch_read(b, alias, table, filter)`,
`batch_insert(b, alias, table, document)`, `batch_update(...)`,
`batch_delete(...)`, `request(b)`, `db_execute(r)`, `response_rows(resp, alias)`,
`response_inserted(resp, alias)` и `response_affected(resp, alias)`. Это
неизменяемые значения (`Batch`, `Request`, `Response`): каждая операция
возвращает новый план. Алиас — ASCII-идентификатор до 64 байт, уникальный в
батче; операций не больше 50 (нарушение — ошибка вида 2). Фильтр `batch_read`
— `null`, скаляр (равенство по `id`) или запись из скаляров (равенство по
каждому полю, поля в порядке имён). Проекция ответа берёт тип из контекста:
`let rows: [{ id: i64 }]? = response_rows(resp, "all");` — отсутствующий
алиас даёт `null`, несовпадение формы — ошибку кодека (вид 3).

Изменение и удаление — `batch_update(b, alias, table, filter, set)` и
`batch_delete(b, alias, table, filter)`: фильтр — непустая запись равенств
по скалярам без `null`, у `batch_update` запись `set` тоже непустая.
Обе операции возвращают только счётчик:
`let n: i64? = response_affected(resp, "u");` — сколько строк изменено
или удалено.

Процедура может вызвать другую хранимую процедуру `shamir`:
`let total: i64 = call("sum_orders", { user_id: id });`. Имя — строковый
литерал. Компилятор сверяет аргументы и тип результата с подписью цели,
которую сохранил при её создании. Подписи видны компилятору только для
процедур, на которые у создателя есть право `Execute`. Нет цели и нет
права — одна и та же ошибка `T004`, по ней нельзя отличить один случай от
другого. При вызове право и подпись проверяются заново; глубина вложенных
вызовов ограничена 32, рекурсия запрещена.

Значение, прочитанное раньше в том же батче, можно передать в следующую
операцию без возврата в процедуру — через скалярную ссылку:

```text
const b0 = batch_read(batch(), "users", "users", { id: user_id });
const row: { name: string } = { name: query_ref_field(b0, "users", 0, "name") };
const resp = db_execute(request(batch_insert(b0, "audit", "audit", row)));
```

Ссылка указывает на поле `[n].field` чтения из того же плана. Она стоит
полем записи в `const` с явным типом, а сама запись целиком идёт в фильтр
равенства чтения или в документ вставки. Если в момент исполнения строки
или поля нет, значение `null` или тип не совпал, операция со ссылкой
пропускается. Процедура тогда завершается ошибкой вида 2
`batch reference did not resolve to the expected scalar`.

Атомарность ограничена одним выполнением: `batch_tx()` делает транзакционным
ровно один вызов `db_execute(request(...))`; все записи этого батча
фиксируются вместе или откатываются вместе. Обычный `batch()` остаётся
нетранзакционным. Прямые `db_get`, `db_insert` и `db_query` выполняются с
autocommit. `call(...)`/`call_checked` и отдельные вызовы `db_execute` не
объединяются общей транзакцией процедуры: каждый `db_execute` плана
`batch_tx()` выполняется в собственной транзакции, а `db_execute` плана
`batch()` остаётся нетранзакционным. Изоляция `batch_tx()` — Snapshot;
конкурирующие записи разрешаются по last-writer-wins, поэтому read-then-write
не является serializable. Таймаут процедуры не доказывает rollback: если
отмена пришлась на commit, транзакция могла уже зафиксироваться, хотя гостевой
вызов сообщил таймаут.

### Сухая проверка: `check_function`

`check_function` — сухой прогон адмиссии: проходит весь конвейер (скан
лимитов, набор возможностей, allowlist импортов, контрактная проверка,
компиляция) с запрошенным `optimize`. Вердикт `never` может отличаться
от `auto`, когда Winch отвергает модуль. Ничего не сохраняется: ни строки
в каталоге, ни записи в инкрементальном кэше компиляции. Ошибка приходит
как error-результат. При успехе — запись:

```ts
const resp = await Batch.create('check-fn')
  .add('c', ddl.checkFunction({ wasm: '<base64>' }))
  .execute(client, 'my_app');
resp.results.c.records[0]; // { ok: true, size, abi, origin, hash, functions, imports, exports }
```

### Смена режима: `alter_function`

`alter_function` меняет **только** `optimize` (пересборка + перерегистрация;
для `now` — синхронная компиляция на Cranelift). Гранты, `security` и
`visibility` меняются через `create_function` с `replace` (и его HMAC) или
через существующий `chmod`. HMAC у `alter_function` нет — это режим
производительности.

```ts
await Batch.create('alter-fn')
  .add('a', ddl.alterFunction('my_fn', 'now'))
  .execute(client, 'my_app');
```

### Лимиты приёма и разрешённые возможности WASM

Гейтов два набора. **Адмиссия** (create, replace, `check_function`,
`alter_function` при пересборке) прогоняет всё перечисленное ниже, включая
операторные и временной лимиты. **Загрузка** (boot, apply на реплике) —
только детерминированные гейты (парсинг, allowlist импортов, контракт,
набор возможностей): операторные и временной лимиты не применяются, поэтому
сохранённая строка грузится одинаково на каждом узле.

Лимиты адмиссии (значения по умолчанию, включительные верхние границы):

|| Лимит | Значение |
||---|---|
|| размер модуля | 8 MiB |
|| функций | 20 000 |
|| типов | 5 000 |
|| импортов | 64 |
|| экспортов | 64 |
|| глобальных переменных | 1 000 |
|| локальных переменных на функцию | 10 000 |
|| размер тела функции | 1 MiB |
|| операторов на функцию | 500 000 |
|| глубина вложенности блоков | 256 |
|| целей `br_table` | 4 096 |
|| params+results на тип | 128 |
|| min/max элементов таблицы | 100 000 |
|| element-сегментов | 1 000 |
|| суммарно element-элементов | 200 000 |
|| data-сегментов | 10 000 |
|| суммарно data-байтов | 4 MiB |
|| min страниц памяти | 1 024 |
|| Rust-исходник | 1 MiB |
|| время компиляции | 10 s (отвергает только запрос — структурные лимиты остаются реальной границей) |
|| одновременных компиляций | половина ядер (min 1) |

Custom-секции (`shamir.abi`, `shamir.origin`, `name`, `producers`, DWARF)
разрешены и считаются в лимит размера. На модуль — один вызываемый:
`shamir_call`.

Разрешённый набор возможностей: MVP, sign-extension, non-trapping
float-to-int, bulk memory, multi-value, mutable globals, reference types.
Частичное использование — только одна таблица `funcref`, без `externref`,
без табличных операций кроме `call_indirect`. Отвергаются: threads, SIMD,
GC, exceptions, tail calls, memory64, multi-memory, component model.

### Права

|| Операция | Право |
||---|---|
|| `describe_function` | `Read` на функцию (legacy-строки открыты, как все остальные права на них) |
|| `export_function` | `Write` на функцию (байты выдают логику; legacy-строки без режима открыты — это заявлено открыто, а не спрятано) |
|| `check_function` | `Create` на `FunctionNamespace`, плюс `Execute` на `WasmCompiler` для исходника (`lang = rust` и `lang = shamir`), как у `create_function` (сухая проверка запускает компилятор) |
|| `alter_function` | `Write` на функцию |

По проводу эти операции — административные, как `create_function` и `drop_function`: сервер принимает их
только от суперпользователя, а токен-клиент и REST их отклоняют. Права в таблице проверяет фасад `ShamirDb`
(встраиваемое использование).

## 4. Вызов: `call`

Зарегистрированная функция вызывается через `call(name, params)`:

```ts
import { call, Batch } from '@shamir/client';

const resp = await Batch.create('call-add')
  .add('sum', call('add', [3, 5]))
  .execute(client, 'my_app');

resp.results.sum.value; // { sum: 8 }
```

`value` — произвольный `QueryValue` (object, array, scalar, null), который
вернула функция. `records` — пустой массив (call не читает таблицу).

### Позиционные параметры

`params` — массив. Внутри функции доступен по ключам `"0"`, `"1"`, …
Также передаётся `"args"` — весь массив целиком.

### Зависимые вызовы (`filter.queryRef`)

`call` поддерживает `filter.queryRef`-ссылки, как и другие операции:

```ts
import { call, filter, Batch } from '@shamir/client';

const resp = await Batch.create('chained')
  .add('user', db.query('users').where(filter.eq('name', 'alice')))
  .add('enrich', call('enrich_user', [filter.queryRef('@user', '[0].id')]))
  .run();
```

Планировщик выстроит этапы: `user` → `enrich`.

## 5. Валидаторы

Валидатор — функция с особым контрактом: вызывается **до** записи в
таблицу, может отклонить операцию.

### Создание

```ts
await Batch.create('mk-val')
  .add('v', ddl.createValidator('check_age', { wasm: '<base64>' }))
  .execute(client, 'my_app');
```

Ответ: `{ "created_validator": "check_age", "id": "…" }`.

### Привязка к таблице

```ts
await Batch.create('bind')
  .add('b', ddl.bindValidator('check_age', 'users', ['Insert', 'Update'], 100, {
    db: 'mydb',
    repo: 'main',
  }))
  .execute(client, 'mydb');
```

* `ops` — на какие операции реагировать: `Insert`, `Update`, `Delete`.
* `priority` — порядок срабатывания (меньше = раньше). Валидатор с
  `stop: true` блокирует дальнейшие.

### Механика валидации

1. Запись попадает в батч (insert/update/delete).
2. Движок находит все bound-валидаторы для данной таблицы + op.
3. Вызывает их по возрастанию `priority`.
4. Если валидатор вернул ошибки — запись **отклонена**, в ответе —
  field-bound error codes.
5. Если валидатор установил `stop: true` — последующие валидаторы не
  вызываются.

Пример ответа при ошибке валидации (wire form; MessagePack на проводе):

```
{
  "error": {
    "code": "validation_failed",
    "details": {
      "errors": [
        { "field": ["age"], "code": "too_young" },
        { "field": ["name"], "code": "name_required" }
      ]
    }
  }
}
```

### Отвязка

```ts
await Batch.create('unbind')
  .add('u', ddl.unbindValidator('check_age', { db: 'mydb', repo: 'main', table: 'users' }))
  .execute(client, 'mydb');
```

### Удаление валидатора

Валидатор, который привязан к таблице, удалить нельзя — сначала `unbind`.
Свободный — можно:

```ts
await Batch.create('drop-val')
  .add('d', ddl.dropValidator('check_age'))
  .execute(client, 'mydb');
```

## 6. Setuid (SECURITY DEFINER)

Функция может выполняться с правами **владельца**, а не вызывающего:

```ts
import { admin, Batch } from '@shamir/client';

await Batch.create('chmod-fn')
  .add('cm', admin.chmod(client, admin.refFunction('get_secrets'), 0o4750))
  .execute(client, 'mydb');
```

`0o4750` = setuid-бит + `rwxr-x---`. Когда пользователь `bob` вызывает
`get_secrets`, движок резолвит **effective actor** как владельца функции
(например, `admin`), и чтение таблицы идёт с правами admin — даже если у
bob нет прямого read на `secrets`.

Это «data firewall»: доступ к данным — **только** через процедуру.
Процедура контролирует, что именно возвращается.

| Сценарий | Как |
|---|---|
| bob вызывает setuid-функцию | effective actor = владелец функции |
| bob вызывает обычную функцию | effective actor = bob |
| Валидация внутри setuid | ctx.db() работает от имени владельца |

<!-- TODO: verify setuid bit position in mode integer — see ACCESS_FABRIC.md P5 and tests/functions/facade/getter_only_e2e.rs -->

## 7. Папки функций

Функции можно организовывать в папки (аналог POSIX-директорий):

```ts
await Batch.create('mk-folder')
  .add('f', ddl.createFunctionFolder(['reports', 'daily']))
  .execute(client, 'mydb');
```

Создаёт путь `reports/daily`. Функция `reports/daily/summary` — полное
имя с папкой. На папки действуют `chmod`/`chown` (как на каталоги) —
см. [этаж 4](./04-access.md).

## 8. Встроенная библиотека (funclib)

ShamirDB поставляется с ~120 встроенными скалярами и агрегатами:

|| Категория | Примеры |
||---|---|
|| `math/` | `abs`, `ceil`, `floor`, `round`, `sqrt`, `min`, `max`, `between` |
|| `null/` | `coalesce`, `if_null`, `nullif`, `is_null` |
|| `strings/` | `lower`, `upper`, `trim`, `length`, `contains`, `replace` |
|| `arrays/` | `length`, `contains`, `flatten`, `sort` |
|| `cast/` | `to_int`, `to_string`, `to_float`, `to_bool` |
|| `datetime/` | `now`, `format`, `parse` |
|| `value_nav/` | `get_path`, `array_length`, `keys`, `type_of` |
|| `validate/` | `is_email`, `is_uuid` |
|| `encode/` | `base64_enc`, `hex_enc`, `to_json`, `parse_json` |
|| `gen/` | `uuid_v4`, `random`, `random_bytes` |
|| `object/` | `keys`, `values`, `merge`, `pick`, `omit` |
|| `text/` | `slugify`, `levenshtein`, `word_count` (без `camel`/`snake` — не реализованы) |
|| `crypto/` | `sha256`, `hmac_sha256` |

Функции `datetime/` принимают гражданские годы от -9999 до 9999; директивы смещения, не стоящие в конце шаблона `parse`, обрабатываются согласно правилам Jiff.

Сравнение (`>`, `<`, `=`, `between`) в фильтрах выполняется через операторы `$gt`/`$lt`/`$eq` в `WHERE`; отдельной папки `compare/` в реестре нет.

Используются в запросах через `$fn` (вычисляемые поля и фильтры), в
`GROUP BY` (агрегаты: `median`, `percentile`), в `SELECT` (проекции).

## 9. Жизненный цикл

```
ddl.createFunction  →  call(...)  →  ddl.renameFunction  →  call(...)  →  ddl.dropFunction
ddl.createValidator →  ddl.bindValidator → (validates inserts) → ddl.unbindValidator → ddl.dropValidator
```

* `ddl.renameFunction` / `ddl.renameValidator` — атомарное переименование.
* `ddl.listFunctions()` / `ddl.listValidators_()` — интроспекция через `access_tree`.
* Персистентность: функции и валидаторы хранятся в каталоге (catalogue),
  переживают рестарт сервера.

## Что важно знать уже сейчас (дозированно)

* **WASM — sandboxed.** Функция не может выйти за пределы хост-импортов.
  Побочные эффекты — только через `ctx`: БД, HTTP-эгресс, globals.
* **Toolchain на сервере — опционален.** Компиляция из исходника —
  experimental и работает только на dev-машине с checkout репозитория; в
  проде отправляй pre-compiled `.wasm`.
* **Функция — ресурс Shomer.** На неё действуют `chmod`/`chown`/`chgrp`
  (этаж 4). Setuid — это setuid-бит в mode.
* **Валидатор — fail-closed.** Если bound-валидатор не найден в registry,
  insert/update abort с ошибкой `ValidatorInvalid`.
* **`call` — в графе зависимостей.** Можно строить цепочки:
  read → call → read, с `filter.queryRef`-ссылками между этапами.

## Куда дальше

|| Упёрся в… | Поднимайся на |
||---|---|---|
|| «нужен полнотекстовый или векторный поиск» | [Этаж 6 — Поиск](./06-search.md) |
|| «выкатываю в прод, нужны метрики и сервис» | [Этаж 7 — Эксплуатация](./07-operations.md) |
