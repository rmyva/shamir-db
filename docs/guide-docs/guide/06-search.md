בְּשֵׁם יהוה הָרַחֲמָן וְהַחַנּוּן

# Этаж 6 — Поиск: полнотекстовый, векторный / embeddings

**Когда подниматься:** нужен поиск / AI-фичи.

До этого этажа запросы к ShamirDB — точные совпадения (`filter.eq`), диапазоны
(`filter.between`), шаблоны (`filter.like`). Но реальные продукты требуют:
«найди документы со словом *rust* и *world*» или «найди записи, похожие
на этот вектор». Этот этаж — про два специализированных вида поиска:
**FTS** (full-text search) и **vector similarity** (ANN через HNSW).

## 1. FTS-индекс: создание

FTS-индекс строится по текстовому полю и ускоряет поиск по словам
(токенам). Создаётся через `ddl.createIndex` с `index_type: 'fts'`:

```ts
import { ddl, Batch } from '@shamir/client';

await Batch.create('mk-fts')
  .add('idx', ddl.createIndex('body_fts', 'posts', [['body']], {
    index_type:    'fts',
    fts_tokenizer: 'whitespace',
  }))
  .execute(client, 'my_app');
```

Ключевые опции:

||| Поле | Описание | Значения |
|||---|---|---|
||| `index_type` | тип индекса | `'fts'` |
||| `fts_tokenizer` | алгоритм токенизации | `'whitespace'`, `'unicode'`, `'stemmed_en'`, `'ngram3'`, … |
||| `fts_language` | подсказка языка (reserved) | строка, пока не влияет на `whitespace` |

### Токенайзеры

||| Токенайзер | Значение | Как работает |
|||---|---|---|
|| **Whitespace** | `'whitespace'` | Разбивает по пробелам → lowercase. Быстро, просто. |
|| **Unicode** | `'unicode'` | Unicode-aware границы слов (буквы/цифры vs punctuation). |
|| **Ngram** | `'ngram2'` … `'ngram9'` | Скользящее окно по символам. Для CJK-языков и substring-search. |
|| **Full (stemming)** | `'stemmed_en'`, `'stemmed_ru'`, … | Полный pipeline: whitespace → lowercase → **stopwords** → **Snowball stemming**. 18 языков. |

Пример stemmed-индекса:

```ts
await Batch.create('mk-stemmed')
  .add('idx', ddl.createIndex('body_stemmed', 'articles', [['content']], {
    index_type:    'fts',
    fts_tokenizer: 'stemmed_en',
  }))
  .execute(client, 'my_app');
```

При stemmed-токенизации:
* слово `"running"` → stem `"run"` → токен хешируется → posting entry.
* стоп-слова (`"the"`, `"a"`, `"is"`, …) удаляются.
* запрос `"was running"` → токены `["run"]` (was — стоп-слово).

### Поддерживаемые языки стемминга

English, Russian, Arabic, Danish, Dutch, Finnish, French, German,
Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, Spanish,
Swedish, Tamil, Turkish. Стоп-словые списки кураторованы для EN и RU.

## 2. FTS-запрос

### AND-режим (все токены должны быть)

```ts
import { filter } from '@shamir/client';

const rows = await db.query('posts')
  .where(filter.fts('body', 'hello world', 'and'))
  .rows();
```

Строка `"hello world"` токенизируется тем же токенайзером, что и индекс,
и движок ищет записи, содержащие **оба** токена.

Ответ с BM25-ранжированием (FTS-индекс использует `index2_ranked`; структура ответа — MessagePack на проводе):

```
{
  "results": {
    "r": {
      "records": [
        { "body": "hello rust world", "$score": 1.82 }
      ],
      "stats": { "index_used": "index2_ranked" }
    }
  }
}
```

`$score` — BM25-оценка (Okapi BM25, k1=1.2, b=0.75). Выше = релевантнее.

### OR-режим (хотя бы один токен)

```ts
const rows = await db.query('posts')
  .where(filter.fts('body', 'apple banana', 'or'))
  .rows();
```

Вернёт записи с `"apple"` ИЛИ `"banana"`.

### Brute-force fallback

Если FTS-индекс **не** создан, `filter.fts` всё равно работает — движок
сканирует таблицу и проверяет каждую запись. Это медленно (O(n)), но
работает для прототипирования. При появлении индекса — автоматически
переключается на индексный путь.

## 3. BM25-ранжирование

FTS-индекс использует алгоритм **Okapi BM25** для ранжирования:

* **TF** (term frequency) — сколько раз токен встречается в документе.
* **IDF** (inverse document frequency) — редкость токена по корпусу.
* **Doc length normalisation** — более короткие документы получают бонус.

Статистика (`doc_count`, `sum_doc_len`) обновляется атомарно при каждой
записи. Результаты отсортированы по убыванию `$score`.

## 4. Vector-индекс: создание

Для поиска «ближайших соседей» по embeddings — HNSW-индекс:

```ts
import { ddl, Batch } from '@shamir/client';

await Batch.create('mk-vec')
  .add('idx', ddl.createIndex('vec_idx', 'docs', [['embedding']], {
    index_type:    'vector',
    vector_dim:    128,
    vector_metric: 'cosine',
  }))
  .execute(client, 'my_app');
```

||| Поле | Описание | Значения |
|||---|---|---|
||| `index_type` | тип индекса | `'vector'` |
||| `vector_dim` | размерность вектора | число (default: 384) |
||| `vector_metric` | метрика расстояния | `'cosine'` (default), `'l2'`, `'dot'` |
||| `vector_quantization` | скалярное квантование (V5.2) | `'sq8'` (opt-in; см. §8) |

> **Одна таблица — один vector-индекс.** На данный момент DDL отказывает в
> создании второго vector-индекса на таблицу, где уже есть vector-индекс,
> независимо от поля или размерности. Причина — инфраструктурная:
> `staged_vectors` в `TxContext` ключуется по **токену таблицы** (а не
> индекса), а post-commit `promote_vectors` разносит один и тот же батч
> векторов по **всем** vector-backend'ам таблицы — два индекса с разной `dim`
> привели бы к `DimMismatch` и провалу промоута. Пока этот конвейер не
> переработан под per-index keying, ограничение держится на уровне DDL
> (внятная ошибка валидации вместо молчаливого падения на коммите). Полная
> поддержка мульти-vector-index — в `docs/dev-artifacts/BACKLOG.md`.


### Метрики

||| Метрика | Формула | Когда |
|||---|---|---|
||| `cosine` | косинусное сходство | embeddings (NLP, vision) — наиболее частый |
||| `l2` | евклидово расстояние | пространственные координаты, физические величины |
||| `dot` | скалярное произведение | преднормализованные векторы |

> `dot` трактуется как inner-product similarity (выше = ближе). Все три
> метрики мапятся HNSW-адаптером внутренне.

Вычисления используют **SIMD**: AVX-512F (16-lane FMA), AVX2+FMA
(8-lane), NEON (aarch64), scalar fallback.

### Двухуровневый поиск (exact vs approximate)

Адаптер автоматически выбирает путь по числу векторов:

* **≤ 256 векторов** (`BRUTE_FORCE_MAX`) — exact brute-force KNN
  (точный, O(n)). Гарантия 100% recall.
* **> 256 векторов** — HNSW (approximate nearest neighbor):
  * `ef_construction: 200`, `M: 16` — параметры графа при постройке.
  * `ef_search: 50` — ширина исследования графа при поиске (build-time
    default; переопределяется per-query — см. §6).
  * Top-k ограничен 10 000 (`MAX_TOPK`, защита от DoS).
  * Мягкое удаление (tombstones) — `delete` не перестраивает граф
    (см. §11).

## 5. Vector-запрос: top-k ближайших

```ts
import { filter } from '@shamir/client';

const rows = await db.query('docs')
  .where(filter.vectorSimilarity('embedding', [0.95, 0.1, 0.0], 5))
  .rows();
```

* Второй аргумент — вектор-запрос (массив float), той же размерности, что `vector_dim`.
* Третий аргумент `k` — сколько ближайших соседей вернуть.

`$score` в результатах — similarity score (чем выше, тем ближе). Для `l2` это
negative distance (0 = идентичные).

Ответ ранжирован по убыванию `$score`. Путь помечен в
`stats.index_used`:

```ts
// bare vectorSimilarity → ранжированный индексный путь
expect(resp.results.r.stats?.index_used).toBe('index2_ranked');
```

## 6. Per-query `ef_search` и `oversample` (V1.1 / V3.1)

Эти опции управляют компромиссом recall/latency **без перестройки индекса** —
точка управления на каждый запрос.

### `ef_search` — ширина исследования HNSW

Чем выше `ef_search`, тем больше кандидатов HNSW рассматривает на каждом слое
графа → выше recall, но выше latency. Передаётся четвёртым аргументом
`opts`:

```ts
// узкий ef — быстро, возможен пропуск далёких соседей
const fast = await db.query('docs')
  .where(filter.vectorSimilarity('embedding', q, 3, { efSearch: 16 }))
  .rows();

// широкий ef — выше recall, дороже
const thorough = await db.query('docs')
  .where(filter.vectorSimilarity('embedding', q, 3, { efSearch: 256 }))
  .rows();
```

Дефолты и ограничения:

* `None` (не задан) → build-time default адаптера (`ef_search: 50`).
* **Clamp server-side:** значение выше `MAX_EF_SEARCH` (10 000) не
  rejected — оно молча ограничивается сверху. `ef_search = 999_999_999`
  ведёт себя идентично `ef_search = 10_000`.
* Неявный минимум — `max(ef_search, k)`: запрашивать `ef_search < k`
  бессмысленно (нельзя вернуть k соседей, исследовав меньше k кандидатов).

Когда крутить: поднимай `ef_search`, если recall@10 на твоём датасете ниже
целевого; опускай, если латентность важнее точности. SQ8-индексы (§8)
обычно хотят чуть больший `ef_search` из-за lossy-дистанции на u8-графе.

### Цепочечный builder `vs()`

Для fluent-стиля есть `vs()` с методами `.efSearch()` / `.oversample()`
(иммутабельный, каждый метод возвращает свежий builder):

```ts
import { filter, vs } from '@shamir/client';

const f = vs('embedding', q, 10).efSearch(400).oversample(2).build();
const rows = await db.query('docs').where(f).rows();
```

### `oversample` — расширение кандидатов для filtered ANN

`oversample` — множитель candidate-widening для **filtered ANN** (§7):
движок запрашивает `k′ = k × oversample` кандидатов, применяет residual-
предикат и при недостатке `k` выживших — ретраит с удвоенным `k′`
(до `MAX_TOPK`).

* Дефолт (`None` для filtered ANN) → `2.0` (`DEFAULT_OVERSAMPLE`).
* Clamp снизу: `< 1.0` поднимается до `1.0` (`MIN_OVERSAMPLE`).
* На **bare** `vectorSimilarity` (без `and`) `oversample` принимается
  на проводе, но не потребляется — расширяет только filtered-путь.

```ts
// bare — oversample принят, эффект отсутствует (нет residual-предиката)
await db.query('docs')
  .where(filter.vectorSimilarity('embedding', q, 2, { oversample: 3.0 }))
  .rows();
```

### Rust-эквиваленты

В Rust-клиенте (`shamir-query-builder`) те же опции доступны через
отдельные конструкторы листьев:

```rust
use shamir_query_builder::filter;

// ef_search только
let f = filter::vector_similarity_ef("embedding", q.to_vec(), 10, 400);

// ef_search + oversample вместе
let f = filter::vector_similarity_opts(
    "embedding", q.to_vec(), 10, Some(400), Some(2.0),
);
```

## 7. Filtered ANN: `and(vectorSimilarity, предикат)` (V3.1 / V3.2)

Фильтрованный ANN — гибрид векторного поиска с обычным предикатом: «дай
ближайшие k векторов СРЕДИ записей, удовлетворяющих фильтру». Форма
запроса — ровно `and` с **одной** `vector_similarity` и одним или
несколькими residual-предикатами:

```ts
const rows = await db.query('docs')
  .where(filter.and(
    filter.vectorSimilarity('embedding', q, 3),
    filter.eq('group', 'g1'),
  ))
  .rows();

expect(resp.results.r.stats?.index_used).toBe('filtered_vector_scan');
```

Гарантии:
* Каждая возвращённая запись прошла residual-предикат.
* Если предикат ничего не матчит — запрос терминирует с 0 записей
  (без бесконечного ретраита, без зависания).
* `stats.index_used == 'filtered_vector_scan'` (в отличие от
  `'index2_ranked'` у bare vector).

### Выбор пути по статическим порогам (V3.2)

Движок сам выбирает один из трёх внутренних путей в зависимости от
селективности residual-предиката (см. `docs/dev-artifacts/design/vector-compaction.md`,
`docs/dev-artifacts/benchmarks/vector/2026-07-05-filtered-ann.md`):

||| Путь | Когда выбирается | Как работает |
|||---|---|---|
||| **pre-filter** | residual resolve'ится во вторичный индекс и кандидатов мало (≤ `PRE_FILTER_MAX_CANDIDATES` = 4096) | SIMD brute-force по разрешённому RID-множеству (exact) |
||| **post-filter** | индекса на residual нет ИЛИ кандидатов много | HNSW с `k′ = k × oversample`, затем residual-фильтр + ретраит |
||| **co-filter** | residual селективен умеренно (≤ `CO_FILTER_MAX_SELECTIVITY` = 0.20) | HNSW-обход с allow-set (graph traversal с reject'ом не-разрешённых узлов) |

Эмпирические кроссоверы (n=10K, dim=128, из бенчмарка): pre-filter
выигрывает до ~5–10% селективности, post-filter — на 10–25%, co-filter
конкурентен только на ≥25–50%. Пороги статичны и подобраны на одном
датасете (n=10K, dim=128); на нём co-filter в своей зоне (≤20%) был самым
медленным путём — пороги требуют перекалибровки на больших n. **Клиенту выбирать путь не нужно** —
планировщик делает это автоматически.

### Oversample для filtered ANN

Per-query `oversample` (§6) управляет шириной candidate-set именно на
post-filter пути:

```ts
// более агрессивный oversample → выше recall при сильной селективности
const f = filter.and(
  filter.vectorSimilarity('embedding', q, 3, { efSearch: 128 }),
  filter.eq('group', 'g1'),
);
```

## 8. SQ8-квантование (V5.2)

`vector_quantization: 'sq8'` включает скалярное квантование: каждый `f32`
компонент ужат до `u8` (256 уровней), адаптер держит u8-коды вместо
полных векторов + два dual-graph режима.

```ts
await Batch.create('mk-sq8')
  .add('idx', ddl.createIndex('vec_sq8', 'docs', [['embedding']], {
    index_type:          'vector',
    vector_dim:          128,
    vector_metric:       'cosine',
    vector_quantization: 'sq8',
  }))
  .execute(client, 'my_app');
```

### Deferred-fit (двухфазный переход)

Адаптер не квантует сразу — он ждёт накопления данных:

* **Ниже `FIT_THRESHOLD` (256 векторов):** работает в f32 режиме (exact
  brute-force, гарантия recall).
* **На пороге 256:** quantizer обучается на текущем датасете (per-dim
  `mins`/`scales`), строится u8 HNSW-граф, и адаптер переключается на
  квантованный путь. После переключения f32-граф **освобождается**
  (#418) — память экономится по-настоящему, а не маскируется.

> **Fit — одноразовый.** Quantizer обучается ровно ОДИН раз — на первых
> 256 векторах, накопленных к моменту перехода порога, — и далее **не
> переобучается** при дрейфе распределения. Если реальные данные со временем
> уходят от того распределения, на котором квантователь фитился (другие
> magnitudes, смещённые средние), точность u8-кодов падает — `mins`/`scales`
> перестают покрывать актуальный диапазон, и lossy-дистанция деградирует.
> На практике это ОК для статичных embedding-моделей (распределление
> зафиксировано моделью); для дрейфующих доменов единственный рычаг сегодня —
> дропнуть и пересоздать индекс (переобучение с нуля). Рефит «на ходу» — в
> `docs/dev-artifacts/BACKLOG.md`.


### Dequant-rescore

Квантованная дистанция lossy, поэтому на каждом поиске: u8-граф
возвращает overscan-кандидатов (`16k+64`), каждый кандидат
**деквантуется** и переранжируется точной f32-дистанцией (`O(dim ·
overscan)`). Это сохраняет recall, но делает sq8 ~3× медленнее f32 на
малых n (n=1200) — кроссовер ожидается на больших датасетах.

### Цифры (n=1200, dim=128, cosine)

Из `docs/dev-artifacts/benchmarks/vector/2026-07-05-quantization.md` (QUICK mode,
f32-граф освобождён post-fit):

||| Метрика | f32 | sq8 |
|||---|---|---|
||| recall@10 | 1.000 (baseline) | **0.978** (≈ 2% drop) |
||| RSS footprint | ~9.4 MiB | **~2.9 MiB** (median ratio ~0.29) |
||| QPS (n=1200) | ~1400 | ~480 (~3× медленнее) |

Экономия памяти: sq8 занимает ≈ 25–44% от f32 footprint (медиана ~29%,
теоретический предел 4× = 25%; разброс — аллокатор-фрагментация на
Windows). Recall@10 ≈ 0.978 — в пределах ≤5% цели.

> Когда включать sq8: индекс **в RAM** и экономия памяти важнее
> пиковой QPS. На малых датасетах (< FIT_THRESHOLD) квантование
> не активируется вообще — адаптер остаётся f32.

## 9. Персистентность и crash-recovery (V2.1–V2.4)

Векторный индекс — durable: граф переживает рестарт процесса и crash.
Стек персистентности состоит из трёх слоёв.

### Снапшот (snapshot codec v2)

Граф HNSW дампится в info_store в трёх видах записей под generation `N`:

* **Chunks** — файлы дампа `hnsw_rs` (`.graph` / `.data`), нарезанные на
  ~1 MiB куски с per-chunk crc32.
* **Sidecar** — `MetaEnvelope`-обёрнутый bincode: карты адаптера
  (`rid_map`, `tombstones`, `vectors`), build-параметры, cross-section
  crc32 и (v2) **`QuantMeta`** — замороженные параметры SQ8-квантизатора
  (`mins`/`scales`/`dim`/`method`), чтобы после рестарта квантование
  восстановилось без переобучения.
* **Manifest** — единственный источник правды «какой generation живёт».

`SNAPSHOT_FORMAT_VERSION` сейчас `2`; поддерживаются версии `[1, 2]`
(back-compat со старыми снапшотами).

### Delta-log + generation-flip

Между снапшотами tx-мутации пишутся в delta-log (построчно).
`delta_applied_upto` в manifest'е фиксирует, сколько delta-chunk'ов уже
абсорбировано текущим generation. При накоплении delta движок:

1. Дампит свежий снапшот (gen `N+1`).
2. Атомарно инвертирует manifest через `flip_generation` (одна
   `Store::transact`) → gen `N+1` становится live, поглощённые delta
   (индекс `< new_delta_applied_upto`) прунятся.

### Cold-start поведение

При открытии таблицы (`VectorBackend::restore_on_open`) движок выбирает
путь:

* **Снапшот валиден** → `load_snapshot` восстанавливает граф из dump'а
  за O(dump-size). Сканирование строк данных НЕ выполняется.
  (n=100K, dim=128: load ~3.4с vs full-rebuild ~16.4с — **4.8× быстрее**.)
* **Снапшот отсутствует/повреждён** → `rebuild`: полный O(rows × dim)
  scan data-store + re-insert каждого вектора в свежий граф.

Crash-safety (V2.4): любая порча снапшота (truncated chunk → crc mismatch,
garbage manifest, `hnsw_rs` version mismatch) маршрутизируется в
warn+rebuild без abort'а open'а. Это проверяется двумя РАЗНЫМИ тестами
(`crates/shamir-vector/src/tests/lifecycle/crash_recovery_tests.rs`, #1070):
fidelity — снапшот-дамп и загрузка одного и того же графа возвращают
ИДЕНТИЧНЫХ соседей и дистанции (детерминированная проверка, нулевой допуск);
и quality — recall@10 свежепостроенного графа (3K векторов, не 10K) против
brute-force ground truth, порог ≥ 0.90. Порог подтверждён кросс-платформенным
прогоном (`.github/workflows/hnsw-recall-matrix.yml`, вручную запускаемый) —
60 прогонов (20 × Linux/macOS/Windows) дали минимум 0.968, максимум 0.998.

## 10. Транзакции и FTS/Vector

Индексы FTS и Vector — **проекции** (overlay), как и обычные индексы.
Внутри транзакции:

* Вставленные записи немедленно видны векторному поиску **внутри tx**
  (staged vectors).
* При коммите — векторы промотируются в HNSW-граф.
* При rollback — staged vectors отбрасываются.

Delta-log (§9) пишется **только** на tx-commit path (`apply_committed_vectors`).
Non-tx мутации (прямой CRUD, репликация) применяются напрямую в live-
адаптер и попадают в персистентность только через следующий снапшот /
rebuild при crash.

## 11. Компакция tombstone (V4.2)

Удаление вектора — soft: internal id помечается в `deleted` (`scc::HashMap`),
а HNSW-граф продолжает хранить мёртвый узел. Со временем доля tombstone
растёт, ухудшая recall и раздувая memory footprint.

### Когда срабатывает

Фоновая компакция триггерится после ack-пути мутации, если:

* `deleted_ratio() >= VECTOR_COMPACTION_RATIO_THRESHOLD` (**0.30**), И
* `live_count() >= VECTOR_COMPACTION_MIN_LIVE` (**1000**) — tiny-индексы
  не компактируются (оверхед не оправдан).

### Что происходит

1. Строится **свежий** `HnswAdapter` без tombstone'ов (rebuild-aside).
2. На время постройки каждая hot-path мутация **double-write'ится**
   в новый адаптер (`compaction_target`, lock-free `ArcSwapOption`,
   ~0 ns overhead когда компакция не идёт).
3. Live-set из старого адаптера backfill'ится в новый с семантикой
   `INSERT-IF-ABSENT` (double-write-значения новее — не перезаписываются).
4. Атомарный swap через `ArcSwap<AdapterSlot>` (RCU) — новый адаптер
   становится live без блокировки читателей.
5. Форс-снапшот от нового адаптера + сброс `delta_count`.

Single-flight: одновременно идёт **либо** компакция, **либо** фоновый
снапшот (координация через `compaction_in_flight` / `snapshot_in_flight`
`AtomicBool`).

## 12. Полный пример: FTS + vector вместе

```ts
import { ddl, write, filter, Batch } from '@shamir/client';

// Создание таблицы + двух индексов
await Batch.create('setup')
  .add('mk-table', ddl.createTable('articles', { repo: 'main' }))
  .add('fts-idx', ddl.createIndex('title_fts', 'articles', [['title']], {
    index_type:    'fts',
    fts_tokenizer: 'stemmed_en',
  }))
  .add('vec-idx', ddl.createIndex('embedding_idx', 'articles', [['embedding']], {
    index_type:    'vector',
    vector_dim:    3,
    vector_metric: 'cosine',
  }))
  .execute(client, 'my_app');

// Вставка данных
await Batch.create('insert')
  .add('a1', write.insert('articles', [
    { title: 'Rust programming language', embedding: [1.0, 0.0, 0.0] },
  ]))
  .add('a2', write.insert('articles', [
    { title: 'Python for data science', embedding: [0.0, 1.0, 0.0] },
  ]))
  .execute(client, 'my_app');

// Поиск по тексту
const textRows = await db.query('articles')
  .where(filter.fts('title', 'programming', 'and'))
  .rows();

// Поиск по вектору
const vecRows = await db.query('articles')
  .where(filter.vectorSimilarity('embedding', [0.9, 0.1, 0.0], 2))
  .rows();
```

## 13. Functional-индекс (бонус)

Третий вид `index2` — functional: индекс по **результату функции** над
полем. Пример: case-insensitive поиск по email:

```ts
await Batch.create('mk-func-idx')
  .add('idx', ddl.createIndex('email_lower', 'users', [['email']], {
    index_type:    'functional',
    functional_op: 'lower',
  }))
  .execute(client, 'my_app');
```

Запрос через computed-фильтр:

```ts
const rows = await db.query('users')
  .where(filter.computed('lower', 'email', 'eq', 'alice@foo.com'))
  .rows();
```

Движок применяет `lower(email)` к каждой записи (или через индекс, если
он есть) и сравнивает результат с `"alice@foo.com"`.

## Что важно знать уже сейчас (дозированно)

* **FTS без индекса — работает, но медленно.** Создай индекс, как только
  данных становится больше нескольких сотен записей.
* **HNSW — approximate.** Recall@10 на 1K векторах — наблюдаемый диапазон
  90–100% (60 прогонов, 3 платформы: 58 × 100%, 2 × 90%,
  `.github/workflows/hnsw-recall-matrix.yml` запуск 38074322496; тест —
  `recall_at_10_on_1k_vectors`, один запрос). Для exact KNN
  на малых данных (≤256) — автоматический brute-force.
* **SQ8-квантование** (`vector_quantization: 'sq8'`) занимает 25–44% RAM от
  f32 (измерено на n=1200) при ~2% loss recall@10 — включай, если индекс в
  RAM и память важнее
  пиковой QPS (см. §8).
* **Per-query `efSearch` / `oversample`** — точка управления recall /
  latency без перестройки индекса (см. §6). `efSearch` clamp'ится до
  `MAX_EF_SEARCH` (10 000).
* **Filtered ANN** (`and(vectorSimilarity, предикат)`) — автоматический
  выбор pre/post/co-filter по статическим порогам (см. §7).
* **Размерность — fixed.** `vector_dim` задаётся при создании индекса и
  не меняется. Все записи должны иметь вектор той же размерности.
* **Vector-индекс** переживает crash через snapshot v2 + delta-log +
  rebuild-fallback (§9). **У FTS- и functional-индексов снапшотов нет** —
  они перестраиваются из данных при каждом открытии таблицы (рестарт стоит
  O(rows × индексов)). Компакция tombstone (§11) предотвращает деградацию
  recall от накопленных удалений.
* **`$score` — не persist.** Это runtime-оценка, вычисленная при запросе.
  Не хранится в записи.

## Куда дальше

||| Упёрся в… | Поднимайся на |
|||---|---|---|
||| «выкатываю в прод, нужны метрики и сервис» | [Этаж 7 — Эксплуатация](./07-operations.md) |
||| «нужна децентрализация / P2P» | [Этаж 8 — Interconnect](./08-interconnect.md) |
