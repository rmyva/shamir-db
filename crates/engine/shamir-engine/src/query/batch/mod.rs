//! Batch query execution module.
//!
//! **Batch — единая точка входа для всех запросов к S.H.A.M.I.R. Database.**
//!
//! Этот мод предоставляет унифицированный msgpack-интерфейс для выполнения запросов.
//! Используйте Batch API для всех операций — от простых запросов до сложных
//! многозапросных транзакций с зависимостями.
//!
//! # Почему Batch?
//!
//! | Подход | Пример |
//! |--------|--------|
//! | Один запрос | `{ "queries": { "q": { "from": "table" } } }` |
//! | Несколько запросов | Map с автоматическим параллелизмом |
//! | С зависимостями | `{ "$query": "@users", "path": "[0].id" }` |
//!
//! ## Преимущества
//!
//! - **Единый формат** — все запросы через QueryValue / msgpack
//! - **Автоматический параллелизм** — независимые запросы выполняются одновременно
//! - **Ссылки на результаты** — используй результаты одного запроса в другом
//! - **Валидация** — проверка зависимостей, циклов, лимитов
//! - **Транзакции** — опциональная MVCC изоляция
//!
//! # Quick Start
//!
//! ## Простой запрос (аналог обычного Query)
//!
//! ```text
//! {
//!   "queries": {
//!     "users": {
//!       "from": "users",
//!       "where": { "op": "eq", "field": "status", "value": "active" },
//!       "limit": 10
//!     }
//!   }
//! }
//! ```
//!
//! ## Запросы с зависимостями
//!
//! ```text
//! {
//!   "queries": {
//!     "user": { "from": "users", "where": { "op": "eq", "field": "id", "value": 123 } },
//!     "orders": {
//!       "from": "orders",
//!       "where": {
//!         "op": "eq",
//!         "field": "user_id",
//!         "value": { "$query": "@user", "path": "[0].id" }
//!       }
//!     }
//!   }
//! }
//! ```
//!
//! # Query Reference Syntax
//!
//! Ссылка на результаты другого запроса в том же batch'е — пара
//! `{ "$query": "@<alias>", "path": "<path>" }`. Префикс `@` —
//! явный маркер reference (отличает от литеральной строки); сервер
//! стрипает его перед lookup в queries map.
//!
//! | Syntax                                              | Description |
//! |-----------------------------------------------------|-------------|
//! | `{ "$query": "@users" }`                            | Весь массив результатов |
//! | `{ "$query": "@users", "path": "[0]" }`             | Первая запись |
//! | `{ "$query": "@users", "path": "[0].name" }`        | Поле первой записи |
//! | `{ "$query": "@users", "path": "[].id" }`           | Колонка id из всех записей (для `in`) |
//!
//! # Execution Plan
//!
//! Планировщик автоматически создаёт стадии для параллельного выполнения:
//!
//! ```text
//! queries: { users, products, orders, stats }
//! dependencies:
//!   users    -> {}
//!   products -> {}
//!   orders   -> {users, products}
//!   stats    -> {orders}
//!
//! stages: [[users, products], [orders], [stats]]
//! ```
//!
//! Stage 1: `users` и `products` параллельно
//! Stage 2: `orders` после Stage 1
//! Stage 3: `stats` после Stage 2
//!
//! # Полная документация
//!
//! См. [README.md](./README.md) для:
//! - Полного описания формата запроса
//! - Всех операторов фильтрации
//! - Лимитов безопасности
//! - Примеров использования
//! - Архитектуры модуля
//!
//! # Example: E-commerce Dashboard
//!
//! ```text
//! {
//!   "name": "dashboard",
//!   "queries": {
//!     "user": {
//!       "from": "users",
//!       "where": { "op": "eq", "field": "id", "value": 123 }
//!     },
//!     "orders": {
//!       "from": "orders",
//!       "where": {
//!         "op": "eq",
//!         "field": "user_id",
//!         "value": { "$query": "@user", "path": "[0].id" }
//!       }
//!     },
//!     "items": {
//!       "from": "order_items",
//!       "where": {
//!         "op": "in",
//!         "field": "order_id",
//!         "values": [{ "$query": "@orders", "path": "[].id" }]
//!       }
//!     },
//!     "products": {
//!       "from": "products",
//!       "where": {
//!         "op": "in",
//!         "field": "id",
//!         "values": [{ "$query": "items[].product_id" }]
//!       }
//!     }
//!   },
//!   "return_all": true
//! }
//! ```

mod control;
mod exec;
mod fk;
mod runner;

pub(crate) use control::implicit::{
    begin_implicit_batch_tx, begin_implicit_batch_tx_with_isolation, commit_implicit_batch_tx,
    run_implicit_batch_tx,
};
pub(crate) use control::receipts;
use control::{authorized, op_watchdog};
pub(crate) use exec::exec_scope;
use exec::{batch_execute, batch_validate, execution_deadline, executor_traits, interactive_tx};
pub use fk::reverse_fk_scan::build_reverse_fk_entries;
use fk::{fk_actions, fk_on_update, fk_restrict};
use runner::{param_subst, query_runner, query_value_serializer, result_size};
// Only the executor (which actually drives a TableManager) lives here.
// DTOs, the topological planner, and the `$query` reference parser are
// all pure-data and live in `shamir-query-types::batch` — re-exported
// here so callers keep using `shamir_db::query::batch::*` paths.
pub use authorized::{AccessGate, Authorized};
pub use batch_execute::execute_batch;
pub use exec_scope::ExecScope;
pub use execution_deadline::ExecutionDeadline;
pub use executor_traits::{
    AdminExecutor, BatchResolver, BatchTxPort, FunctionInvoker, TableResolver,
};
pub use interactive_tx::{commit_interactive_tx, execute_in_open_tx, open_interactive_tx};
pub use query_runner::QueryRunner;
pub use shamir_query_types::batch::{
    collect_required_access, BatchError, BatchLimits, BatchOp, BatchPlan, BatchPlanner,
    BatchRequest, BatchResponse, InternerDelta, QueryEntry, QueryPath, QueryReference,
    ReferenceParseError, ResultEncoding, TransactionInfo,
};

// Retained for engine-level unit tests; not accessible outside test builds.
// `execute_batch_unchecked`/`execute_in_open_tx_unchecked` (#1199) preserve
// the pre-token call shape for tests that exercise execution mechanics, not
// authorization — see their doc comments in `batch_execute.rs`/`interactive_tx.rs`.
#[cfg(test)]
pub(crate) use batch_execute::{execute_batch_unchecked, execute_batch_with_permissions};
#[cfg(test)]
pub(crate) use interactive_tx::execute_in_open_tx_unchecked;

// Test-only export for watchdog thread-leak regression test (#1105).
// `IN_ITER_SYNC` additionally supports the #1200 lock-scope regression test.
#[cfg(test)]
pub(crate) use op_watchdog::{register_op_watchdog, IN_ITER_SYNC, REGISTRY, THREAD_SPAWN_COUNT};

// F-40: expose the two FK fail-closed hooks (and the op-kind enum
// `implicit_tx_isolation_for_fk_parent` takes) so the fail-closed test
// suite (`tests/fk_fail_closed_tests.rs`) can drive them directly with an
// injectable-failure resolver, instead of having to reproduce the whole
// `execute_batch` call sequence just to reach the error branches. Mirrors
// the `execute_batch_with_permissions` re-export pattern above.
#[cfg(test)]
pub(crate) use query_runner::{
    implicit_tx_isolation_for_fk_parent, require_footprint_if_fk_child, FkParentOpKind,
};

#[cfg(test)]
mod tests;
