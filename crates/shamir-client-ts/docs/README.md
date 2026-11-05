# @shamir/client

Pure-TypeScript client for **S.H.A.M.I.R. Database** — one shared platform-agnostic core with thin Node.js and Browser adapters over WebSocket + MessagePack + SCRAM-Argon2id authentication. No native bindings required.

For bearer-token connections, token administration limitations, and password login, see [Tokens and token-based connections](./TOKENS.md). Tokens are issued with fixed mode/channel/database scope; `connect` does not widen it. Node IPC `connectLocal` is not available in the browser entry.

---

## Install

```bash
npm i @shamir/client
```

Dependencies (`ws`, `@msgpack/msgpack`, `argon2-browser`, `@noble/*`) are installed automatically.

---

## Quick start (Node)

```ts
import {
  connect,
  Query,
  Batch,
  write,
  ddl,
} from '@shamir/client';

// Connect over WSS with SCRAM-Argon2id auth
const client = await connect({
  host: '127.0.0.1',
  port: 13760,
  username: 'admin',
  password: 'correct horse battery staple',
  tls: { rejectUnauthorized: false }, // self-signed certs
  origin: 'https://127.0.0.1',
});

// DDL: create database + table
await client.execute('default', {
  id: 'setup',
  queries: {
    db: ddl.createDb('my_app'),
  },
});
await client.execute('my_app', {
  id: 'tables',
  queries: {
    repo: { create_repo: 'main' },
    tbl: ddl.createTable('items'),
  },
});

// Insert a record
const ins = await Batch.create('ins')
  .add('i', write.insert('items', [{ id: 'A1', name: 'widget', qty: 10 }]))
  .execute(client, 'my_app');

// Read it back
const resp = await Batch.create('read')
  .add('r', Query.from('items'))
  .execute(client, 'my_app');
console.log(resp.results.r.records); // [{ id: 'A1', name: 'widget', qty: 10 }]

await client.close();
```

### Native transports (Node only): TCP+TLS and native WebSocket

`connect()` and `resume()` accept `transport?: 'ws-browser' | 'tcp' | 'ws-native' | 'tcp-plain'`. The default stays `'ws-browser'` (the browser-profile WebSocket). `'tcp'` connects to a listener `kind: tcp, profile: tls_exporter`; `'ws-native'` connects to `wss://host:port/shamir/v1` (listener `kind: ws, profile: tls_exporter, path: /shamir/v1`, subprotocol `shamir-v1`, no Origin header). `'tcp-plain'` connects to a listener `kind: tcp, profile: plain` — plain TCP with no TLS, loopback only. A non-loopback host is refused client-side, before any socket is opened (same rule as the Rust `connect_plain`); `tls` options throw with this transport. The channel binding is `{ TCP, 0x00 }` with the zero exporter. For `'tcp'` and `'ws-native'` the channel binding is the real TLS exporter (label `EXPORTER-ShamirDB-AUTH-v1`). `changePassword` works over these sessions. `tls.ca` (PEM of the CA certificate to trust) is valid only for `'tcp'` and `'ws-native'`; `tls.rejectUnauthorized` defaults to `true`. Not supported in the browser entry.

Trust model: TLS certificate verification (`rejectUnauthorized: true` plus `ca`) plus the server's Ed25519 identity. Every session handshake verifies `identity_sig`; pass `trustedPin` — the pin of a previous connection — to pin the server (recommended hardening); a mismatch throws `ServerIdentityChangedError` before any data flows. Without a pin the first server is accepted (trust-on-first-use, `acceptNewHost`, default `true`) and its pin is exposed for persistence. Resume over a `tls_exporter` transport additionally verifies a server identity proof before the ticket is sent. See the [client matrix](../../../docs/guide-docs/guide/client-matrix.md). Over `'tcp-plain'` and IPC (binding mode `0x00`) no identity proof is exchanged — the ticket is sent right after the dial. A ticket can be resumed on the same or a STRONGER channel binding but never a weaker one (server anti-downgrade rule: plain `0x00` < browser `0x02` < TLS exporter `0x01`): a plain ticket resumes over TLS TCP, a TLS-exporter ticket is refused over plain TCP. See the [client matrix](../../../docs/guide-docs/guide/client-matrix.md).

```ts
import { connect, resume } from '@shamir/client';
import { readFileSync } from 'node:fs';

const host = '127.0.0.1';
const port = 13760;
const ca = readFileSync('server-cert.pem', 'utf8');

// TCP + TLS 1.3
const client = await connect({
  host, port, username: 'admin', password: 'correct horse battery staple',
  transport: 'tcp',
  tls: { ca },
});

// Native WebSocket (wss://host:port/shamir/v1)
const wsClient = await connect({
  host, port, username: 'admin', password: 'correct horse battery staple',
  transport: 'ws-native',
  tls: { ca },
});

// Resume with a ticket
const ticket = client.resumptionTicket();
const pin = client.serverPubKeyPin();
const resumed = await resume({
  host, port, ticket: ticket!, serverPubKey: pin,
  transport: 'tcp',
  tls: { ca },
});
```

Pinning the server identity (recommended): capture the pin on first contact,
then pass it on every later connection. A mismatch throws
`ServerIdentityChangedError` and no session is established.

```ts
const pin = client.trustedPin(); // SHA256(server_pub_key)
const pinned = await connect({
  host, port, username: 'admin', password: 'correct horse battery staple',
  trustedPin: pin, transport: 'tcp', tls: { ca },
});
```

### Plain TCP (loopback only) and resume over local IPC

```ts
import { connect, resume, resumeLocal } from '@shamir/client';

// Plain loopback TCP (listener `kind: tcp, profile: plain`): no TLS options.
const plain = await connect({
  host: '127.0.0.1', port: 13761,
  username: 'admin', password: 'correct horse battery staple',
  transport: 'tcp-plain',
});

// Resume over plain TCP
const r1 = await resume({
  host: '127.0.0.1', port: 13761,
  ticket: plain.resumptionTicket()!, serverPubKey: plain.serverPubKeyPin(),
  transport: 'tcp-plain',
});

// Resume over local IPC (Unix domain socket / Windows Named Pipe)
const local = await resumeLocal({
  addr: '/run/shamir/shamir.sock', // Windows: '\\\\.\\pipe\\shamir'
  ticket: plain.resumptionTicket()!,
  serverPubKey: plain.serverPubKeyPin(),
});
```

`resumeLocal({ addr, ticket, serverPubKey })` is the Node-only IPC counterpart of `resume` — same options as `connectLocal` plus the ticket and pinned key.

---

## Bound handle (no connection threading)

After `const db = client.db('my_app')`, no call re-threads the connection:

```ts
const db = client.db('my_app');

// Read: one-shot rows or full QueryResult
const rows = await db.query('items').where(filter.eq('id', 'A1')).rows();   // → records[]
const qr   = await db.query('items').where(filter.eq('id', 'A1')).ex();     // → QueryResult

// Write: any op/builder → QueryResult
await db.run(write.insert('items', [{ id: 'B2', qty: 3 }]));
await db.run(write.update('items').where(filter.eq('id', 'B2')).set({ qty: 9 }));

// HMAC-gated DDL — signer injected internally
await db.dropTable('main', 'old_table');
await db.dropIndex('main', 'items', 'by_email', { unique: true });
await db.dropRepo('archive', { cascade: true });
await db.dropDb({ cascade: true });

// Batch with bound queries
const resp = await db.batch()
  .add('users',  db.query('users'))
  .add('orders', db.query('orders').where(filter.eq('user_id', filter.queryRef('@users', '[0].id'))))
  .transactional()
  .run();                                                                   // → BatchResponse
```

`client.db(name)` returns a `Db` instance that holds the connection and database name. All Layer-1 APIs (`filter.*`, `select.*`, `write.*`, `ddl.*`, `Query`, `Batch`) continue to work unchanged — the handle is purely additive.

### Transactions (auto-managed)

```ts
const db = client.db('my_app');

// Auto-commit on resolve; rollback only for callback failure
const receipt = await db.tx(async (t) => {
  await t.run(write.insert('acct', [{ id: 'a', bal: 100 }]));
  await t.run(write.update('acct').where(filter.eq('id', 'a')).set({ bal: 90 }));
  const rows = await t.query('acct').rows();   // reads inside the tx
});
// Inspect receipt.state and receipt.transaction.error; pending is committed.
// Rollback only on callback failure, never after a commit attempt.

// With options
const serializableReceipt = await db.tx(async (t) => { /* ... */ }, { isolation: 'serializable', repo: 'main' });
```

The callback receives a `Tx` handle whose operations route through `txExecute`. If the server reports `status === 'aborted'` at commit time, the wrapper throws an error carrying the reason.

---

## Browser

```ts
import { connect, resume, Query, Batch } from '@shamir/client/browser';

const client = await connect({
  host: 'db.example.com',
  port: 443,
  username: 'reader',
  password: 's3cret',
  // origin defaults to `https://${host}`
});

// Ticket and identity come from a previous browser session:
//   `client.resumptionTicket()` — the ticket for `resume`;
//   `client.serverPubKeyPin()` — the RAW server public key that `resume`
//   expects as `serverPubKey` (binding mode 0x02 has no resume_proof, so
//   this pinned key is the identity check on the resume path);
//   `client.trustedPin()` — the SHA256(server_pub_key) pin for
//   `connect`'s `trustedPin` option. NOT interchangeable: passing the pin
//   as `serverPubKey` (or the key as `trustedPin`) pins the wrong bytes.
const resumed = await resume({
  host: 'db.example.com',
  port: 443,
  ticket,
  serverPubKey: rawServerPubKey,
});
```

**Notes:**
- The browser entry point uses native `WebSocket` and `WebCrypto`/`argon2-browser`.
- There is no `rejectUnauthorized` in browsers — self-signed certificates must be OS-trusted.
- The server requires the `Origin` header to be in its `browser_origin_allowlist`.
- `resume` is exported from the browser entry for the browser WebSocket profile; transports `tcp` / `ws-native` remain Node-only.

---

## Namespace imports

Each builder domain exports a single named namespace object. Import one object per domain and call methods off it:

```ts
import {
  // client
  connect, ShamirClient,
  // query
  Query, Batch,
  // filter
  filter,
  // select
  select,
  // write
  write,
  // ddl
  ddl,
  // admin
  admin,
  // call
  call,
} from '@shamir/client';

// Usage: filter.eq(...), select.field(...), write.insert(...), ddl.createDb(...), admin.chmod(...)
```

Individual function exports are still available for internal / advanced use (they power the namespace objects internally), but the recommended public API is the namespace style shown above.

---

## Query builder (OQL)

```ts
import { Query, filter, select } from '@shamir/client';
```

### from / withRepo

```ts
Query.from('items')                    // default repo ("main")
Query.withRepo('archive', 'orders')    // explicit repo
```

### select — fields and aggregations

```ts
// Column projection
Query.from('orders').select([select.field('user'), select.field('amount')])

// Aggregations
Query.from('orders').select([
  select.countAll('n'),
  select.sum('amount', { alias: 'total' }),
  select.avg('amount', { alias: 'mean' }),
  select.min('amount', { alias: 'lo' }),
  select.max('amount', { alias: 'hi' }),
])

// Library aggregate (e.g. median, stddev)
import { select } from '@shamir/client';
Query.from('orders').select([
  select.aggregateFn('median', 'amount', { alias: 'med' }),
])
```

### where / andWhere with filter constructors

```ts
Query.from('items').where(filter.eq('tag', 'red'))

// AND multiple conditions
Query.from('items')
  .where(filter.gt('qty', 5))
  .andWhere(filter.eq('tag', 'blue'))

// Nested field paths
Query.from('items').where(filter.eq(['addr', 'city'], 'NYC'))

// Combined
Query.from('items').where(
  filter.and([filter.eq('tag', 'blue'), filter.gt('qty', 10)])
)
```

### groupBy / having

```ts
Query.from('orders')
  .groupBy('user')
  .having(filter.gt('amount', 100))
  .select([select.field('user'), select.sum('amount', { alias: 'total' })])
```

### orderByAsc / orderByDesc / orderBy

```ts
Query.from('items').orderByAsc('score')
Query.from('items').orderByDesc('score')

// Multiple sorts
Query.from('items').orderBy([
  { field: ['bucket'], direction: 'asc' },
  { field: ['score'], direction: 'desc' },
])
```

### limit / offset / page

```ts
Query.from('items').orderByAsc('id').limit(5).offset(0)    // limit/offset
Query.from('items').page(1, 20)                             // 1-based page
```

### countTotal

```ts
Query.from('items').limit(3).offset(0).countTotal()
// → resp.results.r.pagination.total_count has the full count
```

### Temporal (MVCC)

```ts
import { atVersion, atTimestamp } from '@shamir/client';

Query.from('items').asOfVersion(42)
Query.from('items').asOfTimestamp(1718000000000)
Query.from('items').history({ from: atVersion(10), to: atVersion(20), limit: 5 })
Query.from('items').withVersion()
```

---

## Writes

```ts
import { write, filter } from '@shamir/client';
```

### insert

```ts
write.insert('items', { id: 'A1', name: 'widget', qty: 10 })
write.insert('items', [{ id: 'A1', name: 'widget' }, { id: 'A2', name: 'gear' }])
```

### update (fluent builder)

```ts
write.update('items')
  .where(filter.eq('id', 'B2'))
  .set({ qty: 7 })
  .returning('changed')
  .build()
```

### upsert

```ts
write.upsert('items', { id: 'A1' }, { id: 'A1', name: 'widget-v2', qty: 99 })
```

### del

```ts
write.del('items', filter.eq('id', 'A1'))
```

---

## Batch

```ts
import { Batch, Query, write, filter } from '@shamir/client';
```

### Basics

```ts
const resp = await Batch.create('my-batch')
  .add('users', Query.from('users'))
  .add('orders', Query.from('orders'))
  .add('products', Query.from('products'))
  .execute(client, 'my_app');

resp.results.users.records;      // typed Record[]
resp.execution_plan;              // string[][] — stages
```

### Transactional

```ts
const resp = await Batch.create('tx')
  .add('ins_items', write.insert('items', [{ name: 'cross-item' }]))
  .add('ins_logs', write.insert('logs', [{ event: 'item_created' }]))
  .transactional()              // or .transactional('serializable')
  .execute(client, 'my_app');

resp.transaction.status;   // 'committed'
resp.transaction.tx_id;    // number
```

### Options

```ts
Batch.create(1)
  .add('q1', Query.from('t'))
  .add('q2', write.insert('t', { id: 1 }), { returnResult: false })
  .add('q3', Query.from('t'), { after: ['q1', 'q2'] })
  .name('debug-label')
  .durability('synced')
  .returnOnly(['q1'])
  .limits({ max_queries: 50, max_dependency_depth: 10 })
  .build()
```

### Query dependencies ($query)

Operations can reference results of earlier aliases via `filter.queryRef()`. Two forms:

**Single value** — extract a scalar from another query's result:

```ts
const db = client.db('my_app');

const resp = await db.batch()
  .add('user', db.query('users').where(filter.eq('name', 'Alice')))
  .add('orders', db.query('orders').where(
    filter.eq('user_id', filter.queryRef('@user', '[0].id')),
  ))
  .run();

resp.results.orders.records;     // Alice's orders
resp.execution_plan.length;       // 2 (user → orders)
```

**Column / IN-expansion** — pluck a whole column into an `in` filter:

```ts
const resp = await db.batch()
  .add('all', db.query('users'))
  .add('their_orders', db.query('orders').where(
    filter.in_('user_id', [filter.queryRef('@all', '[].id')]),
  ))
  .run();

resp.results.their_orders.records;  // every order whose user_id is in the users table
```

`filter.queryRef(alias, path?)` builds `{ $query, path? }`. Omit `path` to pass the full upstream result. The `execution_plan` reflects the dependency graph — independent ops are grouped into one stage, dependent ops form subsequent stages.

### Same-document field references ($ref)

```ts
filter.eq('city', filter.ref(['address', 'city']))  // { $ref: ['address', 'city'] }
```

---

## DDL + the HMAC intent-gate

Non-destructive DDL ops are plain function calls:

```ts
import { ddl } from '@shamir/client';

ddl.createDb('my_app')
ddl.createTable('items', { repo: 'main' })
ddl.createIndex('by_email', 'users', [['email']], { unique: true })
```

**Destructive ops** (`dropTable`, `dropDb`, `dropRepo`, `dropIndex`, `startMigration`, `commitMigration`, `rollbackMigration`) require an HMAC tag derived from the session. Pass the connected `client` as the first `signer` argument — the builder calls `client.hmacTagHex()` internally:

```ts
import { ddl } from '@shamir/client';

// ddl.dropTable(signer, dbInUse, repo, table)
await Batch.create('drop')
  .add('d', ddl.dropTable(client, 'my_app', 'main', 'old_table'))
  .execute(client, 'my_app');

// ddl.dropDb(signer, db, { cascade? })
await Batch.create('drop-db')
  .add('d', ddl.dropDb(client, 'my_app', { cascade: true }))
  .execute(client, 'default');
```

The HMAC is a "did-you-mean-it" guard, not an additional authentication layer. The server rejects destructive ops that arrive without an HMAC (`hmac_required`) or with a wrong one (`hmac_mismatch`).

---

## Access control + RBAC

```ts
import { admin } from '@shamir/client';
```

### ACL (POSIX-style)

```ts
admin.chmod(admin.refTable('my_app', 'main', 'items'), 0o644)
admin.chown(admin.refDatabase('my_app'), 1)
admin.chgrp(admin.refTable('my_app', 'main', 'items'), 2)
```

### Users + roles

```ts
// admin.createRole(name, permissions)
admin.createRole('reader', [
  admin.permission('allow', ['read'], admin.scopeTable('my_app', 'main', 'items')),
])

// admin.createUser(name, password, { roles?, profile? })
admin.createUser('alice', 'password123', { roles: ['reader'] })

// HMAC-gated drops
admin.dropUser(client, 'alice')
admin.dropRole(client, 'reader')

// Grant / revoke
admin.grantRole('reader', 'alice')
admin.revokeRole('reader', 'alice')
```

---

## Interactive transactions

For multi-call (interactive) transactions where you need fine-grained control over begin/commit/rollback:

```ts
// Open a transaction scoped to a repo
const opened = await client.txBegin('my_app', 'main');
// opened.tx_handle, opened.snapshot_version, opened.isolation

// Execute a batch inside the open transaction
await client.txExecute('my_app', opened.tx_handle,
  Batch.create('ins').add('i', write.insert('items', [{ id: 'a', bal: 100 }])).build(),
);

// Commit
const info = await client.txCommit('my_app', opened.tx_handle);
// info.status === 'committed', info.commit_version

// — or abort:
// await client.txRollback('my_app', opened.tx_handle);
```

**Caveat:** a `txExecute` does not see its own uncommitted writes from a prior `txExecute`. Writes become visible only after `txCommit`. If you need read-your-writes, commit then read in a fresh query.

---

## Stored functions

```ts
import { call } from '@shamir/client';

await Batch.create('fn')
  .add('c', call('my_function', [42, 'hello']))
  .execute(client, 'my_app');
```

---

## User provisioning

`createScramUser` creates a user that can authenticate over the wire via SCRAM-Argon2id. Requires a superuser session:

```ts
const created = await client.createScramUser('bob', 'password', ['reader']);
created.name;    // 'bob'
created.user_id;  // Uint8Array(16)
```

The newly created user can immediately open a new connection:

```ts
const bobClient = await connect({
  host: '127.0.0.1',
  port: 13760,
  username: 'bob',
  password: 'password',
  tls: { rejectUnauthorized: false },
});
await bobClient.close();
```

---

## Architecture

The package has a platform-agnostic `core/` layer (types, client logic, builders) and two thin platform adapters (`platform/node.ts` → `ws` + `node:crypto`, `platform/browser.ts` → native `WebSocket` + `WebCrypto`). The entry points `index.ts` (Node) and `browser.ts` wire the shared `ShamirClient` to the appropriate platform. See [`TRANSPORT_SPEC.md`](./TRANSPORT_SPEC.md) for the wire protocol details.

---

## Testing

```bash
npm test
```

Runs [vitest](https://vitest.dev/) with the unit test suite. The live e2e test (`src/__tests__/engine/e2e.test.ts`) spawns a release `shamir-server` binary and is skipped automatically if the binary is absent. To run it:

```bash
cargo build --release -p shamir-server   # needs openssl on PATH
npm test
```
