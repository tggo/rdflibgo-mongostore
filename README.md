# rdflibgo-mongostore

A MongoDB storage backend for [goRDFlib](https://github.com/tggo/goRDFlib).

It implements `store.Store`, so it drops in wherever a store goes — `Graph`,
SPARQL, SHACL, reasoning, the parsers — and adds one thing the other backends
cannot do: SPARQL property paths are answered by MongoDB's `$graphLookup` in a
single round trip instead of by a traversal that ships every candidate edge to
the client.

It is a separate module on purpose. The MongoDB driver is a substantial
dependency and `go.mod` is per-module, so shipping this inside goRDFlib would
put the driver in the dependency graph of everyone who uses the library for
anything.

```bash
go get github.com/tggo/rdflibgo-mongostore
```

## Usage

```go
import (
    "github.com/tggo/goRDFlib/graph"
    "github.com/tggo/goRDFlib/sparql"
    mongostore "github.com/tggo/rdflibgo-mongostore"
)

s, err := mongostore.New(
    mongostore.WithURI("mongodb://localhost:27017"),
    mongostore.WithDatabase("rdf"),
)
if err != nil {
    log.Fatal(err)
}
defer s.Close()

g := graph.NewGraph(graph.WithStore(s))
g.Add(alice, knows, bob)

res, err := sparql.Query(g, `PREFIX ex: <http://example.org/>
    SELECT ?x WHERE { ex:alice ex:knows+ ?x }`)
```

### Options

| Option | Purpose |
|---|---|
| `WithURI(uri)` | connection string; required unless `WithClient` is used |
| `WithDatabase(name)` | database name, default `"rdf"` |
| `WithClient(client)` | use an already-connected driver client; `Close` leaves it alone |
| `WithContext(ctx)` | base context for every operation |
| `WithTimeout(d)` | per-operation deadline, default 30s |
| `WithErrorHandler(fn)` | see below |

### Errors

`store.Store` has no way to return a write error — the interface predates that
concern and every backend in goRDFlib inherits it. This one therefore reports
what it cannot return:

```go
mongostore.WithErrorHandler(func(op string, err error) {
    metrics.StoreErrors.WithLabelValues(op).Inc()
})
```

The default logs through the standard `log` package. Without a handler a
connection failure looks exactly like an empty graph, so it is worth wiring up.

### Plugin registration

The backend registers itself as `"mongo"` with goRDFlib's plugin registry. The
factory signature has nowhere to put a connection string, so it reads one from
`RDFLIBGO_MONGO_URI` (and optionally `RDFLIBGO_MONGO_DB`), and panics if it
cannot connect — a store that silently swallows every write is worse than a
stack trace. For anything beyond a URI, call `New` directly.

## Storage layout

One document per quad, every term written through `term.TermKey`:

```js
{ s: "U:http://example.org/Alice",
  p: "U:http://example.org/knows",
  o: "U:http://example.org/Bob",
  g: "" }                                  // "" is the default graph
```

Indexes, the same SPO/POS/OSP design the other goRDFlib backends use:

```js
db.triples.createIndex({s: 1, p: 1, o: 1, g: 1}, {unique: true, name: "spog"})
db.triples.createIndex({p: 1, o: 1, s: 1, g: 1}, {name: "posg"})
db.triples.createIndex({o: 1, s: 1, p: 1, g: 1}, {name: "ospg"})
db.triples.createIndex({g: 1}, {name: "g"})
```

The unique index is what makes `Add` idempotent without a read: it is an upsert
on the full key.

Namespace bindings live in a second collection keyed by prefix.

Two conventions come from goRDFlib and are not negotiable if the store is to
behave like the others: a **nil context means the default graph**, not every
graph, and a **blank node context means the default graph too** — a blank node
cannot name a graph, and `Graph` passes its own identifier, a blank node when
the graph is unnamed, straight through as the context.

`Collection()` exposes the underlying `*mongo.Collection` for the things
`store.Store` cannot express: a custom aggregation, a backup, an extra index.
Writing through it bypasses every invariant here.

## Property paths through `$graphLookup`

`MongoStore` implements `store.ReachabilityStore`, so goRDFlib's
`paths.MulPath` hands it the transitive part of a property path instead of
walking the graph itself. The shapes that push down:

| SPARQL | pushed down |
|---|---|
| `<p>*`, `<p>+` | yes |
| `^<p>*`, `^<p>+` | yes — the traversal direction flips |
| `(<p1>\|<p2>)+` | yes — `restrictSearchWithMatch` with `$in` |
| `!(<p1>\|<p2>)+` | yes — the same with `$nin` |
| `<p>?` | no; a single optional step is not worth a round trip |
| `<p1>/<p2>+` | only the repeated part; the sequence stays client-side |
| `(<p1>/<p2>)+` | no |

The aggregation, roughly:

```js
db.triples.aggregate([
  { $match: { s: startKey, p: predKey, g: "" } },
  { $group: { _id: null, seeds: { $addToSet: "$o" } } },
  { $graphLookup: {
      from: "triples",
      startWith: "$seeds",
      connectFromField: "o",
      connectToField: "s",
      restrictSearchWithMatch: { p: predKey, g: "" },
      as: "reached" } },
  { $project: { nodes: { $setUnion: ["$seeds", "$reached.o"] } } },
  { $unwind: "$nodes" },
  { $group: { _id: "$nodes" } }
])
```

Collecting the seeds into one array first matters: running `$graphLookup` once
per matching edge would materialize the reached set once per edge and hit the
stage's memory cap at a fraction of the graph size.

Cycles need no handling — `$graphLookup` never revisits a document.

**When it declines.** `$graphLookup` is capped at 100 MiB per stage and ignores
`allowDiskUse`, so a wide closure (`foaf:knows+` across a social graph) fails
rather than spills. That is designed for: `Reachable` returns an error, and
goRDFlib falls back to its own traversal. The contract is all-or-nothing —
a complete result or an error, never a truncated one — precisely so the caller
can fall back without double-counting.

Measured on a laptop against `mongo:8` in Docker: the closure over a 2000-edge
chain takes ~55 ms, one round trip.

## Transactions

`Set` must replace `(s, p, *)` atomically, so that a concurrent reader never
sees the moment where the old value is gone and the new one is not yet there.
That needs a transaction, and transactions need a replica set or a sharded
cluster.

`TransactionAware()` reports what the connected server can actually do, probed
once at construction. On a standalone `mongod` it returns false and `Set` runs
its two operations unprotected, because the server offers no way to close that
window. Run a replica set if it matters — even a single-node one.

## Tests

Everything here is an integration test. This is a database driver; a suite that
never points it at a server proves nothing.

```bash
make test                       # starts mongo:8 in Docker via dockertest
MONGO_URI=mongodb://localhost:27017/?directConnection=true make test
```

The suite runs a **single-node replica set**, not a standalone, so that `Set`'s
transactional path is the one under test rather than the fallback.

The load-bearing test is `TestConformance`, which runs goRDFlib's shared
`store/storetest` suite — the same suite the in-memory, Badger, SQLite and
SPARQL-protocol backends run. It passes with **no declared exemptions**, and it
should stay that way: a failure there belongs in this store, not in a
`storetest.Config.Known` entry.

## Status

The `go.mod` carries a `replace` to a local checkout of goRDFlib, because this
backend depends on `store/storetest` and `store.ReachabilityStore`, which are on
goRDFlib's main branch and not yet in a tagged release. Remove the `replace` and
pin a version once one is cut.

## License

BSD 3-Clause, matching goRDFlib.
