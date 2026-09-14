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

One document per **(subject, predicate, graph)**, holding every object those
three point at. Every term is written through `term.TermKey`:

```js
{ s: "U:http://example.org/Alice",
  p: "U:http://example.org/knows",
  o: ["U:http://example.org/Bob", "U:http://example.org/Carol"],
  g: "" }                                  // "" is the default graph
```

Not one document per triple — and that is the single most important decision in
this package. See [Atomicity on a standalone mongod](#atomicity-on-a-standalone-mongod).

```js
db.triples.createIndex({s: 1, p: 1, g: 1}, {unique: true, name: "spg"})
db.triples.createIndex({p: 1, o: 1, g: 1}, {name: "pog"})   // multikey over o
db.triples.createIndex({o: 1, g: 1},       {name: "og"})    // multikey over o
db.triples.createIndex({g: 1},             {name: "g"})
```

`spg` identifies a document, which is what makes `Add` an upsert with
`$addToSet` — idempotent without a read — and `Set` a single-document update.
The other two are multikey indexes over the object array, so a bound object
matches by containment.

The cost of the layout is MongoDB's 16 MiB document limit: it caps how many
objects one subject and predicate may have in a single graph, on the order of a
hundred thousand. Exceeding it is a write error reported through the error
handler, not silent loss.

Namespace bindings live in a second collection keyed by prefix.

Two conventions come from goRDFlib (v0.5.3 and later) and are not negotiable if
the store is to behave like the others: a **nil context and `store.DefaultGraph`
mean the default graph**, not every graph, and **any other context names a
graph, a blank node included** (TriG `_:g { }`). `Graph` gives unnamed graphs
the `store.DefaultGraph` identifier and passes it straight through as the
context.

Documents written by earlier versions need no migration: blank node contexts
were always stored under the default graph key.

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
  { $unwind: "$o" },                                  // objects are an array
  { $group: { _id: null, seeds: { $addToSet: "$o" } } },
  { $graphLookup: {
      from: "triples",
      startWith: "$seeds",
      connectFromField: "o",                          // array: every element is followed
      connectToField: "s",
      restrictSearchWithMatch: { p: predKey, g: "" },
      as: "reached" } },
  { $project: { nodes: { $setUnion: ["$seeds",
      { $reduce: { input: "$reached.o", initialValue: [],
                   in: { $setUnion: ["$$value", "$$this"] } } }] } } },
  { $unwind: "$nodes" },
  { $group: { _id: "$nodes" } }
])
```

Collecting the seeds into one array first matters: running `$graphLookup` once
per matching edge would materialize the reached set once per edge and hit the
stage's memory cap at a fraction of the graph size. The final `$unwind` streams
one document per node instead of returning one array, which would be bounded by
the 16 MiB reply limit.

Cycles need no handling — `$graphLookup` never revisits a document.

**When it declines.** `$graphLookup` is capped at 100 MiB per stage and ignores
`allowDiskUse`, so a wide closure (`foaf:knows+` across a social graph) fails
rather than spills. That is designed for: `Reachable` returns an error, and
goRDFlib falls back to its own traversal. The contract is all-or-nothing —
a complete result or an error, never a truncated one — precisely so the caller
can fall back without double-counting.

Because the objects live in an array, the forward direction flattens
`$reached.o` with a `$reduce`; the inverse direction needs no flattening, since
`connectToField` matches an array by containment.

Measured on a laptop against a standalone `mongo:8` in Docker: the closure over
a 2000-edge chain takes ~180 ms, one round trip.

## Atomicity on a standalone mongod

`store.Store` requires `Set` to replace `(s, p, *)` such that a concurrent
reader never sees the moment where the old value is gone and the new one is not
yet there.

With one document per triple that is a delete plus an insert — two documents,
so two operations, so a transaction. **Transactions need a replica set or a
sharded cluster; a standalone `mongod` cannot run them.** A backend built that
way passes its tests against a replica set and loses the guarantee in
production.

Grouping the objects of a subject and predicate into one document turns `Set`
into a single `updateOne`, and MongoDB makes single-document updates atomic on
every deployment, standalone included. That is the whole reason for the layout.

`TransactionAware()` still probes the server and answers honestly — false on a
standalone. Nothing in this package depends on the answer.

The suite runs a standalone deliberately, and
`TestSetIsASingleDocumentUpdate` hammers `Set` from one goroutine while another
reads, asserting the value is never absent.

## Tests

Everything here is an integration test. This is a database driver; a suite that
never points it at a server proves nothing.

```bash
make test                       # starts mongo:8 in Docker via dockertest
MONGO_URI=mongodb://localhost:27017/ make test
```

The container is a **plain standalone**, matching the deployments this backend
targets. Testing against a replica set would let a design that quietly depends
on transactions pass here and fail where it matters.

The load-bearing test is `TestConformance`, which runs goRDFlib's shared
`store/storetest` suite — the same suite the in-memory, Badger, SQLite and
SPARQL-protocol backends run. It passes with **no declared exemptions**, and it
should stay that way: a failure there belongs in this store, not in a
`storetest.Config.Known` entry.

## Compatibility

Requires goRDFlib **v0.2.0 or later**, which is where `store/storetest` and
`store.ReachabilityStore` were introduced.

## License

BSD 3-Clause, matching goRDFlib.
