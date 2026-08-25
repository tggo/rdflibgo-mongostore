// Package mongostore implements the goRDFlib store.Store interface on top of
// MongoDB.
//
// It lives outside the goRDFlib repository on purpose. The MongoDB driver is a
// substantial dependency, and go.mod is per-module, so shipping this backend in
// the main repository would put the driver in the dependency graph of everyone
// who uses goRDFlib for anything. As a separate module it costs nothing to
// those who do not import it.
//
// # Storage layout
//
// Triples live in one collection, one document per (subject, predicate, graph),
// holding every object those three point at. Every term is written through
// term.TermKey:
//
//	{ s: "U:http://example.org/Alice",
//	  p: "U:http://example.org/knows",
//	  o: ["U:http://example.org/Bob", "U:http://example.org/Carol"],
//	  g: "" }
//
// It is not one document per triple, and the reason is atomicity. store.Store
// requires Set to replace (s, p, *) without a concurrent reader ever seeing the
// interval where the old value is gone and the new one is not yet there. Across
// several documents that needs a transaction, and a standalone mongod cannot
// run one — transactions require a replica set or a sharded cluster. Grouped
// this way, Set is a single-document update, which MongoDB makes atomic on
// every deployment.
//
// The empty string in g is the default graph, following the convention the
// other goRDFlib backends use: both a nil context and a blank node context mean
// the default graph, because a blank node cannot name a graph.
//
// The indexes cover the eight triple pattern shapes: (s,p,g) unique — it
// identifies a document and makes Add an upsert — plus the multikey (p,o,g) and
// (o,g) over the object array, and (g) for the graph-led scans.
//
// The cost of the layout is MongoDB's 16 MiB document limit: it caps how many
// objects a single subject and predicate may have in one graph. Reaching that
// takes on the order of a hundred thousand objects on one (s, p) pair, and it
// surfaces as a write error through the handler rather than as silent loss.
//
// Namespace bindings live in a second collection keyed by prefix.
//
// # Why MongoDB
//
// The reason to reach for this backend rather than a generic document store is
// $graphLookup. It implements store.ReachabilityStore directly, so SPARQL
// property paths of the form <p>*, <p>+, ^<p>+, (<p1>|<p2>)* and !(<p1>|<p2>)+
// are answered by one aggregation instead of a traversal that ships every edge
// of the predicate to the client. See reachability.go, which also documents the
// limits that make the backend decline and fall back.
//
// # Thread safety
//
// MongoStore is safe for concurrent use. It holds no mutable state of its own;
// the driver's Client is itself safe for concurrent use and pools connections.
//
// # Error handling
//
// store.Store has no way to report a write error, so write failures are passed
// to the handler installed with WithErrorHandler, which defaults to logging
// through the standard log package. Set it to something that fails loudly in
// tests, or to a no-op to silence it.
package mongostore
