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
// Triples live in one collection, one document per quad, with every term
// written through term.TermKey:
//
//	{ s: "U:http://example.org/Alice",
//	  p: "U:http://example.org/knows",
//	  o: "U:http://example.org/Bob",
//	  g: "" }
//
// The empty string in g is the default graph, following the convention the
// other goRDFlib backends use: both a nil context and a blank node context mean
// the default graph, because a blank node cannot name a graph.
//
// Three compound indexes cover the eight triple pattern shapes — (s,p,o,g)
// unique, (p,o,s,g), and (o,s,p,g) — which is the same SPO/POS/OSP design the
// in-memory, Badger and SQLite backends use.
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
