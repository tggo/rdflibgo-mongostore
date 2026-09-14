//go:build integration

package mongostore_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tggo/goRDFlib/graph"
	"github.com/tggo/goRDFlib/plugin"
	"github.com/tggo/goRDFlib/sparql"
	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"

	"github.com/tggo/rdflibgo-mongostore"
)

// --- construction ----------------------------------------------------------

func (s *MongoStoreSuite) TestNew() {
	s.Run("rejects a store with no way to connect", func() {
		_, err := mongostore.New(mongostore.WithDatabase("x"))
		s.Error(err, "New must refuse without WithURI or WithClient")
	})

	s.Run("rejects an empty database name", func() {
		_, err := mongostore.New(mongostore.WithURI(s.uri), mongostore.WithDatabase(""))
		s.Error(err, "New must refuse an empty database name")
	})

	s.Run("reports an unreachable server instead of failing later", func() {
		_, err := mongostore.New(
			mongostore.WithURI("mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=500"),
			mongostore.WithDatabase("nope"),
		)
		s.Error(err, "New must ping, not wait for the first silent write to fail")
	})

	s.Run("creates the index shapes the pattern lookups need", func() {
		st := s.newStore(s.T())
		st.Add(triple(uri("a"), uri("p"), uri("b")), nil)

		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()

		got := map[string]bool{}
		cur, err := st.Collection().Indexes().List(ctx)
		s.Require().NoError(err)
		defer cur.Close(ctx)
		for cur.Next(ctx) {
			var idx struct {
				Name   string `bson:"name"`
				Unique bool   `bson:"unique"`
			}
			s.Require().NoError(cur.Decode(&idx))
			got[idx.Name] = idx.Unique
		}

		for _, name := range []string{"spg", "pog", "og", "g"} {
			s.Contains(got, name, "index %s is missing", name)
		}
		s.True(got["spg"],
			"spg must be unique — it is what identifies one document, and what makes "+
				"Add an upsert and Set a single-document update")
	})

	s.Run("is idempotent over an existing database", func() {
		db := s.nextDB()
		first := s.newStoreInDB(s.T(), db)
		first.Add(triple(uri("a"), uri("p"), uri("b")), nil)

		second := s.newStoreInDB(s.T(), db)
		s.Equal(1, second.Len(nil), "a second New over the same database must see the data")
	})

	s.Run("reports a standalone server as non-transactional", func() {
		st := s.newStore(s.T())
		s.False(st.TransactionAware(),
			"the suite runs a standalone mongod; claiming transactions would be a lie")
		s.True(st.ContextAware())
	})
}

// --- error reporting -------------------------------------------------------

func (s *MongoStoreSuite) TestErrorHandler() {
	s.Run("a write failure reaches the handler", func() {
		// store.Store cannot return a write error, so a backend that does not
		// report it loses data in silence. Cancelling the base context is the
		// cheapest way to make every operation fail.
		ctx, cancel := context.WithCancel(context.Background())

		var got []string
		st, err := mongostore.New(
			mongostore.WithURI(s.uri),
			mongostore.WithDatabase(s.nextDB()),
			mongostore.WithContext(ctx),
			mongostore.WithErrorHandler(func(op string, err error) {
				got = append(got, op)
			}),
		)
		s.Require().NoError(err)
		defer st.Close()

		cancel()
		st.Add(triple(uri("a"), uri("p"), uri("b")), nil)
		st.AddN([]term.Quad{{Triple: triple(uri("a"), uri("p"), uri("c"))}})
		st.Remove(term.TriplePattern{}, nil)
		st.Bind("ex", uri("ns"))

		s.Subset(got, []string{"Add", "AddN", "Remove", "Bind"},
			"every write that failed must be reported; got %v", got)
	})

	s.Run("a missing namespace is not an error", func() {
		st := s.newStore(s.T()) // its handler fails the test on any report
		_, ok := st.Namespace("absent")
		s.False(ok)
		_, ok = st.Prefix(uri("absent"))
		s.False(ok)
	})
}

// --- $graphLookup ----------------------------------------------------------

// reachGraph builds the graph the reachability tests run on:
//
//	a --p--> b --p--> c --p--> a   (cycle)
//	b --p--> d
//	d --q--> e
//	c --p--> "leaf"
func (s *MongoStoreSuite) reachGraph(st *mongostore.MongoStore, ctx term.Subject) {
	st.AddN([]term.Quad{
		{Triple: triple(uri("a"), uri("p"), uri("b")), Graph: ctx},
		{Triple: triple(uri("b"), uri("p"), uri("c")), Graph: ctx},
		{Triple: triple(uri("c"), uri("p"), uri("a")), Graph: ctx},
		{Triple: triple(uri("b"), uri("p"), uri("d")), Graph: ctx},
		{Triple: triple(uri("d"), uri("q"), uri("e")), Graph: ctx},
		{Triple: triple(uri("c"), uri("p"), term.NewLiteral("leaf")), Graph: ctx},
	})
}

func (s *MongoStoreSuite) TestReachable() {
	st := s.newStore(s.T())
	s.reachGraph(st, nil)

	reach := func(q store.ReachabilityQuery) []string {
		got, err := st.Reachable(q)
		s.Require().NoError(err)
		out := keys(got)
		sort.Strings(out)
		return out
	}
	sorted := func(vals ...string) []string {
		sort.Strings(vals)
		return vals
	}

	p, q := uri("p"), uri("q")

	s.Run("forward closure includes the start when a cycle returns to it", func() {
		// c --p--> "leaf" is reached too: a literal terminates the walk but is
		// still a node the path reaches.
		s.Equal(
			sorted(uri("a").N3(), uri("b").N3(), uri("c").N3(), uri("d").N3(), term.NewLiteral("leaf").N3()),
			reach(store.ReachabilityQuery{Start: uri("a"), Predicates: []term.URIRef{p}}),
			"$graphLookup must terminate on the a->b->c->a cycle and report a as reachable")
	})

	s.Run("a leaf reaches nothing", func() {
		s.Empty(reach(store.ReachabilityQuery{Start: uri("d"), Predicates: []term.URIRef{p}}))
	})

	s.Run("a node outside the graph reaches nothing", func() {
		s.Empty(reach(store.ReachabilityQuery{Start: uri("zzz"), Predicates: []term.URIRef{p}}))
	})

	s.Run("depth 1 is a single hop", func() {
		s.Equal([]string{uri("b").N3()},
			reach(store.ReachabilityQuery{Start: uri("a"), Predicates: []term.URIRef{p}, MaxDepth: 1}),
			"maxDepth arithmetic is off by one in both directions if this fails")
	})

	s.Run("depth 2 is two hops", func() {
		s.Equal(sorted(uri("b").N3(), uri("c").N3(), uri("d").N3()),
			reach(store.ReachabilityQuery{Start: uri("a"), Predicates: []term.URIRef{p}, MaxDepth: 2}))
	})

	s.Run("inverse traversal walks object to subject", func() {
		s.Equal(sorted(uri("a").N3(), uri("b").N3(), uri("c").N3()),
			reach(store.ReachabilityQuery{Start: uri("c"), Predicates: []term.URIRef{p}, Inverse: true}))
	})

	s.Run("inverse traversal may start at a literal", func() {
		// Nothing leads out of a literal, but plenty leads in: "leaf" is
		// reached from c, c from b, b from a.
		s.Equal(sorted(uri("a").N3(), uri("b").N3(), uri("c").N3()),
			reach(store.ReachabilityQuery{Start: term.NewLiteral("leaf"), Predicates: []term.URIRef{p}, Inverse: true}),
			"a literal is a legitimate path endpoint")
	})

	s.Run("a literal is reported as a reachable node", func() {
		s.Equal(
			sorted(uri("a").N3(), uri("b").N3(), uri("c").N3(), uri("d").N3(), uri("e").N3(), term.NewLiteral("leaf").N3()),
			reach(store.ReachabilityQuery{Start: uri("b"), Predicates: []term.URIRef{p, q}}))
	})

	s.Run("no predicates means any predicate", func() {
		s.Equal(
			sorted(uri("a").N3(), uri("b").N3(), uri("c").N3(), uri("d").N3(), uri("e").N3(), term.NewLiteral("leaf").N3()),
			reach(store.ReachabilityQuery{Start: uri("a")}))
	})

	s.Run("a negated set excludes its predicates", func() {
		s.Equal(sorted(uri("a").N3(), uri("b").N3(), uri("c").N3(), uri("d").N3(), term.NewLiteral("leaf").N3()),
			reach(store.ReachabilityQuery{Start: uri("a"), Predicates: []term.URIRef{q}, Negated: true}),
			"excluding q must drop e but keep everything reachable through p")
	})

	s.Run("an empty negated set excludes nothing", func() {
		s.Equal(reach(store.ReachabilityQuery{Start: uri("a")}),
			reach(store.ReachabilityQuery{Start: uri("a"), Negated: true}))
	})

	s.Run("a start with no bound node is refused", func() {
		_, err := st.Reachable(store.ReachabilityQuery{Predicates: []term.URIRef{p}})
		s.ErrorIs(err, store.ErrReachabilityUnsupported,
			"declining must be reported as ErrReachabilityUnsupported so the caller falls back")
	})
}

func (s *MongoStoreSuite) TestReachableIsGraphScoped() {
	st := s.newStore(s.T())
	g1 := uri("graph1")

	st.Add(triple(uri("a"), uri("p"), uri("b")), g1)
	st.Add(triple(uri("b"), uri("p"), uri("c")), nil) // default graph

	s.Run("a named graph sees only its own edges", func() {
		got, err := st.Reachable(store.ReachabilityQuery{
			Start: uri("a"), Predicates: []term.URIRef{uri("p")}, Context: g1,
		})
		s.Require().NoError(err)
		s.Equal([]string{uri("b").N3()}, keys(got),
			"the b->c edge lives in the default graph and must not be traversed")
	})

	s.Run("the default graph sees only its own edges", func() {
		got, err := st.Reachable(store.ReachabilityQuery{
			Start: uri("a"), Predicates: []term.URIRef{uri("p")},
		})
		s.Require().NoError(err)
		s.Empty(got, "a has no outgoing edge in the default graph")
	})
}

// TestReachableDeepChain runs the closure over a chain long enough that a
// per-edge $graphLookup, or a recursion that revisited nodes, would blow up
// rather than merely be slow.
func (s *MongoStoreSuite) TestReachableDeepChain() {
	st := s.newStore(s.T())

	const n = 2000
	quads := make([]term.Quad, 0, n)
	for i := 0; i < n; i++ {
		quads = append(quads, term.Quad{Triple: triple(
			uri(fmt.Sprintf("n%d", i)), uri("p"), uri(fmt.Sprintf("n%d", i+1)))})
	}
	st.AddN(quads)

	start := time.Now()
	got, err := st.Reachable(store.ReachabilityQuery{
		Start: uri("n0"), Predicates: []term.URIRef{uri("p")},
	})
	s.Require().NoError(err)
	s.Len(got, n, "every node after the first must be reachable")
	s.T().Logf("closure over a %d-edge chain took %s", n, time.Since(start))
}

// --- integration through Graph and SPARQL ----------------------------------

// TestPropertyPathThroughSPARQL is the end-to-end check that the pushdown is
// wired up: goRDFlib's paths.MulPath asks the store for reachability, and the
// answer has to be the one SPARQL expects.
func (s *MongoStoreSuite) TestPropertyPathThroughSPARQL() {
	st := s.newStore(s.T())
	g := graph.NewGraph(graph.WithStore(st))

	g.Add(uri("a"), uri("knows"), uri("b"))
	g.Add(uri("b"), uri("knows"), uri("c"))
	g.Add(uri("c"), uri("knows"), uri("d"))
	g.Add(uri("d"), uri("likes"), uri("e"))

	ask := func(query string) []string {
		res, err := sparql.Query(g, query)
		s.Require().NoError(err)
		out := make([]string, 0, len(res.Bindings))
		for _, b := range res.Bindings {
			out = append(out, b["x"].N3())
		}
		sort.Strings(out)
		return out
	}

	const prefix = `PREFIX ex: <http://example.org/> `

	s.Run("one or more", func() {
		s.Equal([]string{uri("b").N3(), uri("c").N3(), uri("d").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:a ex:knows+ ?x }`))
	})

	s.Run("zero or more includes the start", func() {
		s.Equal([]string{uri("a").N3(), uri("b").N3(), uri("c").N3(), uri("d").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:a ex:knows* ?x }`))
	})

	s.Run("inverse", func() {
		s.Equal([]string{uri("a").N3(), uri("b").N3(), uri("c").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:d ^ex:knows+ ?x }`))
	})

	s.Run("alternation", func() {
		s.Equal([]string{uri("b").N3(), uri("c").N3(), uri("d").N3(), uri("e").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:a (ex:knows|ex:likes)+ ?x }`))
	})

	s.Run("negated property set", func() {
		s.Equal([]string{uri("b").N3(), uri("c").N3(), uri("d").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:a !(ex:likes)+ ?x }`))
	})

	s.Run("bound object", func() {
		s.Equal([]string{uri("d").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:a ex:knows+ ?x FILTER(?x = ex:d) }`))
	})

	s.Run("a sequence still evaluates correctly on the fallback path", func() {
		s.Equal([]string{uri("e").N3()},
			ask(prefix+`SELECT ?x WHERE { ex:a ex:knows+/ex:likes ?x }`),
			"a sequence cannot be pushed down; the local traversal must still be right")
	})
}

// TestGraphAndSPARQLOverMongo exercises the ordinary read/write surface through
// Graph and a SPARQL query, which is how a caller actually uses this backend.
func (s *MongoStoreSuite) TestGraphAndSPARQLOverMongo() {
	st := s.newStore(s.T())
	g := graph.NewGraph(graph.WithStore(st))

	g.Add(uri("Alice"), uri("name"), term.NewLiteral("Alice"))
	g.Add(uri("Alice"), uri("age"), term.NewLiteral(30))
	g.Add(uri("Bob"), uri("name"), term.NewLiteral("Bob"))
	g.Add(uri("Bob"), uri("knows"), uri("Alice"))

	s.Run("Graph reads what it wrote", func() {
		s.Equal(4, g.Len())
		s.True(g.Contains(uri("Bob"), uri("knows"), uri("Alice")))
		s.Equal(2, countTriples(g.Triples(uri("Alice"), nil, nil)))
	})

	s.Run("SELECT over the store", func() {
		res, err := sparql.Query(g, `PREFIX ex: <http://example.org/>
			SELECT ?name WHERE { ?p ex:knows ex:Alice . ?p ex:name ?name }`)
		s.Require().NoError(err)
		s.Require().Len(res.Bindings, 1)
		s.Equal(`"Bob"`, res.Bindings[0]["name"].N3())
	})

	s.Run("a literal with quotes survives the round trip", func() {
		lit := term.NewLiteral(`he said "hi"`)
		g.Add(uri("Alice"), uri("quote"), lit)
		got, ok := g.Value(uri("Alice"), predPtr(uri("quote")), nil)
		s.Require().True(ok)
		s.Equal(lit.N3(), got.N3())
	})

	s.Run("Set replaces a single value", func() {
		g.Set(uri("Alice"), uri("age"), term.NewLiteral(31))
		s.Equal(1, countTriples(g.Triples(uri("Alice"), predPtr(uri("age")), nil)))
	})
}

// --- storage layout --------------------------------------------------------

// TestDocumentLayout pins the on-disk shape. It is a public contract: the
// README documents these field names, and anyone querying the collection from
// mongosh or an aggregation depends on them.
//
// The layout is not one document per triple. Every object of a given subject
// and predicate in a given graph lives in one document, because that is what
// makes Set atomic without a transaction — see TestSetIsASingleDocumentUpdate.
func (s *MongoStoreSuite) TestDocumentLayout() {
	st := s.newStore(s.T())
	st.Add(triple(uri("Alice"), uri("knows"), uri("Bob")), nil)
	st.Add(triple(uri("Alice"), uri("knows"), uri("Carol")), nil)
	st.Add(triple(uri("Alice"), uri("name"), term.NewLiteral("Alice")), nil)
	st.Add(triple(uri("Alice"), uri("knows"), uri("Dave")), uri("graph1"))

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	type doc struct {
		S string   `bson:"s"`
		P string   `bson:"p"`
		O []string `bson:"o"`
		G string   `bson:"g"`
	}

	s.Run("terms are stored as TermKey strings", func() {
		var d doc
		err := st.Collection().FindOne(ctx, bson.D{
			{Key: "s", Value: term.TermKey(uri("Alice"))},
			{Key: "p", Value: term.TermKey(uri("knows"))},
			{Key: "g", Value: ""},
		}).Decode(&d)
		s.Require().NoError(err)

		s.Equal("U:http://example.org/Alice", d.S)
		s.Equal("U:http://example.org/knows", d.P)
		s.ElementsMatch([]string{"U:http://example.org/Bob", "U:http://example.org/Carol"}, d.O)
		s.Equal("", d.G, "the default graph is the empty string")
	})

	s.Run("one document per subject, predicate and graph", func() {
		n, err := st.Collection().CountDocuments(ctx, bson.D{})
		s.Require().NoError(err)
		s.Equal(int64(3), n,
			"four triples over three (s,p,g) combinations must be three documents")
		s.Equal(3, st.Len(nil), "three triples in the default graph")
		s.Equal(1, st.Len(uri("graph1")))
	})

	s.Run("a named graph is stored under its own key", func() {
		n, err := st.Collection().CountDocuments(ctx,
			bson.D{{Key: "g", Value: term.TermKey(uri("graph1"))}})
		s.Require().NoError(err)
		s.Equal(int64(1), n)
	})

	bn := term.NewBNode()
	s.Run("a blank node context is a named graph", func() {
		st.Add(triple(uri("Erin"), uri("knows"), uri("Frank")), bn)
		s.Equal(3, st.Len(nil), "a blank node names a graph")
		s.Equal(1, st.Len(bn))
	})

	s.Run("removing the last object removes the document", func() {
		st.Remove(term.TriplePattern{
			Subject: uri("Erin"), Predicate: predPtr(uri("knows")), Object: uri("Frank"),
		}, bn)

		n, err := st.Collection().CountDocuments(ctx,
			bson.D{{Key: "s", Value: term.TermKey(uri("Erin"))}})
		s.Require().NoError(err)
		s.Zero(n, "an empty document would keep its graph alive in Contexts")
	})
}

// TestSetIsASingleDocumentUpdate is the test the storage layout exists for.
//
// store.Store requires that Set replace (s, p, *) without a concurrent reader
// ever seeing the interval where the old value is gone and the new one is not
// yet there. On a standalone mongod there are no transactions, so the only
// atomicity available is per document — which is why every object of a subject
// and predicate lives in one document.
func (s *MongoStoreSuite) TestSetIsASingleDocumentUpdate() {
	st := s.newStore(s.T())

	s.Run("Set collapses many values into one", func() {
		st.Add(triple(uri("Alice"), uri("age"), term.NewLiteral(29)), nil)
		st.Add(triple(uri("Alice"), uri("age"), term.NewLiteral(30)), nil)
		st.Add(triple(uri("Alice"), uri("age"), term.NewLiteral(31)), nil)
		s.Equal(3, st.Len(nil))

		st.Set(triple(uri("Alice"), uri("age"), term.NewLiteral(32)), nil)
		s.Equal(1, st.Len(nil))
	})

	s.Run("a concurrent reader never sees zero values", func() {
		st.Set(triple(uri("Bob"), uri("age"), term.NewLiteral(1)), nil)

		pattern := term.TriplePattern{Subject: uri("Bob"), Predicate: predPtr(uri("age"))}
		done := make(chan struct{})
		var missing int

		go func() {
			defer close(done)
			for i := 2; i < 60; i++ {
				st.Set(triple(uri("Bob"), uri("age"), term.NewLiteral(i)), nil)
			}
		}()

		for {
			select {
			case <-done:
				s.Zero(missing,
					"a reader saw the value missing %d times; Set is not atomic", missing)
				return
			default:
				if countTriples(st.Triples(pattern, nil)) == 0 {
					missing++
				}
			}
		}
	})
}

func (s *MongoStoreSuite) TestAddNLargeBatch() {
	st := s.newStore(s.T())

	// Larger than the internal chunk size, so the chunk boundary is crossed.
	const n = 5000
	quads := make([]term.Quad, 0, n)
	for i := 0; i < n; i++ {
		quads = append(quads, term.Quad{
			Triple: triple(uri(fmt.Sprintf("s%d", i)), uri("p"), term.NewLiteral(i)),
		})
	}

	start := time.Now()
	st.AddN(quads)
	s.T().Logf("AddN of %d quads took %s", n, time.Since(start))

	s.Equal(n, st.Len(nil))

	s.Run("re-adding the same batch changes nothing", func() {
		st.AddN(quads)
		s.Equal(n, st.Len(nil), "the unique index makes AddN idempotent")
	})
}

// --- plugin ----------------------------------------------------------------

func (s *MongoStoreSuite) TestPluginRegistration() {
	s.Run("the factory builds a working store from the environment", func() {
		s.T().Setenv(mongostore.EnvURI, s.uri)
		s.T().Setenv(mongostore.EnvDatabase, s.nextDB())

		// plugin.GetStore builds the store, so registration and construction
		// are checked by the same call.
		st, ok := plugin.GetStore("mongo")
		s.Require().True(ok, "mongostore must register itself as \"mongo\"")
		s.Require().NotNil(st)
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
			defer cancel()
			_ = st.(*mongostore.MongoStore).DropDatabase(ctx)
			_ = st.(*mongostore.MongoStore).Close()
		}()

		st.Add(triple(uri("a"), uri("p"), uri("b")), nil)
		s.Equal(1, st.Len(nil))
	})

	s.Run("the factory refuses to build a store it cannot connect", func() {
		s.T().Setenv(mongostore.EnvURI, "")
		s.Panics(func() { plugin.GetStore("mongo") },
			"a store that silently swallows every write is worse than a panic")
	})
}
