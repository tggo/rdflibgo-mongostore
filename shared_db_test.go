//go:build integration

package mongostore_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tggo/goRDFlib/graph"
	"github.com/tggo/goRDFlib/sparql"
	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"

	"github.com/tggo/rdflibgo-mongostore"
)

// Issue #1: two stores with different collection prefixes share a database
// without seeing each other's data, $graphLookup included.
func (s *MongoStoreSuite) TestCollectionPrefix() {
	t := s.T()
	ctx := context.Background()
	db := s.nextDB()
	a := s.newStoreInDB(t, db, mongostore.WithCollectionPrefix("a_"))
	b := s.newStoreInDB(t, db, mongostore.WithCollectionPrefix("b_"))

	s.Require().NoError(a.Add(ctx, triple(uri("x"), uri("p"), uri("y")), nil))
	s.Require().NoError(a.Add(ctx, triple(uri("y"), uri("p"), uri("z")), nil))
	s.Require().NoError(a.Bind(ctx, "ex", uri("")))

	n, err := b.Len(ctx, nil)
	s.Require().NoError(err)
	s.Equal(0, n, "store b sees store a's triples")
	_, ok, err := b.Namespace(ctx, "ex")
	s.Require().NoError(err)
	s.False(ok, "store b sees store a's namespace binding")

	s.Equal("a_triples", a.Collection().Name())
	s.Equal("a_namespaces", a.NamespaceCollection().Name())

	q := store.ReachabilityQuery{Start: uri("x"), Predicates: []term.URIRef{uri("p")}}
	got, err := a.Reachable(ctx, q)
	s.Require().NoError(err)
	s.ElementsMatch([]string{"<http://example.org/y>", "<http://example.org/z>"}, keys(got),
		"$graphLookup must read the prefixed collection")
	got, err = b.Reachable(ctx, q)
	s.Require().NoError(err)
	s.Empty(got)

	// DropCollections removes a's collections and leaves b's.
	s.Require().NoError(b.Add(ctx, triple(uri("k"), uri("p"), uri("v")), nil))
	s.Require().NoError(a.DropCollections(ctx))
	names, err := a.Collection().Database().ListCollectionNames(ctx, bson.D{})
	s.Require().NoError(err)
	s.ElementsMatch([]string{"b_triples", "b_namespaces"}, names)

	for _, bad := range []string{"x$", "system.", "a\x00"} {
		_, err := mongostore.New(mongostore.WithURI(s.uri), mongostore.WithDatabase(db), mongostore.WithCollectionPrefix(bad))
		s.Error(err, "prefix %q", bad)
	}
}

// Issue #2: the context of a SPARQL query reaches the MongoDB command in
// flight. Every find is made to take 3 s on the server; a 200 ms deadline must
// end the query at about 200 ms, not when the find returns.
func (s *MongoStoreSuite) TestQueryDeadlineCancelsServerCommand() {
	t := s.T()
	st := s.newStore(t)
	g := graph.NewGraph(graph.WithStore(st))
	s.Require().NoError(g.Add(context.Background(), uri("a"), uri("type"), uri("Tool")))

	admin := st.Collection().Database().Client().Database("admin")
	err := admin.RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: "alwaysOn"},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{"find", "aggregate"}},
			{Key: "blockConnection", Value: true},
			{Key: "blockTimeMS", Value: 3000},
		}},
	}).Err()
	if err != nil {
		t.Skipf("server has no failCommand fail point (start mongod with --setParameter enableTestCommands=1): %v", err)
	}
	defer admin.RunCommand(context.Background(), bson.D{
		{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = sparql.QueryContext(ctx, g, `SELECT ?s WHERE { ?s <http://example.org/type> <http://example.org/Tool> }`)
	elapsed := time.Since(start)
	s.Require().Error(err)
	s.True(errors.Is(err, context.DeadlineExceeded), "err = %v", err)
	s.Less(elapsed, 2*time.Second, "the query waited for the server command instead of cancelling it")
}

// Issue #3: rdf:type/rdfs:subClassOf* over 340 items in 20 classes sends one
// $graphLookup per class, not per item.
func (s *MongoStoreSuite) TestPropertyPathAggregatesPerDistinctStart() {
	t := s.T()
	var aggregates atomic.Int64
	mon := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "aggregate" {
			aggregates.Add(1)
		}
	}}
	client, err := mongo.Connect(options.Client().ApplyURI(s.uri).SetMonitor(mon))
	s.Require().NoError(err)
	t.Cleanup(func() { client.Disconnect(context.Background()) })
	st := s.newStore(t, mongostore.WithClient(client))

	ctx := context.Background()
	g := graph.NewGraph(graph.WithStore(st))
	sub := uri("sub")
	for i := 1; i < 20; i++ {
		s.Require().NoError(g.Add(ctx, uri(fmt.Sprintf("C%d", i)), sub, uri(fmt.Sprintf("C%d", i-1))))
	}
	for i := range 340 {
		s.Require().NoError(g.Add(ctx, uri(fmt.Sprintf("item%d", i)), uri("type"), uri(fmt.Sprintf("C%d", i%20))))
	}

	aggregates.Store(0)
	res, err := sparql.QueryContext(ctx, g, `PREFIX ex: <http://example.org/>
SELECT ?item ?class WHERE { ?item ex:type/ex:sub* ?class }`)
	s.Require().NoError(err)
	s.Equal(3570, len(res.Bindings))
	s.LessOrEqual(aggregates.Load(), int64(20), "one $graphLookup per distinct class at most")
}
