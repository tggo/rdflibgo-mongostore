//go:build integration

package mongostore_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"
	"github.com/tggo/rdflibgo-mongostore"
)

const (
	mongoImage    = "mongo"
	mongoTag      = "8"
	startTimeout  = 90 * time.Second
	containerTTL  = 600 // seconds; a safety net if the suite is killed
	opTimeout     = 20 * time.Second
	envExternal   = "MONGO_URI"
	databasePrefx = "rdflibgo_test_"
)

// MongoStoreSuite runs every test in this package against a real mongod.
//
// The server is a plain standalone, deliberately: that is what the deployments
// this backend targets actually run, and a standalone cannot execute
// transactions. Testing against a replica set would let a design that quietly
// depends on transactions pass here and lose data in production. Everything in
// this package is built to be atomic without them, and this is where that is
// checked.
//
// Set MONGO_URI to run against a server you already have; otherwise the suite
// starts a container and removes it afterwards.
type MongoStoreSuite struct {
	suite.Suite

	pool     *dockertest.Pool
	resource *dockertest.Resource
	uri      string
	dbSeq    atomic.Int64
}

func TestMongoStoreSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("integration suite needs a MongoDB server")
	}
	suite.Run(t, new(MongoStoreSuite))
}

func (s *MongoStoreSuite) SetupSuite() {
	if uri := os.Getenv(envExternal); uri != "" {
		s.T().Logf("using the server at %s from %s", uri, envExternal)
		s.uri = uri
		s.waitReady()
		return
	}

	pool, err := dockertest.NewPool("")
	s.Require().NoError(err, "create docker pool")
	s.Require().NoError(pool.Client.Ping(), "docker is not reachable")
	pool.MaxWait = startTimeout
	s.pool = pool

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: mongoImage,
		Tag:        mongoTag,
		Cmd:        []string{"--bind_ip_all"},
	}, func(cfg *dc.HostConfig) {
		cfg.AutoRemove = true
		cfg.RestartPolicy = dc.RestartPolicy{Name: "no"}
	})
	s.Require().NoError(err, "start mongo container")
	s.resource = resource
	s.Require().NoError(resource.Expire(containerTTL), "set container ttl")

	s.uri = fmt.Sprintf("mongodb://localhost:%s/", resource.GetPort("27017/tcp"))

	s.waitReady()
}

// waitReady polls until the server accepts a write, not merely a connection.
// A mongod answers pings before it is willing to write, so pinging alone hands
// the first test a server that rejects every insert.
func (s *MongoStoreSuite) waitReady() {
	s.T().Helper()

	retry := func(op func() error) error {
		if s.pool != nil {
			return s.pool.Retry(op)
		}
		deadline := time.Now().Add(startTimeout)
		var last error
		for time.Now().Before(deadline) {
			if last = op(); last == nil {
				return nil
			}
			time.Sleep(time.Second)
		}
		return last
	}

	err := retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()

		client, err := mongo.Connect(options.Client().ApplyURI(s.uri))
		if err != nil {
			return err
		}
		defer client.Disconnect(context.Background())

		coll := client.Database(databasePrefx + "ready").Collection("probe")
		if _, err := coll.InsertOne(ctx, bson.D{{Key: "ok", Value: 1}}); err != nil {
			return err
		}
		return client.Database(databasePrefx + "ready").Drop(ctx)
	})
	s.Require().NoError(err, "wait for a writable mongod")
}

func (s *MongoStoreSuite) TearDownSuite() {
	if s.resource == nil {
		return
	}
	s.NoError(s.pool.Purge(s.resource), "remove mongo container")
}

// --- helpers ---------------------------------------------------------------

// newStore returns a store over a database of its own, dropped when the test
// that asked for it finishes. Isolating by database rather than by cleaning
// collections means one test can never see another's leftovers, including its
// indexes.
func (s *MongoStoreSuite) newStore(t *testing.T, opts ...mongostore.Option) *mongostore.MongoStore {
	t.Helper()
	return s.newStoreInDB(t, s.nextDB(), opts...)
}

func (s *MongoStoreSuite) nextDB() string {
	return fmt.Sprintf("%s%d", databasePrefx, s.dbSeq.Add(1))
}

func (s *MongoStoreSuite) newStoreInDB(t *testing.T, db string, opts ...mongostore.Option) *mongostore.MongoStore {
	t.Helper()

	all := append([]mongostore.Option{
		mongostore.WithURI(s.uri),
		mongostore.WithDatabase(db),
		mongostore.WithTimeout(opTimeout),
		// Any error store.Store cannot return is a test failure. Left at the
		// default it would go to the log and the test would pass with a store
		// that quietly dropped every write.
		mongostore.WithErrorHandler(func(op string, err error) {
			t.Errorf("mongostore reported an error from %s: %v", op, err)
		}),
	}, opts...)

	st, err := mongostore.New(all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		defer cancel()
		if err := st.DropDatabase(ctx); err != nil {
			t.Logf("drop database %s: %v", db, err)
		}
		st.Close()
	})
	return st
}

func uri(local string) term.URIRef { return term.NewURIRefUnsafe("http://example.org/" + local) }

func predPtr(p term.URIRef) *term.URIRef { return &p }

func triple(subj term.Subject, pred term.URIRef, obj term.Term) term.Triple {
	return term.Triple{Subject: subj, Predicate: pred, Object: obj}
}

func keys(terms []term.Term) []string {
	out := make([]string, 0, len(terms))
	for _, t := range terms {
		out = append(out, t.N3())
	}
	return out
}

func countTriples(it store.TripleIterator) int {
	n := 0
	it(func(term.Triple) bool {
		n++
		return true
	})
	return n
}
