package mongostore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"
)

// Collection names. They are fixed rather than configurable: two stores sharing
// a database would otherwise silently disagree about where the data is.
const (
	triplesCollection    = "triples"
	namespacesCollection = "namespaces"
)

// defaultGraphKey is what the default graph is called on disk. Both a nil
// context and a blank node context map to it.
const defaultGraphKey = ""

// MongoStore implements store.Store on top of MongoDB.
// Safe for concurrent use.
type MongoStore struct {
	client *mongo.Client
	db     *mongo.Database
	tripl  *mongo.Collection
	ns     *mongo.Collection

	ctx     context.Context
	timeout time.Duration
	onError func(op string, err error)

	// ownsClient records whether New dialed the client, and so whether Close
	// should disconnect it. A client passed in with WithClient belongs to the
	// caller.
	ownsClient bool

	// transactional is probed once at construction. A standalone mongod cannot
	// run transactions, and claiming otherwise would make Set's atomicity a lie.
	transactional bool
}

// Option configures a MongoStore.
type Option func(*config)

type config struct {
	uri      string
	database string
	client   *mongo.Client
	ctx      context.Context
	timeout  time.Duration
	onError  func(op string, err error)
}

// WithURI sets the MongoDB connection string. Required unless WithClient is
// used.
func WithURI(uri string) Option {
	return func(c *config) { c.uri = uri }
}

// WithDatabase sets the database name. Defaults to "rdf".
func WithDatabase(name string) Option {
	return func(c *config) { c.database = name }
}

// WithClient uses an already-connected driver client instead of dialing one.
// The client is not closed by Close; it stays the caller's to manage.
func WithClient(client *mongo.Client) Option {
	return func(c *config) { c.client = client }
}

// WithContext sets the base context for every operation.
//
// store.Store predates context and none of its methods take one, so the context
// has to be supplied here. Cancelling it makes subsequent operations fail:
// reads return nothing and writes report through the error handler.
func WithContext(ctx context.Context) Option {
	return func(c *config) { c.ctx = ctx }
}

// WithTimeout bounds each individual operation. Defaults to 30s. A
// non-positive value disables the per-operation deadline and leaves only the
// base context.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// WithErrorHandler installs a callback for errors that store.Store cannot
// return — every write, and any read that fails mid-iteration. op names the
// operation ("Add", "AddN", "Triples", …).
//
// The default logs through the standard log package. A test suite will usually
// want to fail on any call; a production caller will usually want to count
// them.
func WithErrorHandler(h func(op string, err error)) Option {
	return func(c *config) { c.onError = h }
}

// New connects to MongoDB and ensures the collections and indexes exist.
//
// It is safe to call against a database that already holds data: index creation
// is idempotent, and an existing index with the same specification is left
// alone.
func New(opts ...Option) (*MongoStore, error) {
	cfg := config{
		database: "rdf",
		ctx:      context.Background(),
		timeout:  30 * time.Second,
		onError: func(op string, err error) {
			log.Printf("mongostore: %s: %v", op, err)
		},
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.ctx == nil {
		cfg.ctx = context.Background()
	}
	if cfg.database == "" {
		return nil, errors.New("mongostore: database name is empty")
	}

	s := &MongoStore{
		ctx:     cfg.ctx,
		timeout: cfg.timeout,
		onError: cfg.onError,
	}

	switch {
	case cfg.client != nil:
		s.client = cfg.client
	case cfg.uri != "":
		client, err := mongo.Connect(options.Client().ApplyURI(cfg.uri))
		if err != nil {
			return nil, fmt.Errorf("mongostore: connect: %w", err)
		}
		s.client = client
		s.ownsClient = true
	default:
		return nil, errors.New("mongostore: neither WithURI nor WithClient was provided")
	}

	s.db = s.client.Database(cfg.database)
	s.tripl = s.db.Collection(triplesCollection)
	s.ns = s.db.Collection(namespacesCollection)

	ctx, cancel := s.opCtx()
	defer cancel()

	// Dialing is lazy, so nothing has touched the server yet. Ping here rather
	// than letting the first silent write be where an unreachable server shows
	// up.
	if err := s.client.Ping(ctx, nil); err != nil {
		s.closeClient()
		return nil, fmt.Errorf("mongostore: ping: %w", err)
	}
	if err := s.ensureIndexes(ctx); err != nil {
		s.closeClient()
		return nil, err
	}
	s.transactional = probeTransactions(ctx, s.client)

	return s, nil
}

// ensureIndexes creates the three compound indexes that cover the eight triple
// pattern shapes, plus the namespace index.
//
// The (s,p,o,g) index is unique, which is what makes Add idempotent without a
// read: an upsert on the full key either inserts or does nothing.
func (s *MongoStore) ensureIndexes(ctx context.Context) error {
	_, err := s.tripl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "s", Value: 1}, {Key: "p", Value: 1}, {Key: "o", Value: 1}, {Key: "g", Value: 1}},
			Options: options.Index().SetName("spog").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "p", Value: 1}, {Key: "o", Value: 1}, {Key: "s", Value: 1}, {Key: "g", Value: 1}},
			Options: options.Index().SetName("posg"),
		},
		{
			Keys:    bson.D{{Key: "o", Value: 1}, {Key: "s", Value: 1}, {Key: "p", Value: 1}, {Key: "g", Value: 1}},
			Options: options.Index().SetName("ospg"),
		},
		{
			// Contexts and every graph-scoped scan lead with g.
			Keys:    bson.D{{Key: "g", Value: 1}},
			Options: options.Index().SetName("g"),
		},
	})
	if err != nil {
		return fmt.Errorf("mongostore: create triple indexes: %w", err)
	}

	// Not unique: MemoryStore lets two prefixes point at the same namespace,
	// with the last binding winning the reverse lookup.
	_, err = s.ns.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "ns", Value: 1}},
		Options: options.Index().SetName("ns"),
	})
	if err != nil {
		return fmt.Errorf("mongostore: create namespace index: %w", err)
	}
	return nil
}

// probeTransactions asks the server what it is. Transactions need a replica set
// or a sharded cluster; on a standalone mongod they fail at run time, so it is
// better to know here than to promise something Set cannot deliver.
func probeTransactions(ctx context.Context, client *mongo.Client) bool {
	var hello struct {
		SetName string `bson:"setName"`
		Msg     string `bson:"msg"`
	}
	err := client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).
		Decode(&hello)
	if err != nil {
		return false
	}
	// setName is present on a replica set member; msg is "isdbgrid" on mongos.
	return hello.SetName != "" || hello.Msg == "isdbgrid"
}

// Close disconnects the driver client, unless the client was supplied by the
// caller through WithClient.
func (s *MongoStore) Close() error {
	return s.closeClient()
}

func (s *MongoStore) closeClient() error {
	if !s.ownsClient || s.client == nil {
		return nil
	}
	return s.client.Disconnect(context.Background())
}

// opCtx derives the context for one operation. The returned cancel func must
// always be called, even when no timeout is configured.
func (s *MongoStore) opCtx() (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return context.WithCancel(s.ctx)
	}
	return context.WithTimeout(s.ctx, s.timeout)
}

func (s *MongoStore) fail(op string, err error) {
	if err != nil && s.onError != nil {
		s.onError(op, err)
	}
}

// ContextAware reports true: named graphs are stored in the g field.
func (s *MongoStore) ContextAware() bool { return true }

// TransactionAware reports whether the server this store is connected to can
// run transactions — that is, whether it is a replica set or a sharded cluster.
// A standalone mongod cannot, and reports false.
func (s *MongoStore) TransactionAware() bool { return s.transactional }

// graphKey maps a context term to the stored graph key.
//
// nil is the default graph, and so is a blank node: blank nodes cannot name a
// graph, and Graph passes its own identifier — a BNode when the graph is
// unnamed — straight through as the context.
func graphKey(ctx term.Term) string {
	if ctx == nil {
		return defaultGraphKey
	}
	if _, isBNode := ctx.(term.BNode); isBNode {
		return defaultGraphKey
	}
	return term.TermKey(ctx)
}

// --- writes ----------------------------------------------------------------

// Add inserts a triple. Adding the same triple twice is a no-op.
func (s *MongoStore) Add(t term.Triple, ctx term.Term) {
	c, cancel := s.opCtx()
	defer cancel()

	filter := tripleDoc(t, graphKey(ctx))
	_, err := s.tripl.UpdateOne(c, filter,
		bson.D{{Key: "$setOnInsert", Value: filter}},
		options.UpdateOne().SetUpsert(true))
	s.fail("Add", err)
}

// AddN batch-inserts quads. Duplicates, within the batch or against what is
// already stored, are ignored.
func (s *MongoStore) AddN(quads []term.Quad) {
	if len(quads) == 0 {
		return
	}
	c, cancel := s.opCtx()
	defer cancel()

	// Chunked so that one enormous AddN cannot exceed the server's write
	// command limits. The unique index makes each chunk idempotent, so a retry
	// or an overlapping chunk changes nothing.
	const chunk = 1000
	models := make([]mongo.WriteModel, 0, min(chunk, len(quads)))
	flush := func() {
		if len(models) == 0 {
			return
		}
		_, err := s.tripl.BulkWrite(c, models, options.BulkWrite().SetOrdered(false))
		s.fail("AddN", err)
		models = models[:0]
	}

	for _, q := range quads {
		doc := tripleDoc(q.Triple, graphKey(q.Graph))
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(doc).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: doc}}).
			SetUpsert(true))
		if len(models) == chunk {
			flush()
		}
	}
	flush()
}

// Remove deletes every triple matching the pattern from the given context.
func (s *MongoStore) Remove(pattern term.TriplePattern, ctx term.Term) {
	c, cancel := s.opCtx()
	defer cancel()

	_, err := s.tripl.DeleteMany(c, patternFilter(pattern, ctx))
	s.fail("Remove", err)
}

// Set replaces every triple matching (s, p, *) in the context with the given
// triple.
//
// On a replica set or a sharded cluster the two operations run in one
// transaction, so a concurrent reader never sees the interval where the old
// value is gone and the new one is not yet there. On a standalone mongod —
// where TransactionAware reports false — that window exists, because the server
// offers no way to close it.
func (s *MongoStore) Set(t term.Triple, ctx term.Term) {
	c, cancel := s.opCtx()
	defer cancel()

	gk := graphKey(ctx)
	del := bson.D{
		{Key: "s", Value: term.TermKey(t.Subject)},
		{Key: "p", Value: term.TermKey(t.Predicate)},
		{Key: "g", Value: gk},
	}
	doc := tripleDoc(t, gk)

	apply := func(c context.Context) error {
		if _, err := s.tripl.DeleteMany(c, del); err != nil {
			return err
		}
		_, err := s.tripl.UpdateOne(c, doc,
			bson.D{{Key: "$setOnInsert", Value: doc}},
			options.UpdateOne().SetUpsert(true))
		return err
	}

	if !s.transactional {
		s.fail("Set", apply(c))
		return
	}

	sess, err := s.client.StartSession()
	if err != nil {
		s.fail("Set", err)
		return
	}
	defer sess.EndSession(c)

	_, err = sess.WithTransaction(c, func(sc context.Context) (any, error) {
		return nil, apply(sc)
	})
	s.fail("Set", err)
}

// tripleDoc builds the stored document for a triple, which doubles as the
// filter that identifies it uniquely.
func tripleDoc(t term.Triple, gk string) bson.D {
	return bson.D{
		{Key: "s", Value: term.TermKey(t.Subject)},
		{Key: "p", Value: term.TermKey(t.Predicate)},
		{Key: "o", Value: term.TermKey(t.Object)},
		{Key: "g", Value: gk},
	}
}

// --- namespaces ------------------------------------------------------------

// Bind associates a prefix with a namespace, replacing any previous binding for
// that prefix.
func (s *MongoStore) Bind(prefix string, namespace term.URIRef) {
	c, cancel := s.opCtx()
	defer cancel()

	_, err := s.ns.ReplaceOne(c,
		bson.D{{Key: "_id", Value: prefix}},
		bson.D{{Key: "_id", Value: prefix}, {Key: "ns", Value: namespace.Value()}},
		options.Replace().SetUpsert(true))
	s.fail("Bind", err)
}

// Namespace returns the namespace bound to a prefix.
func (s *MongoStore) Namespace(prefix string) (term.URIRef, bool) {
	c, cancel := s.opCtx()
	defer cancel()

	var doc struct {
		NS string `bson:"ns"`
	}
	err := s.ns.FindOne(c, bson.D{{Key: "_id", Value: prefix}}).Decode(&doc)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			s.fail("Namespace", err)
		}
		return term.URIRef{}, false
	}
	return term.NewURIRefUnsafe(doc.NS), true
}

// Prefix returns a prefix bound to the namespace.
func (s *MongoStore) Prefix(namespace term.URIRef) (string, bool) {
	c, cancel := s.opCtx()
	defer cancel()

	var doc struct {
		ID string `bson:"_id"`
	}
	err := s.ns.FindOne(c, bson.D{{Key: "ns", Value: namespace.Value()}}).Decode(&doc)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			s.fail("Prefix", err)
		}
		return "", false
	}
	return doc.ID, true
}

// Namespaces iterates over every binding.
func (s *MongoStore) Namespaces() store.NamespaceIterator {
	return func(yield func(string, term.URIRef) bool) {
		c, cancel := s.opCtx()
		defer cancel()

		cur, err := s.ns.Find(c, bson.D{})
		if err != nil {
			s.fail("Namespaces", err)
			return
		}
		defer cur.Close(c)

		for cur.Next(c) {
			var doc struct {
				ID string `bson:"_id"`
				NS string `bson:"ns"`
			}
			if err := cur.Decode(&doc); err != nil {
				s.fail("Namespaces", err)
				continue
			}
			if !yield(doc.ID, term.NewURIRefUnsafe(doc.NS)) {
				return
			}
		}
		s.fail("Namespaces", cur.Err())
	}
}

// Compile-time checks. Every interface this backend claims is asserted here so
// that a change to any of them is a build failure rather than a surprise at the
// first type assertion.
var (
	_ store.Store             = (*MongoStore)(nil)
	_ store.QueryableStore    = (*MongoStore)(nil)
	_ store.ReachabilityStore = (*MongoStore)(nil)
)
