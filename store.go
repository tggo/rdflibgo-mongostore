package mongostore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"
)

// Collection names, before WithCollectionPrefix.
const (
	triplesCollection    = "triples"
	namespacesCollection = "namespaces"
)

// defaultGraphKey is what the default graph is called on disk. A nil context
// and store.DefaultGraph map to it.
const defaultGraphKey = ""

// MongoStore implements store.Store on top of MongoDB.
// Safe for concurrent use. Every method takes the caller's context, passes it
// to the driver, and returns driver failures wrapped; a done context is
// refused before anything is sent.
type MongoStore struct {
	client *mongo.Client
	db     *mongo.Database
	tripl  *mongo.Collection
	ns     *mongo.Collection

	timeout time.Duration

	// ownsClient records whether New dialed the client, and so whether Close
	// should disconnect it. A client passed in with WithClient belongs to the
	// caller.
	ownsClient bool

	// transactional is probed once at construction and only reported, never
	// relied on: every operation here is atomic without transactions.
	transactional bool
}

// Option configures a MongoStore.
type Option func(*config)

type config struct {
	uri      string
	database string
	client   *mongo.Client
	timeout  time.Duration
	prefix   string
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

// WithCollectionPrefix puts the store's collections at "<prefix>triples" and
// "<prefix>namespaces", so several stores can share one database, or one that
// already has collections of those names. Stores with different prefixes do
// not see each other's data; every store opening the same data must use the
// same prefix. Defaults to "" (collections "triples" and "namespaces").
//
// A prefix with '$' or NUL, or one starting with "system.", is refused by New:
// MongoDB does not allow such collection names.
func WithCollectionPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithTimeout bounds each individual operation, on top of whatever deadline the
// caller's context carries, and the connection check in New. Defaults to 30s. A
// non-positive value disables the per-operation deadline and leaves only the
// caller's context.
func WithTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

// New connects to MongoDB and ensures the collections and indexes exist.
//
// It is safe to call against a database that already holds data: index creation
// is idempotent, and an existing index with the same specification is left
// alone.
func New(opts ...Option) (*MongoStore, error) {
	cfg := config{
		database: "rdf",
		timeout:  30 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.database == "" {
		return nil, errors.New("mongostore: database name is empty")
	}
	if strings.ContainsAny(cfg.prefix, "$\x00") || strings.HasPrefix(cfg.prefix, "system.") {
		return nil, fmt.Errorf("mongostore: collection prefix %q is not usable in a MongoDB collection name (no '$', no NUL, not \"system.\")", cfg.prefix)
	}

	s := &MongoStore{timeout: cfg.timeout}

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
	s.tripl = s.db.Collection(cfg.prefix + triplesCollection)
	s.ns = s.db.Collection(cfg.prefix + namespacesCollection)

	ctx, cancel := s.opCtx(context.Background())
	defer cancel()

	// Dialing is lazy, so nothing has touched the server yet. Ping here rather
	// than letting the first write be where an unreachable server shows up.
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

// ensureIndexes creates the indexes that cover the eight triple pattern shapes.
//
// spg is unique and identifies a document, which is what makes Add an upsert
// and Set a single-document update. The other two are multikey over the object
// array: pog serves the predicate-led patterns and og the object-only one.
func (s *MongoStore) ensureIndexes(ctx context.Context) error {
	_, err := s.tripl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "s", Value: 1}, {Key: "p", Value: 1}, {Key: "g", Value: 1}},
			Options: options.Index().SetName("spg").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "p", Value: 1}, {Key: "o", Value: 1}, {Key: "g", Value: 1}},
			Options: options.Index().SetName("pog"),
		},
		{
			Keys:    bson.D{{Key: "o", Value: 1}, {Key: "g", Value: 1}},
			Options: options.Index().SetName("og"),
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
// or a sharded cluster; a standalone mongod cannot run them.
//
// Nothing in this package depends on the answer — the storage layout is built
// so that every operation needing atomicity touches exactly one document — but
// store.Store asks, and answering honestly is free.
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

// opCtx derives the context for one operation from the caller's. The returned
// cancel func must always be called, even when no timeout is configured.
func (s *MongoStore) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.timeout)
}

// wrap names the operation on a driver error so a caller can tell what failed.
// The driver's error stays reachable through errors.Is/As, which is how a
// caller tells a cancelled context (context.Canceled) from an unreachable
// server (mongo.IsNetworkError, mongo.CommandError).
func wrap(op string, err error) error {
	return fmt.Errorf("mongostore: %s: %w", op, err)
}

// ContextAware reports true: named graphs are stored in the g field.
func (s *MongoStore) ContextAware() bool { return true }

// TransactionAware reports whether the server this store is connected to can
// run transactions — a replica set or a sharded cluster can, a standalone
// mongod cannot.
//
// It is informational. Set does not need a transaction to be atomic here, so a
// false answer costs nothing.
func (s *MongoStore) TransactionAware() bool { return s.transactional }

// graphKey maps a context term to the stored graph key.
//
// nil and store.DefaultGraph address the default graph (goRDFlib v0.5.3 gives
// unnamed graphs that identifier). Any other term, a blank node included,
// names a graph: TriG allows `_:g { }`.
func graphKey(graph term.Term) string {
	if store.IsDefaultGraph(graph) {
		return defaultGraphKey
	}
	return term.TermKey(graph)
}

// subjectPredicateKey is the filter that identifies one stored document: the
// subject, predicate and graph a set of objects hangs off.
func subjectPredicateKey(subj term.Subject, pred term.URIRef, gk string) bson.D {
	return bson.D{
		{Key: "s", Value: term.TermKey(subj)},
		{Key: "p", Value: term.TermKey(pred)},
		{Key: "g", Value: gk},
	}
}

// --- writes ----------------------------------------------------------------

// Add inserts a triple. Adding the same triple twice is a no-op.
func (s *MongoStore) Add(ctx context.Context, t term.Triple, graph term.Term) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	_, err := s.tripl.UpdateOne(c,
		subjectPredicateKey(t.Subject, t.Predicate, graphKey(graph)),
		bson.D{{Key: "$addToSet", Value: bson.D{{Key: "o", Value: term.TermKey(t.Object)}}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return wrap("Add", err)
	}
	return nil
}

// AddN batch-inserts quads. Duplicates, within the batch or against what is
// already stored, are ignored. Without transactions a failure part-way leaves
// the chunks already written in place; the retry is safe because the writes are
// idempotent.
func (s *MongoStore) AddN(ctx context.Context, quads []term.Quad) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(quads) == 0 {
		return nil
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	// Quads that share a subject, predicate and graph land in the same
	// document, so grouping them here turns what would be one write per quad
	// into one write per document. A batch of 5000 quads about a handful of
	// subjects becomes a handful of writes.
	type group struct {
		key     bson.D
		objects bson.A
		seen    map[string]struct{}
	}
	order := make([]string, 0, len(quads))
	groups := make(map[string]*group, len(quads))

	for _, q := range quads {
		gk := graphKey(q.Graph)
		id := term.TermKey(q.Subject) + "\x00" + term.TermKey(q.Predicate) + "\x00" + gk
		g, ok := groups[id]
		if !ok {
			g = &group{
				key:  subjectPredicateKey(q.Subject, q.Predicate, gk),
				seen: make(map[string]struct{}),
			}
			groups[id] = g
			order = append(order, id)
		}
		ok2 := term.TermKey(q.Object)
		if _, dup := g.seen[ok2]; dup {
			continue
		}
		g.seen[ok2] = struct{}{}
		g.objects = append(g.objects, ok2)
	}

	// Chunked so that one enormous AddN cannot exceed the server's write
	// command limits. $addToSet is idempotent, so an overlapping or retried
	// chunk changes nothing.
	const chunk = 1000
	models := make([]mongo.WriteModel, 0, min(chunk, len(order)))
	flush := func() error {
		if len(models) == 0 {
			return nil
		}
		_, err := s.tripl.BulkWrite(c, models, options.BulkWrite().SetOrdered(false))
		models = models[:0]
		if err != nil {
			return wrap("AddN", err)
		}
		return nil
	}

	for _, id := range order {
		g := groups[id]
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(g.key).
			SetUpdate(bson.D{{Key: "$addToSet", Value: bson.D{
				{Key: "o", Value: bson.D{{Key: "$each", Value: g.objects}}},
			}}}).
			SetUpsert(true))
		if len(models) == chunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// Remove deletes every triple matching the pattern from the given graph.
func (s *MongoStore) Remove(ctx context.Context, pattern term.TriplePattern, graph term.Term) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	gk := graphKey(graph)
	filter := bson.D{{Key: "g", Value: gk}}
	if pattern.Subject != nil {
		filter = append(filter, bson.E{Key: "s", Value: term.TermKey(pattern.Subject)})
	}
	if pattern.Predicate != nil {
		filter = append(filter, bson.E{Key: "p", Value: term.TermKey(*pattern.Predicate)})
	}

	// With the object unbound, every matching document loses all its triples,
	// so the documents themselves go.
	if pattern.Object == nil {
		if _, err := s.tripl.DeleteMany(c, filter); err != nil {
			return wrap("Remove", err)
		}
		return nil
	}

	// With the object bound, only that element leaves each document. Documents
	// left holding nothing are then deleted, because an empty one would still
	// make its graph show up in Contexts.
	objKey := term.TermKey(pattern.Object)
	pullFrom := append(filter, bson.E{Key: "o", Value: objKey})
	if _, err := s.tripl.UpdateMany(c, pullFrom,
		bson.D{{Key: "$pull", Value: bson.D{{Key: "o", Value: objKey}}}}); err != nil {
		return wrap("Remove", err)
	}
	_, err := s.tripl.DeleteMany(c, bson.D{
		{Key: "g", Value: gk},
		{Key: "o", Value: bson.D{{Key: "$size", Value: 0}}},
	})
	if err != nil {
		return wrap("Remove", err)
	}
	return nil
}

// Set replaces every triple matching (s, p, *) in the graph with the given
// triple.
//
// It is a single-document update, and MongoDB makes those atomic on every
// deployment — standalone included. That is the reason for the storage layout:
// store.Store requires that a concurrent reader never observe the interval
// where the old value is gone and the new one is not yet there, and a
// triple-per-document layout could only deliver that with a transaction, which
// a standalone mongod cannot run.
func (s *MongoStore) Set(ctx context.Context, t term.Triple, graph term.Term) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	_, err := s.tripl.UpdateOne(c,
		subjectPredicateKey(t.Subject, t.Predicate, graphKey(graph)),
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "o", Value: bson.A{term.TermKey(t.Object)}},
		}}},
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		return wrap("Set", err)
	}
	return nil
}

// --- namespaces ------------------------------------------------------------

// Bind associates a prefix with a namespace, replacing any previous binding for
// that prefix.
func (s *MongoStore) Bind(ctx context.Context, prefix string, namespace term.URIRef) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	_, err := s.ns.ReplaceOne(c,
		bson.D{{Key: "_id", Value: prefix}},
		bson.D{{Key: "_id", Value: prefix}, {Key: "ns", Value: namespace.Value()}},
		options.Replace().SetUpsert(true))
	if err != nil {
		return wrap("Bind", err)
	}
	return nil
}

// Namespace returns the namespace bound to a prefix.
func (s *MongoStore) Namespace(ctx context.Context, prefix string) (term.URIRef, bool, error) {
	if err := ctx.Err(); err != nil {
		return term.URIRef{}, false, err
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	var doc struct {
		NS string `bson:"ns"`
	}
	err := s.ns.FindOne(c, bson.D{{Key: "_id", Value: prefix}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return term.URIRef{}, false, nil
	}
	if err != nil {
		return term.URIRef{}, false, wrap("Namespace", err)
	}
	return term.NewURIRefUnsafe(doc.NS), true, nil
}

// Prefix returns a prefix bound to the namespace.
func (s *MongoStore) Prefix(ctx context.Context, namespace term.URIRef) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	c, cancel := s.opCtx(ctx)
	defer cancel()

	var doc struct {
		ID string `bson:"_id"`
	}
	err := s.ns.FindOne(c, bson.D{{Key: "ns", Value: namespace.Value()}}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, wrap("Prefix", err)
	}
	return doc.ID, true, nil
}

// Namespaces iterates over every binding.
func (s *MongoStore) Namespaces(ctx context.Context) store.NamespaceIterator {
	return func(yield func(store.NamespaceBinding, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(store.NamespaceBinding{}, err)
			return
		}
		c, cancel := s.opCtx(ctx)
		defer cancel()

		cur, err := s.ns.Find(c, bson.D{})
		if err != nil {
			yield(store.NamespaceBinding{}, wrap("Namespaces", err))
			return
		}
		defer cur.Close(c)

		for cur.Next(c) {
			var doc struct {
				ID string `bson:"_id"`
				NS string `bson:"ns"`
			}
			if err := cur.Decode(&doc); err != nil {
				yield(store.NamespaceBinding{}, wrap("Namespaces: decode", err))
				return
			}
			b := store.NamespaceBinding{Prefix: doc.ID, Namespace: term.NewURIRefUnsafe(doc.NS)}
			if !yield(b, nil) {
				return
			}
		}
		if err := cur.Err(); err != nil {
			yield(store.NamespaceBinding{}, wrap("Namespaces", err))
		}
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
