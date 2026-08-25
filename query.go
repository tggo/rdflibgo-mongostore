package mongostore

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"
)

// patternFilter builds the query document for a triple pattern in a context.
//
// The graph key is always present, which keeps every read and delete scoped to
// one graph. A bound object matches against the object array by containment,
// which is ordinary MongoDB array semantics and is served by the multikey pog
// and og indexes.
func patternFilter(pattern term.TriplePattern, ctx term.Term) bson.D {
	filter := bson.D{{Key: "g", Value: graphKey(ctx)}}
	if pattern.Subject != nil {
		filter = append(filter, bson.E{Key: "s", Value: term.TermKey(pattern.Subject)})
	}
	if pattern.Predicate != nil {
		filter = append(filter, bson.E{Key: "p", Value: term.TermKey(*pattern.Predicate)})
	}
	if pattern.Object != nil {
		filter = append(filter, bson.E{Key: "o", Value: term.TermKey(pattern.Object)})
	}
	return filter
}

// tripleRow is the decoded form of a stored document: one subject and
// predicate, and every object they point at in this graph.
type tripleRow struct {
	S string   `bson:"s"`
	P string   `bson:"p"`
	O []string `bson:"o"`
}

// subjectPredicate decodes the two scalar fields shared by every triple in the
// document.
func (r tripleRow) subjectPredicate() (term.Subject, term.URIRef, error) {
	s, err := term.TermFromKey(r.S)
	if err != nil {
		return nil, term.URIRef{}, err
	}
	subj, ok := s.(term.Subject)
	if !ok {
		return nil, term.URIRef{}, &decodeError{field: "s", key: r.S, reason: "not a subject term"}
	}

	p, err := term.TermFromKey(r.P)
	if err != nil {
		return nil, term.URIRef{}, err
	}
	pred, ok := p.(term.URIRef)
	if !ok {
		return nil, term.URIRef{}, &decodeError{field: "p", key: r.P, reason: "not a URIRef"}
	}
	return subj, pred, nil
}

type decodeError struct {
	field  string
	key    string
	reason string
}

func (e *decodeError) Error() string {
	return "mongostore: cannot decode " + e.field + " " + e.key + ": " + e.reason
}

// Triples iterates over the triples matching the pattern in the given context.
//
// The cursor is opened when iteration starts, not when Triples is called, and
// closed when it ends — including when the consumer stops early by returning
// false.
func (s *MongoStore) Triples(pattern term.TriplePattern, ctx term.Term) store.TripleIterator {
	filter := patternFilter(pattern, ctx)

	// With the object bound, only the matching element of the array is worth
	// transferring. A subject with a wide predicate would otherwise ship its
	// whole object list to answer a single-triple lookup.
	projection := bson.D{
		{Key: "_id", Value: 0},
		{Key: "s", Value: 1},
		{Key: "p", Value: 1},
		{Key: "o", Value: 1},
	}
	if pattern.Object != nil {
		projection[3] = bson.E{Key: "o", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
			{Key: "$eq", Value: term.TermKey(pattern.Object)},
		}}}}
	}
	opts := options.Find().SetProjection(projection)

	return func(yield func(term.Triple) bool) {
		c, cancel := s.opCtx()
		defer cancel()

		cur, err := s.tripl.Find(c, filter, opts)
		if err != nil {
			s.fail("Triples", err)
			return
		}
		defer cur.Close(c)

		for cur.Next(c) {
			var row tripleRow
			if err := cur.Decode(&row); err != nil {
				s.fail("Triples", err)
				continue
			}
			subj, pred, err := row.subjectPredicate()
			if err != nil {
				s.fail("Triples", err)
				continue
			}
			for _, objKey := range row.O {
				obj, err := term.TermFromKey(objKey)
				if err != nil {
					s.fail("Triples", err)
					continue
				}
				if !yield(term.Triple{Subject: subj, Predicate: pred, Object: obj}) {
					return
				}
			}
		}
		s.fail("Triples", cur.Err())
	}
}

// unwindPipeline turns matching documents back into one entry per triple.
// Anything that counts or pages over triples rather than over documents needs
// it, because a document holds a whole object list.
func unwindPipeline(pattern term.TriplePattern, ctx term.Term) mongo.Pipeline {
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: patternFilter(pattern, ctx)}},
		bson.D{{Key: "$unwind", Value: "$o"}},
	}
	// $unwind emits every object of each matching document, including the ones
	// the bound-object filter was never meant to select, so the filter has to
	// be applied again after it.
	if pattern.Object != nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.D{
			{Key: "o", Value: term.TermKey(pattern.Object)},
		}}})
	}
	return pipeline
}

// TriplesWithLimit applies LIMIT and OFFSET in the server, over triples rather
// than over stored documents.
//
// A limit of zero or less means "no limit", matching the other backends, and a
// negative offset is treated as zero. Store promises no iteration order, and
// this backend does not impose one either, so paging across separate calls is
// only meaningful together with a caller-side sort.
func (s *MongoStore) TriplesWithLimit(pattern term.TriplePattern, ctx term.Term, limit, offset int) store.TripleIterator {
	pipeline := unwindPipeline(pattern, ctx)
	if offset > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$skip", Value: int64(offset)}})
	}
	if limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: int64(limit)}})
	}

	return func(yield func(term.Triple) bool) {
		c, cancel := s.opCtx()
		defer cancel()

		cur, err := s.tripl.Aggregate(c, pipeline)
		if err != nil {
			s.fail("TriplesWithLimit", err)
			return
		}
		defer cur.Close(c)

		for cur.Next(c) {
			// After $unwind the object is a scalar again, so this decodes into
			// a single-triple shape rather than tripleRow.
			var row struct {
				S string `bson:"s"`
				P string `bson:"p"`
				O string `bson:"o"`
			}
			if err := cur.Decode(&row); err != nil {
				s.fail("TriplesWithLimit", err)
				continue
			}
			subj, pred, err := tripleRow{S: row.S, P: row.P}.subjectPredicate()
			if err != nil {
				s.fail("TriplesWithLimit", err)
				continue
			}
			obj, err := term.TermFromKey(row.O)
			if err != nil {
				s.fail("TriplesWithLimit", err)
				continue
			}
			if !yield(term.Triple{Subject: subj, Predicate: pred, Object: obj}) {
				return
			}
		}
		s.fail("TriplesWithLimit", cur.Err())
	}
}

// Len returns the number of triples in the given context.
// A nil or blank node context means the default graph.
func (s *MongoStore) Len(ctx term.Term) int {
	return s.Count(term.TriplePattern{}, ctx)
}

// Count returns the number of triples matching the pattern without
// materializing them.
//
// A bound object means at most one triple per document, so counting documents
// answers it directly. Otherwise the object lists have to be summed, which is
// what the aggregation does.
func (s *MongoStore) Count(pattern term.TriplePattern, ctx term.Term) int {
	c, cancel := s.opCtx()
	defer cancel()

	if pattern.Object != nil {
		n, err := s.tripl.CountDocuments(c, patternFilter(pattern, ctx))
		if err != nil {
			s.fail("Count", err)
			return 0
		}
		return int(n)
	}

	cur, err := s.tripl.Aggregate(c, mongo.Pipeline{
		bson.D{{Key: "$match", Value: patternFilter(pattern, ctx)}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "n", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$size", Value: "$o"}}}}},
		}}},
	})
	if err != nil {
		s.fail("Count", err)
		return 0
	}
	defer cur.Close(c)

	total := 0
	for cur.Next(c) {
		var row struct {
			N int `bson:"n"`
		}
		if err := cur.Decode(&row); err != nil {
			s.fail("Count", err)
			return 0
		}
		total += row.N
	}
	if err := cur.Err(); err != nil {
		s.fail("Count", err)
		return 0
	}
	return total
}

// Exists reports whether at least one triple matches the pattern. It asks the
// server for one document rather than counting them all.
func (s *MongoStore) Exists(pattern term.TriplePattern, ctx term.Term) bool {
	c, cancel := s.opCtx()
	defer cancel()

	err := s.tripl.FindOne(c, patternFilter(pattern, ctx),
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).
		Err()
	switch {
	case err == nil:
		return true
	case errors.Is(err, mongo.ErrNoDocuments):
		return false
	default:
		s.fail("Exists", err)
		return false
	}
}

// Contexts iterates over the named graphs, optionally restricted to those
// holding a particular triple.
//
// The default graph is not a context and is never reported, which is why the
// filter excludes the empty graph key.
func (s *MongoStore) Contexts(triple *term.Triple) store.TermIterator {
	return func(yield func(term.Term) bool) {
		c, cancel := s.opCtx()
		defer cancel()

		filter := bson.D{{Key: "g", Value: bson.D{{Key: "$ne", Value: defaultGraphKey}}}}
		if triple != nil {
			filter = append(filter,
				bson.E{Key: "s", Value: term.TermKey(triple.Subject)},
				bson.E{Key: "p", Value: term.TermKey(triple.Predicate)},
				bson.E{Key: "o", Value: term.TermKey(triple.Object)},
			)
		}

		// Grouping streams one document per distinct graph. Distinct would
		// return every graph key in a single reply instead, which caps the
		// number of named graphs at whatever fits in one document.
		cur, err := s.tripl.Aggregate(c, mongo.Pipeline{
			bson.D{{Key: "$match", Value: filter}},
			bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$g"}}}},
		})
		if err != nil {
			s.fail("Contexts", err)
			return
		}
		defer cur.Close(c)

		for cur.Next(c) {
			var row struct {
				ID string `bson:"_id"`
			}
			if err := cur.Decode(&row); err != nil {
				s.fail("Contexts", err)
				continue
			}
			g, err := term.TermFromKey(row.ID)
			if err != nil {
				s.fail("Contexts", err)
				continue
			}
			if !yield(g) {
				return
			}
		}
		s.fail("Contexts", cur.Err())
	}
}

// Collection exposes the triples collection.
//
// It is here for the things store.Store cannot express: a custom aggregation, a
// backup, an index tuned for a particular workload, or a test that wants to see
// what was actually written. Writing through it bypasses every invariant this
// package maintains, so treat it as read-mostly.
func (s *MongoStore) Collection() *mongo.Collection { return s.tripl }

// NamespaceCollection exposes the namespace-binding collection, on the same
// terms as Collection.
func (s *MongoStore) NamespaceCollection() *mongo.Collection { return s.ns }

// DropDatabase removes the store's entire database, indexes included. It is not
// part of store.Store; it exists so that a test or a tear-down can reclaim
// everything in one call.
func (s *MongoStore) DropDatabase(ctx context.Context) error {
	return s.db.Drop(ctx)
}

// Drop removes every triple and every namespace binding, leaving the indexes in
// place. It is not part of store.Store; it exists because a test or a reload
// needs a cheap way to start over.
func (s *MongoStore) Drop(ctx context.Context) error {
	if _, err := s.tripl.DeleteMany(ctx, bson.D{}); err != nil {
		return err
	}
	_, err := s.ns.DeleteMany(ctx, bson.D{})
	return err
}
