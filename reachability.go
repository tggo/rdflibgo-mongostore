package mongostore

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/term"
)

// Reachable answers a transitive-closure query with a single $graphLookup
// aggregation, so a SPARQL property path such as `<p>+` costs one round trip
// instead of shipping every edge of the predicate to the client.
//
// This is the reason to run RDF on MongoDB rather than on a generic document
// store, and it is the one operation here that a client-side traversal cannot
// match on a large graph.
//
// # Shape of the pipeline
//
// The seeds — the nodes one hop from Start — are collected first and handed to
// $graphLookup as a single array. Running the lookup once from a grouped
// document rather than once per matching edge matters: with one lookup per edge
// the reached set is materialized once per edge, and the stage's memory cap is
// hit at a fraction of the graph size.
//
// # When it declines
//
// $graphLookup is capped at 100 MiB per stage and ignores allowDiskUse, so a
// wide closure — `foaf:knows+` over a social graph — will fail rather than
// spill. That is expected, not exceptional: store.ReachabilityStore lets a
// backend return an error meaning "compute this yourself", and paths.MulPath
// then falls back to its own traversal. Because the contract requires a
// complete result or none at all, the caller can fall back safely: nothing has
// been emitted.
//
// Cycles need no special handling. $graphLookup performs a breadth-first search
// and never revisits a document it has already seen.
func (s *MongoStore) Reachable(q store.ReachabilityQuery) ([]term.Term, error) {
	if q.Start == nil {
		return nil, fmt.Errorf("mongostore: %w: no start node", store.ErrReachabilityUnsupported)
	}

	gk := graphKey(q.Context)
	startKey := term.TermKey(q.Start)

	// Which field an edge is entered by and which it is left by. Forward
	// traversal enters at s and leaves at o; inverse traversal is the mirror
	// image, which is all `^<p>` amounts to here.
	fromField, toField := "o", "s"
	if q.Inverse {
		fromField, toField = "s", "o"
	}

	predMatch := predicateFilter(q.Predicates, q.Negated)

	// Edges leaving Start. Their far ends are both the depth-1 answers and the
	// seeds the recursion starts from.
	seedMatch := bson.D{{Key: "g", Value: gk}, {Key: toField, Value: startKey}}
	seedMatch = append(seedMatch, predMatch...)

	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: seedMatch}},
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: nil},
			{Key: "seeds", Value: bson.D{{Key: "$addToSet", Value: "$" + fromField}}},
		}}},
	}

	// MaxDepth counts hops from Start. The $match above is hop 1, so the lookup
	// covers hops 2..MaxDepth and its own maxDepth — which counts recursion
	// levels beyond the seeds, starting at 0 — is MaxDepth-2. A MaxDepth of 1
	// therefore needs no lookup at all.
	runLookup := q.MaxDepth <= 0 || q.MaxDepth >= 2
	if runLookup {
		restrict := bson.D{{Key: "g", Value: gk}}
		restrict = append(restrict, predMatch...)

		lookup := bson.D{
			{Key: "from", Value: triplesCollection},
			{Key: "startWith", Value: "$seeds"},
			{Key: "connectFromField", Value: fromField},
			{Key: "connectToField", Value: toField},
			{Key: "as", Value: "reached"},
			{Key: "restrictSearchWithMatch", Value: restrict},
		}
		if q.MaxDepth >= 2 {
			lookup = append(lookup, bson.E{Key: "maxDepth", Value: int64(q.MaxDepth - 2)})
		}
		pipeline = append(pipeline,
			bson.D{{Key: "$graphLookup", Value: lookup}},
			bson.D{{Key: "$project", Value: bson.D{
				{Key: "nodes", Value: bson.D{{Key: "$setUnion", Value: bson.A{"$seeds", "$reached." + fromField}}}},
			}}},
		)
	} else {
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.D{
			{Key: "nodes", Value: "$seeds"},
		}}})
	}

	// Unwinding turns the node set into one document per node, so the reply is
	// streamed rather than capped by the 16 MiB document limit.
	pipeline = append(pipeline,
		bson.D{{Key: "$unwind", Value: "$nodes"}},
		bson.D{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$nodes"}}}},
	)

	c, cancel := s.opCtx()
	defer cancel()

	cur, err := s.tripl.Aggregate(c, pipeline)
	if err != nil {
		return nil, fmt.Errorf("mongostore: reachable: %w", err)
	}
	defer cur.Close(c)

	var out []term.Term
	for cur.Next(c) {
		var row struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, fmt.Errorf("mongostore: reachable: decode: %w", err)
		}
		t, err := term.TermFromKey(row.ID)
		if err != nil {
			return nil, fmt.Errorf("mongostore: reachable: %w", err)
		}
		out = append(out, t)
	}
	// A mid-cursor failure has to be reported rather than returned as a short
	// result: the contract is all-or-nothing precisely so the caller can fall
	// back to a local traversal without double-counting what it already got.
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("mongostore: reachable: %w", err)
	}
	return out, nil
}

// predicateFilter renders the predicate restriction as filter elements.
//
// An empty predicate set means "any predicate" in both polarities: an empty
// exclusion list excludes nothing.
func predicateFilter(preds []term.URIRef, negated bool) bson.D {
	if len(preds) == 0 {
		return nil
	}
	keys := make(bson.A, 0, len(preds))
	for _, p := range preds {
		keys = append(keys, term.TermKey(p))
	}
	op := "$in"
	if negated {
		op = "$nin"
	}
	if len(keys) == 1 && !negated {
		return bson.D{{Key: "p", Value: keys[0]}}
	}
	return bson.D{{Key: "p", Value: bson.D{{Key: op, Value: keys}}}}
}
