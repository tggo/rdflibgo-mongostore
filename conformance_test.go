//go:build integration

package mongostore_test

import (
	"testing"

	"github.com/tggo/goRDFlib/store"
	"github.com/tggo/goRDFlib/store/storetest"

	"github.com/tggo/rdflibgo-mongostore"
)

// TestConformance runs goRDFlib's shared store.Store conformance suite.
//
// This is the load-bearing test of the package. Everything else here checks
// behaviour specific to MongoDB; this one checks that the backend is
// interchangeable with the in-memory, Badger and SQLite stores — which is the
// only property that makes it usable by Graph, SPARQL, SHACL and the parsers.
//
// There is no Known map: this backend has no declared limitation, and it must
// stay that way. If a case here starts failing, the fix belongs in the store,
// not in an exemption.
func (s *MongoStoreSuite) TestConformance() {
	t := s.T()

	// Every subtest gets its own database, and Reopen has to land on the same
	// one — a second handle over the same data is exactly what persistence
	// means for a server-backed store.
	dbs := map[store.Store]string{}

	storetest.Run(t, storetest.Config{
		New: func(t *testing.T) store.Store {
			db := s.nextDB()
			st := s.newStoreInDB(t, db)
			dbs[st] = db
			return st
		},
		Reopen: func(t *testing.T, st store.Store) store.Store {
			db := dbs[st]
			if db == "" {
				t.Fatal("Reopen was handed a store New did not create")
			}
			if err := st.(*mongostore.MongoStore).Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			return s.newStoreInDB(t, db)
		},
	})
}
