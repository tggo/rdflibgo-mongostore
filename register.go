package mongostore

import (
	"os"

	"github.com/tggo/goRDFlib/plugin"
	"github.com/tggo/goRDFlib/store"
)

// EnvURI is the environment variable the plugin factory reads its connection
// string from.
const EnvURI = "RDFLIBGO_MONGO_URI"

// EnvDatabase optionally overrides the database name used by the plugin
// factory. It defaults to "rdf".
const EnvDatabase = "RDFLIBGO_MONGO_DB"

// init registers the "mongo" store type with the goRDFlib plugin registry.
//
// The factory signature has nowhere to put a connection string, so it reads one
// from the environment. Anything beyond a URI and a database name — a timeout,
// a shared client, an error handler — needs New directly.
//
// The factory panics when it cannot connect, because plugin.RegisterStore's
// factory cannot return an error and a store that silently swallows every write
// is worse than a stack trace.
func init() {
	plugin.RegisterStore("mongo", func() store.Store {
		uri := os.Getenv(EnvURI)
		if uri == "" {
			panic("mongostore: the \"mongo\" store plugin needs a connection string in " +
				EnvURI + "; call mongostore.New(WithURI(...)) instead to pass one directly")
		}
		db := os.Getenv(EnvDatabase)
		if db == "" {
			db = "rdf"
		}
		s, err := New(WithURI(uri), WithDatabase(db))
		if err != nil {
			panic("mongostore: " + err.Error())
		}
		return s
	})
}
