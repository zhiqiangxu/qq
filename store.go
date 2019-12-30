package qq

import (
	"path"

	"github.com/dgraph-io/badger"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type store interface {
	Set(k, v []byte) error
	// Insert will do Set if k not exists, otherwise ErrKeyALreadyExists
	Insert(k, v []byte) (err error)
	MultiInsert(ks, vs [][]byte) (err error)
	// Delete will return ErrKeyNotExists if k not exists
	Delete(k []byte) error
	PrefixScan(prefix []byte, f func(k, v []byte) error) error
	Get(k []byte) ([]byte, error)
}

var _ store = (*Store)(nil)

// Store for qq
type Store struct {
	db *badger.DB
}

// OpenOrCreateStore for open or create a Store
func OpenOrCreateStore(dataDir string) (s *Store) {
	storeDir := path.Join(dataDir, storeSubPath)

	opts := badger.DefaultOptions(storeDir)
	db, err := badger.Open(opts)
	if err != nil {
		logger.Instance().Fatal("badger.Open", zap.String("storeDir", storeDir), zap.Error(err))
	}

	s = &Store{db: db}
	return
}

// Set k v pair
func (s *Store) Set(k, v []byte) (err error) {
	return
}

// Insert will Set k v pair if not already exist
func (s *Store) Insert(k, v []byte) (err error) {
	return
}

// MultiInsert for multi insert
func (s *Store) MultiInsert(ks, vs [][]byte) (err error) {
	return
}

// Delete k
func (s *Store) Delete(k []byte) (err error) {
	return
}

// PrefixScan for prefix scan
func (s *Store) PrefixScan(prefix []byte, f func(k, v []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				return f(k, v)
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Get v by k
func (s *Store) Get(k []byte) (v []byte, err error) {
	return
}
