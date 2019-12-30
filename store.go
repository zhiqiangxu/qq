package qq

import (
	"path"

	"github.com/dgraph-io/badger"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type store interface {
	Set(k, v []byte) error
	Insert(k, v []byte) error
	Delete(k []byte) error
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

// Delete k
func (s *Store) Delete(k []byte) (err error) {
	return
}

// Get v by k
func (s *Store) Get(k []byte) (v []byte, err error) {
	return
}
