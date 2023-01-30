package standalone_storage

import (
	badger "github.com/dgraph-io/badger/v3"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	config.Config
	*badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		Config: *conf,
	}
}

func (s *StandAloneStorage) Start() error {
	db, err := badger.Open(badger.DefaultOptions(s.Config.DBPath))
	if err != nil {
		return err
	}
	s.DB = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.DB.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Return StorageReader that supports key/value's point get and scan operations
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return nil
}

type standaloneReader struct {
	inner *StandAloneStorage
}

func (sr *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	err := sr.inner.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return nil, err
		}
		return item.Value()
	})

	if err != nil {
		return nil, err
	}

	return nil, nil
}
