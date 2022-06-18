package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	if s.db == nil {
		return fmt.Errorf("storage cannot be nil")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	err := s.db.Update(func(txn *badger.Txn) error {
		wb := new(engine_util.WriteBatch)
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				wb.SetCF(data.Cf, data.Key, data.Value)
			case storage.Delete:
				wb.DeleteCF(data.Cf, data.Key)
			default:
			}
		}
		return wb.WriteToDB(s.db)
	})

	return err
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

// Close implements storage.StorageReader
func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

// GetCF implements storage.StorageReader
func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return []byte(nil), nil
	}
	return val, nil
}

// IterCF implements storage.StorageReader
func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
