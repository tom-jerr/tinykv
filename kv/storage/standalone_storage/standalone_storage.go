package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.

type StandAloneStorage struct {
	// Your Data Here (1).
	badger_db *badger.DB
	txn       *badger.Txn
	iter      *engine_util.BadgerIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandAloneStorage{db, nil, nil}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.txn = s.badger_db.NewTransaction(true)
	if s.txn == nil {
		return badger.ErrConflict
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.txn.Commit(); err != nil {
		return err
	}
	if s.iter != nil && s.iter.Valid() {
		s.iter.Close()
	}
	defer s.txn.Discard()
	defer s.badger_db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		if err := s.txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value()); err != nil {
			return err
		}
	}

	return nil
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	// Your Code Here (1).
	// get value
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	// Your Code Here (1).
	// create iterator
	s.iter = engine_util.NewCFIterator(cf, s.txn)
	return s.iter
}

func (s *StandAloneStorage) Close() {
	// Your Code Here (1).
	s.iter.Close()
}
