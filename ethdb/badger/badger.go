
// Copyright 2019 ChainSafe Systems (ON) Corp.
// This file is part of gossamer.
//
// The gossamer library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The gossamer library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the gossamer library. If not, see <http://www.gnu.org/licenses/>.

package badger

import (


	"errors"
	"fmt"
	"sync"
	"os"
  "github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/common"
	"github.com/dgraph-io/badger/v3"
)

var (
	errSnapshotReleased = errors.New("snapshot released")
	set2 = 1

)

const MaxInt = int(^uint(0) >> 1)

// Database contains directory path to data and db instance
type Database struct {
	file   string
	db     *badger.DB
	lock   sync.RWMutex
	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

}

func exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { return true, nil }
    if os.IsNotExist(err) { return false, nil }
    return false, err
}

// NewDatabase initializes Database instance
func New(file string, cache int, handles int, namespace string, readonly bool) (*Database, error) {

	_, err := os.Stat(file)
	if err == nil {
		file = file + "+"
	 }
	if os.IsNotExist(err) {
		os.MkdirAll(file, 0755)
	}
	opts := badger.DefaultOptions(file)
	opts.ValueDir = file
	opts.Dir = file
	if readonly == true {
		opts.ReadOnly = true
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Database{
		file: file,
		db:     db,
		quitChan: make(chan chan error),
	}, nil
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.file
}

func (db *Database) NewBatch() ethdb.Batch {
	return &BadgerBatch{db: db.db, b: db.db.NewWriteBatch(), keyvalue: map[string]string{}, max_size: MaxInt }
}

func (db *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &BadgerBatch{db: db.db, b: db.db.NewWriteBatch(), keyvalue: map[string]string{}, max_size: size}
}

type BadgerBatch struct {
	db      *badger.DB
	b       *badger.WriteBatch
	size     int
	max_size int
	keyvalue map[string]string
	lock sync.RWMutex
}

func (b *BadgerBatch) Put(key []byte, value []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	err := b.b.SetEntry(badger.NewEntry(common.CopyBytes(key), common.CopyBytes(value)).WithMeta(0))
	if err != nil {
		return err
	}
	b.size += len(key) + len(value)
	b.keyvalue[string(key)] = string(value)
	return nil
	}


func (b *BadgerBatch) Delete(key []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	err := b.b.Delete(key)
	if err != nil {
		return err
	}
	b.size += len(key)
	delete(b.keyvalue, string(key))
	return nil
}


func (b *BadgerBatch) Write() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	defer func() {
		b.b.Cancel()
		}()

	return b.b.Flush()
}

func (b *BadgerBatch) ValueSize() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.size
}

func (b *BadgerBatch) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.b = b.db.NewWriteBatch()
	b.keyvalue = make(map[string]string)
	b.size = 0

}

func (b *BadgerBatch) Replay(w ethdb.KeyValueWriter) error {
	b.lock.RLock()
	defer b.lock.RUnlock()
	for key, value := range b.keyvalue{
		if err := w.Put(common.CopyBytes([]byte(key)), common.CopyBytes([]byte(value))); err!=nil{
			return err
		}
	}
	return nil
}


func (db *Database) Stat(property string) (string, error) {
	return "", errors.New("unknown property")
}


// Put puts the given key / value to the queue
func (db *Database) Put(key []byte, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		return err
	})
}

// Has checks the given key exists already; returning true or false
func (db *Database) Has(key []byte) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	err = db.db.View(func(txn *badger.Txn) error {
		item, errr := txn.Get(key)
		if item != nil {
			exists = true
		}
		if errr == badger.ErrKeyNotFound {
			exists = false
			errr = nil
		}
		return errr
	})
	return exists, err
}

// Get returns the given key
func (db *Database) Get(key []byte) (data []byte, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	err = db.db.View(func(txn *badger.Txn) error {
		item, e := txn.Get(key)
		if e != nil {
			return e
		}
		data, e = item.ValueCopy(nil)
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Del removes the key from the queue and database
func (db *Database) Delete(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			err = nil
		}
		return err
	})
}

// Flush commits pending writes to disk
func (db *Database) Flush() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Sync()
}

// Close closes a DB
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		db.quitChan = nil
	}
	db.lock.Lock()
	defer db.lock.Unlock()
	if err := db.db.Close(); err != nil {
		return err
	}
	return nil
}

// ClearAll would delete all the data stored in DB.
func (db *Database) ClearAll() error {
	return db.db.DropAll()
}

type BadgerIterator struct {
	badgerIter *badger.Iterator
	first      []byte
	txn        *badger.Txn
	init       bool
	prefixx     []byte
	opts       badger.IteratorOptions
}

//type BadgerIterator struct {
//	txn  *badger.Txn
//	iter *badger.Iterator
//	init bool
//	lock sync.RWMutex
//}


func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {

	iteratorOptions := badger.IteratorOptions{
		Prefix:         prefix,
	}

	var it *badger.Iterator
	badgerTxn := db.db.NewTransaction(false)
	it = badgerTxn.NewIterator(iteratorOptions)

	badgerIterator := &BadgerIterator{
		badgerIter: it,
		first:      append(prefix,start...),
		txn:        badgerTxn,
		init:       false,
		prefixx: 		prefix,
		opts: 		  iteratorOptions,
	}
	return badgerIterator
}

//func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
//	txn := db.db.NewTransaction(false)
//	opts := badger.IteratorOptions{
//	PrefetchValues: true,
//	PrefetchSize:   100,
//	Reverse:        false,
//	AllVersions:    false,
//	Prefix:         prefix,

//}
//	iter := txn.NewIterator(opts)
//	return &BadgerIterator{
//		txn:  txn,
//		iter: iter,
//	}
//}


func (iter *BadgerIterator) Key() []byte {

	if iter.badgerIter.Valid() {
		return iter.badgerIter.Item().Key()
	}
	return nil
}

// // Key returns an item key
// func (i *BadgerIterator) Key() []byte {
// 	i.lock.RLock()
// 	defer i.lock.RUnlock()
// 	return i.iter.Item().Key()
// }

func (iter *BadgerIterator) Value() []byte {


	if iter.badgerIter.Valid() {
		item := iter.badgerIter.Item()
		ival, err := item.ValueCopy(nil)
		if err != nil {
			return nil
		}
		return ival
	}
	fmt.Println("iterator is invalid.......")
	return nil
}

// // Value returns a copy of the value of the item
// func (i *BadgerIterator) Value() []byte {
// 	i.lock.RLock()
// 	defer i.lock.RUnlock()
// 	val, err := i.iter.Item().ValueCopy(nil)
// 	if err != nil {
// 		log.Warn("value retrieval error ", "error", err)
// 	}
// 	return val
// }


func (iter *BadgerIterator) Next() bool {

	if !iter.init {
		iter.badgerIter.Rewind()
		iter.Seek(iter.first)
		iter.init = true
	} else {
		if !iter.badgerIter.Valid() {
			return false
		}
		iter.badgerIter.Next()
	}
	// if iter.prefixIter {
	// 	return iter.badgerIter.Valid() && iter.badgerIter.ValidForPrefix(iter.first)
	// }
	// // for general iterator
	return iter.badgerIter.Valid()  && iter.badgerIter.ValidForPrefix(iter.prefixx)
}

func (iter *BadgerIterator) Error() error {
	return nil
}

func (iter *BadgerIterator) Release() {

	iter.badgerIter.Close()
	iter.txn.Discard()
}
// Release closes the iterator and discards the created transaction.
// func (i *BadgerIterator) Release() {
// 	i.lock.Lock()
// 	defer i.lock.Unlock()
// 	i.iter.Close()
// 	i.txn.Discard()
// }

// Next rewinds the iterator to the zero-th position if uninitialized, and then will advance the iterator by one
// returns bool to ensure access to the item
// func (i *BadgerIterator) Next() bool {
// 	i.lock.RLock()
// 	defer i.lock.RUnlock()
//
// 	if !i.init {
// 		i.iter.Rewind()
// 		i.init = true
// 		return i.iter.Valid()
// 	}
//
// 	if !i.iter.Valid() {
// 		return false
// 	}
// 	i.iter.Next()
// 	return i.iter.Valid()
// }
//
// // Seek will look for the provided key if present and go to that position. If
// // absent, it would seek to the next smallest key
 func (iter *BadgerIterator) Seek(key []byte) {

 	 iter.badgerIter.Seek(key)

 }

func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

// NewSnapshot creates a database snapshot based on the current state.
// The created snapshot will not be affected by all following mutations
// happened on the database.
// Note don't forget to release the snapshot once it's used up, otherwise
// the stale data will never be cleaned up by the underlying compactor.
func (db *Database) NewSnapshot() (ethdb.Snapshot, error) {
	new_file := "/nvme/sioannou/storage/"
	new_file = new_file+"snap"
	new_db,errr := New(new_file,1024,1,"",false)
	if errr!=nil{
		return nil,errr
	}
	err := db.db.View(func(txn *badger.Txn) error {
  opts := badger.DefaultIteratorOptions
  opts.PrefetchSize = 10
  it := txn.NewIterator(opts)
  defer it.Close()
  for it.Rewind(); it.Valid(); it.Next() {
    item := it.Item()
    k := item.Key()
    err := item.Value(func(v []byte) error {
      new_db.Put(k,v)
      return nil
    })
    if err != nil {
      return err
    }
  }
  return nil
})
if err != nil {
	return nil,err
}
	return newSnapshot(new_db), nil
}


// snapshot wraps a batch of key-value entries deep copied from the in-memory
// database for implementing the Snapshot interface.
type snapshot struct {
	db   *Database
	lock sync.RWMutex
}

// newSnapshot initializes the snapshot with the given database instance.
func newSnapshot(db *Database) *snapshot {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return &snapshot{db: db}
}

// Has retrieves if a key is present in the snapshot backing by a key-value
// data store.
func (snap *snapshot) Has(key []byte) (exists bool, err error) {
	snap.lock.RLock()
	defer snap.lock.RUnlock()
	if snap.db == nil {
		return false, errSnapshotReleased
	}
	return snap.db.Has(key)
}

// Get retrieves the given key if it's present in the snapshot backing by
// key-value data store.
func (snap *snapshot) Get(key []byte) (data []byte, err error) {
	snap.lock.RLock()
	defer snap.lock.RUnlock()

	if snap.db == nil {
		return nil, errSnapshotReleased
	}
	return snap.db.Get(key)

}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (snap *snapshot) Release() {
	snap.lock.Lock()
	defer snap.lock.Unlock()
	snap.db = nil
}
