package main

import (
	"os"
	"sync"
)

//DB is user facing , wrapping our dal layer
type DB struct {
	rwlock sync.RWMutex
	*dal
}

func Open(path string, options *Options) (*DB, error) {
	var err error
	options.pageSize = os.Getpagesize()
	dal, err := newDal(path, options)
	if err != nil {
		return nil, err
	}
	db := &DB{
		sync.RWMutex{},
		dal,
	}
	return db, nil
}

func (db *DB) Close() error {
	//calls Dal file close function
	return db.close()
}

func (db *DB) ReadTx() *tx {
	db.rwlock.RLock()
	return newTx(db, false)
}

func (db *DB) WriteTx() *tx {
	db.rwlock.Lock()
	return newTx(db, true)
}
