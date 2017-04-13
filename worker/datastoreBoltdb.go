package main

import (
	"bytes"
	"github.com/boltdb/bolt"
	"time"
)

const (
	boltdbPath = "schedulerWorker.db"
	bucketName = "main"
)

func getBoltdbStore() (DataStore, error) {
	boltdb, err := bolt.Open(boltdbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = boltdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	return &BoltdbStore{boltdb}, err
}

type BoltdbStore struct {
	*bolt.DB
}

func (bdb *BoltdbStore) Set(topic string, when time.Time, data []byte) error {
	return bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Put([]byte(when.Format(time.RFC3339)), data)
	})
}

func (bdb *BoltdbStore) GetLatest(window time.Duration) (time.Time, error) {
	var ret time.Time
	bdb.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucketName)).Cursor()
		k, _ := c.First()
		if k == nil {
			return nil
		}

		t, err := time.Parse(time.RFC3339, string(k))
		if err != nil {
			return err
		}

		ret = t
		return nil
	})

	return ret, nil
}

func (bdb *BoltdbStore) Delete(t time.Time) error {
	return bdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Delete([]byte(t.Format(time.RFC3339)))
	})
}

func (bdb *BoltdbStore) GetBefore(t time.Time) (keys []time.Time, values [][]byte, err error) {
	err = bdb.View(func(tx *bolt.Tx) error {
		maxKey := []byte(t.Format(time.RFC3339))
		c := tx.Bucket([]byte(bucketName)).Cursor()
		for k, v := c.First(); k != nil && bytes.Compare(k, maxKey) <= 0; k, v = c.Next() {
			t, err := time.Parse(time.RFC3339, string(k))
			if err != nil {
				return err
			}
			keys = append(keys, t)
			values = append(values, v)
		}
		return nil
	})
	return
}

func (bdb *BoltdbStore) Close() error {
	return bdb.Close()
}
