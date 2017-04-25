package main

import "time"

type DataStore interface {
	Set(topic string, when time.Time, data []byte) error
	GetBefore(t time.Time) ([]time.Time, [][]byte, error)
	GetLatest() (time.Time, error)
	Delete(t time.Time) error
	Close() error
}
