package kv

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// MockedKV .
type MockedKV struct {
	sync.Mutex
	pool    sync.Map
	nextSeq uint64
}

// NewMockedKV .
func NewMockedKV() *MockedKV {
	return &MockedKV{
		nextSeq: 1,
	}
}

// Open .
func (m *MockedKV) Open(path string, mode os.FileMode, timeout time.Duration) error {
	return nil
}

// Close .
func (m *MockedKV) Close() error {
	keys := []interface{}{}
	m.pool.Range(func(key, _ interface{}) bool {
		keys = append(keys, key)
		return true
	})

	for _, key := range keys {
		m.pool.Delete(key)
	}

	return nil
}

// NextSequence .
func (m *MockedKV) NextSequence() (nextSeq uint64, err error) {
	m.Lock()
	defer m.Unlock()
	nextSeq = m.nextSeq
	m.nextSeq++
	return
}

// Put .
func (m *MockedKV) Put(key, value []byte) (err error) {
	m.pool.Store(string(key), value)
	return
}

// Get .
func (m *MockedKV) Get(key []byte) (value []byte, err error) {
	raw, ok := m.pool.Load(string(key))
	if !ok {
		err = fmt.Errorf("no such key: %s", key)
		return
	}

	if value, ok = raw.([]byte); !ok {
		err = fmt.Errorf("value must be a []byte, but %v", raw)
	}

	return
}

// Delete .
func (m *MockedKV) Delete(key []byte) (err error) {
	m.pool.Delete(string(key))
	return
}

// Scan .
func (m *MockedKV) Scan(prefix []byte) (<-chan ScanEntry, func()) {
	ch := make(chan ScanEntry)

	exit := make(chan struct{})
	abort := func() {
		close(exit)
	}

	go func() {
		defer close(ch)

		m.pool.Range(func(rkey, rvalue interface{}) (next bool) {
			var entry MockedScanEntry
			defer func() {
				select {
				case <-exit:
					next = false
				case ch <- entry:
					next = true
				}
			}()

			var ok bool
			if entry.Key, ok = rkey.(string); !ok {
				entry.Err = fmt.Errorf("key must be a string, but %v", rkey)
				return
			}

			if !strings.HasPrefix(entry.Key, string(prefix)) {
				return
			}

			if entry.Value, ok = rvalue.([]byte); !ok {
				entry.Err = fmt.Errorf("value must be a []byte, but %v", rvalue)
				return
			}

			return
		})
	}()

	return ch, abort
}

// MockedScanEntry .
type MockedScanEntry struct {
	Err   error
	Key   string
	Value []byte
}

// Pair .
func (e MockedScanEntry) Pair() ([]byte, []byte) {
	return []byte(e.Key), e.Value
}

// Error .
func (e MockedScanEntry) Error() error {
	return e.Err
}
