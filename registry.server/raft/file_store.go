package raft

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"sync"
)

func newFileStore() *FileStore {
	return &FileStore{
		kv: make(map[string]string),
	}
}

type FileStore struct {
	mu sync.Mutex
	kv map[string]string
}

func (s *FileStore) Apply(data []byte) {
	var e entry
	if err := json.Unmarshal(data, &e); err != nil {
		log.Println("unable to Unmarshal entry", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[e.Key] = e.Value
}

func (s *FileStore) Snapshot() (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, err := json.Marshal(&s.kv)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(string(buf))), nil
}

func (s *FileStore) Restore(r io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	err = json.Unmarshal(buf, &s.kv)
	if err != nil {
		return err
	}

	return r.Close()
}

func (s *FileStore) Read(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key]
}
