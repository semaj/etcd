// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/coreos/etcd/raftsnap"
	"log"
	"sync"
	"time"
)

// a key-value store backed by raft
type kvstore struct {
	startTimes  map[string]time.Time
	latencies   []time.Duration
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	timeLock    sync.Mutex
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *raftsnap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *raftsnap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, reader chan string, sizeReader chan int) *kvstore {
	s := &kvstore{
		proposeC:    proposeC,
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
		startTimes:  make(map[string]time.Time),
		latencies:   make([]time.Duration, 0),
	}
	// replay log into key-value map
	s.readCommits(commitC, errorC, reader, sizeReader)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC, reader, sizeReader)
	return s
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.kvStore[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *kvstore) Propose(k string, v string) {
	// start timer here
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.timeLock.Lock()
	s.startTimes[k] = time.Now()
	s.proposeC <- buf.String()
	s.timeLock.Unlock()
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error, reader chan string, sizeReader chan int) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == raftsnap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.timeLock.Lock()
		start, ok := s.startTimes[dataKv.Key]
		if ok {
			t := time.Since(start)
			s.latencies = append(s.latencies, t)
		}
		s.timeLock.Unlock()
		s.mu.Lock()
		s.kvStore[dataKv.Key] = dataKv.Val
		reader <- dataKv.Key
		sizeReader <- len(s.kvStore)
		s.mu.Unlock()
		// end timer here
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	s.kvStore = store
	s.mu.Unlock()
	return nil
}
