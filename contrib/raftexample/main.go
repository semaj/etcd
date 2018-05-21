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
	"flag"
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	appendIndex := flag.Int("appendIndex", 1, "append index")
	wait := *flag.String("wait", "10ms", "10ms, 50ms, 1s, etc")
	join := flag.Bool("join", false, "join an existing cluster")
	limit := *flag.Int("limit", 1000, "number of appends")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	reader := make(chan string, 100000)
	sizeReader := make(chan int, 100000)
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC, reader, sizeReader)

	time.Sleep(10000 * time.Millisecond)
	increment := 0
	canAppend := true
	start := time.Now()
	if *id <= *appendIndex {
		duration, err := time.ParseDuration(wait)
		if err != nil {
			os.Exit(1)
		}
		go func() {
			for {
				sNewValue := <-reader
				newValue, _ := strconv.Atoi(sNewValue)
				increment = newValue + 1
				canAppend = true
			}
		}()
		appendTicker := time.NewTicker(duration)
		go func() {
			for range appendTicker.C {
				if increment >= limit {
					break
				}
				if canAppend {
					sIncrement := strconv.Itoa(increment)
					kvs.Propose(sIncrement, sIncrement)
					canAppend = false
				}
			}
		}()
	}
	for {
		size := <-sizeReader
		if size >= limit {
			elapsed := time.Since(start).Seconds()
			fmt.Print("BLAST:")
			fmt.Print(float64(size) / elapsed)
			if *id <= *appendIndex {
				for _, latency := range kvs.latencies {
					fmt.Print(",", latency.Seconds())
				}
			}
			fmt.Println("")
			time.Sleep(10000 * time.Millisecond)
			os.Exit(0)
		}
	}
	// the key-value http handler will propose updates to raft
}
