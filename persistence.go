// Raft一致性实现
// 持久化(目前比较简单)

package raft

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
)

// loadFromStorage 恢复之前的数据,通过简单的gob来做
func (r *raft) loadFromStorage() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if termData, found := r.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&r.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		if b, err := ioutil.ReadFile("term"); err == nil {
			d := gob.NewDecoder(bytes.NewBuffer(b))
			if err := d.Decode(&r.currentTerm); err != nil {
				_ = os.Remove("term")
				log.Fatal(err)
			}
		}
	}

	if votedData, found := r.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&r.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		if b, err := ioutil.ReadFile("vote"); err == nil {
			d := gob.NewDecoder(bytes.NewBuffer(b))
			if err := d.Decode(&r.votedFor); err != nil {
				_ = os.Remove("vote")
				log.Fatal(err)
			}
		}
	}

	if logData, found := r.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&r.log); err != nil {
			log.Fatal(err)
		}
	} else {
		if b, err := ioutil.ReadFile("log"); err == nil {
			d := gob.NewDecoder(bytes.NewBuffer(b))
			if err := d.Decode(&r.log); err != nil {
				_ = os.Remove("log")
				log.Fatal(err)
			}
		}
	}
}

// saveToStorage 进行持久化存储
func (r *raft) saveToStorage() {
	r.mu.Lock()
	defer r.mu.Unlock()

	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(r.currentTerm); err != nil {
		log.Fatal(err)
	}
	r.storage.Set("currentTerm", termData.Bytes())
	if err := ioutil.WriteFile("term", termData.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(r.votedFor); err != nil {
		log.Fatal(err)
	}
	r.storage.Set("votedFor", votedData.Bytes())
	if err := ioutil.WriteFile("vote", votedData.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(r.log); err != nil {
		log.Fatal(err)
	}
	r.storage.Set("log", logData.Bytes())
	if err := ioutil.WriteFile("log", logData.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}
}

// saveNodes 保存节点信息
func (r *raft) saveNodes() {
	var nodeData bytes.Buffer
	var nodes []string
	for node := range r.nodes() {
		nodes = append(nodes, node)
	}
	if err := gob.NewEncoder(&nodeData).Encode(nodes); err != nil {
		log.Fatal(err)
	}
	if err := ioutil.WriteFile("node", nodeData.Bytes(), 0644); err != nil {
		log.Fatal(err)
	}
}
