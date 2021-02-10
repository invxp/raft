package raft

import "sync"

type Storage interface {
	Set(key string, value []byte)
	Get(key string) ([]byte, bool)
	Len() int
}

type MapStorage struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewMapStorage() *MapStorage {
	m := make(map[string][]byte)
	return &MapStorage{
		m: m,
	}
}

func (ms *MapStorage) Get(key string) ([]byte, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	v, found := ms.m[key]
	return v, found
}

func (ms *MapStorage) Set(key string, value []byte) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.m[key] = value
}

func (ms *MapStorage) Len() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.m)
}
