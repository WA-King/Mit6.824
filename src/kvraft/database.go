package kvraft

import (
    "sync"
)

type datebase struct {
    data  map[string]string
    mu    sync.Mutex
}

func (db *datebase) Get(key string) (string, bool) {
    value, ok := db.data[key]
    return value, ok
}

func (db *datebase) Put(key string, value string) {
    db.data[key] = value
}

func (db *datebase) Append(key string, value string) {
    if _, ok := db.data[key]; ok {
        db.data[key] += value
    } else {
        db.data[key]= value
    }
}

func (db *datebase) Init() {
    db.data = make(map[string]string)
    
}
