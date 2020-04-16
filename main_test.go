package main

import (
	"fmt"
	"math/rand"
	"testing"

	"./common"
	"github.com/mediocregopher/radix"
)

var (
	address   = []string{"127.0.0.1:6379"}
	transport = "tcp"
)

func GetKey(conn *radix.Pool, key string) string {
	val := ""
	if err := conn.Do(radix.Cmd(&val, "GET", key)); err != nil {
		panic(fmt.Errorf("Could not fetch the value for key: %s from redis", key))
	}

	return val
}

func RemoveKey(conn *radix.Pool, key string) string {
	val := ""
	if err := conn.Do(radix.Cmd(&val, "DEL", key)); err != nil {
		panic(fmt.Errorf("Could not delete the key: %s from redis", key))
	}

	return val
}

func GetHKey(conn *radix.Pool, hashID string, keyInHash string) string {
	val := ""
	if err := conn.Do(radix.Cmd(&val, "HGET", hashID, keyInHash)); err != nil {
		panic(fmt.Errorf("Could not fetch the value for hash: %s and key: %s from redis", hashID, keyInHash))
	}

	return val
}

func RemoveHKey(conn *radix.Pool, hashID string, keyInHash string) string {
	val := ""
	if err := conn.Do(radix.Cmd(&val, "HDEL", hashID, keyInHash)); err != nil {
		panic(fmt.Errorf("Could not fetch the value for hash: %s and key: %s from redis", hashID, keyInHash))
	}

	return val
}

func BenchmarkINCRKeyInSerializedSystem(b *testing.B) {
	conn, err := common.ConnectToSingleNode(transport, address[0], 10)
	if err != nil {
		b.Fatalf("Connection error occured. More info: %v", err)
	}

	userID := "manjinder" + string(rand.Int31())
	key := userID + "_complaints"
	RemoveKey(conn, key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter := 0
		if err := conn.Do(radix.Cmd(&counter, "INCR", key)); err != nil {
			b.Fatalf("Failed to execute the query. more info: %v", err)
		}
	}
	b.StopTimer()

	b.Logf("Final value of the counter: %s", GetKey(conn, key))
	RemoveKey(conn, key)
}

func BenchmarkINCRKeyInParallelSystem(b *testing.B) {
	conn, err := common.ConnectToSingleNode(transport, address[0], 1)

	if err != nil {
		b.Fatalf("Connection error occured. More info: %v", err)
	}

	userID := "manjinder" + string(rand.Int31()) // so that we can run this test in parallel
	key := userID + "_complaints"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// increment in a separated go routine
			counter := 0
			if err := conn.Do(radix.Cmd(&counter, "INCR", key)); err != nil {
				b.Fatalf("Failed to execute the query. more info: %v", err)
			}
		}
	})
	b.StopTimer()

	b.Logf("Final value of the counter: %s", GetKey(conn, key))
	RemoveKey(conn, key)
}

func BenchmarkINCRBYKeyInSerializedSystem(b *testing.B) {
	conn, err := common.ConnectToSingleNode(transport, address[0], 10)
	if err != nil {
		b.Fatalf("Connection error occured. More info: %v", err)
	}

	userID := "manjinder" + string(rand.Int31())
	key := userID + "_complaints"
	RemoveKey(conn, key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter := 0
		incrementBy := "1"
		if err := conn.Do(radix.Cmd(&counter, "INCRBY", key, incrementBy)); err != nil {
			b.Fatalf("Failed to execute the query. more info: %v", err)
		}
	}
	b.StopTimer()

	b.Logf("Final value of the counter: %s", GetKey(conn, key))
	RemoveKey(conn, key)
}

func BenchmarkINCRBYKeyInParallelSystem(b *testing.B) {
	conn, err := common.ConnectToSingleNode(transport, address[0], 1)

	if err != nil {
		b.Fatalf("Connection error occured. More info: %v", err)
	}

	userID := "manjinder" + string(rand.Int31()) // so that we can run this test in parallel
	key := userID + "_complaints"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// increment in a separated go routine
			counter := 0
			incrementBy := "1"
			if err := conn.Do(radix.Cmd(&counter, "INCRBY", key, incrementBy)); err != nil {
				b.Fatalf("Failed to execute the query. more info: %v", err)
			}
		}
	})
	b.StopTimer()

	b.Logf("Final value of the counter: %s", GetKey(conn, key))
	RemoveKey(conn, key)
}

func BenchmarkINCRHashInSerializedSystem(b *testing.B) {
	conn, err := common.ConnectToSingleNode(transport, address[0], 10)
	if err != nil {
		b.Fatalf("Connection error occured. More info: %v", err)
	}

	hashID := "manjinder"
	keyInHash := "complaints"
	RemoveHKey(conn, hashID, keyInHash)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter := 0
		incrementBy := "1" // need to write it as a string
		if err := conn.Do(radix.Cmd(&counter, "HINCRBY", hashID, keyInHash, incrementBy)); err != nil {
			b.Fatalf("Failed to execute the query. more info: %v", err)
		}
	}
	b.StopTimer()

	b.Logf("Final value of the counter: %s", GetHKey(conn, hashID, keyInHash))
	RemoveHKey(conn, hashID, keyInHash)
}

func BenchmarkINCRHashInParallelSystem(b *testing.B) {
	conn, err := common.ConnectToSingleNode(transport, address[0], 10)
	if err != nil {
		b.Fatalf("Connection error occured. More info: %v", err)
	}

	hashID := "manjinder"
	keyInHash := "complaints"
	RemoveHKey(conn, hashID, keyInHash)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter := 0
			incrementBy := "1" // need to write it as a string
			if err := conn.Do(radix.Cmd(&counter, "HINCRBY", hashID, keyInHash, incrementBy)); err != nil {
				b.Fatalf("Failed to execute the query. more info: %v", err)
			}
		}
	})
	b.StopTimer()

	b.Logf("Final value of the counter: %s", GetHKey(conn, hashID, keyInHash))
	RemoveHKey(conn, hashID, keyInHash)
}

// // BenchmarkINCRBY() on simple key

// Benchmark() increment multiple keys in pipeline
