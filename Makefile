.PHONY: test bench build run demo crash-short crash-long property

test:
	go test ./...

build:
	mkdir -p bin
	go build -o bin/lsmstore ./cmd/lsmstore
	go build -o bin/lsmctl ./cmd/lsmctl
	go build -o bin/lsmbench ./cmd/lsmbench

run: build
	./bin/lsmstore -addr :8080 -dir ./data

bench: build
	./bin/lsmbench -n 50000 -value 64 -workers 1
	./bin/lsmbench -n 20000 -value 64 -workers 1 -sync

crash-short:
	LSMSTORE_CRASH_RUNS=20 go test ./internal/lsm/ -run TestCrashKill9 -count=1 -timeout 5m -v

crash-long:
	LSMSTORE_CRASH_RUNS=220 go test ./internal/lsm/ -run TestCrashKill9 -count=1 -timeout 45m -v

property:
	go test ./internal/lsm/ -run TestModelProperty -count=1 -timeout 15m -v -args -rapid.checks=200 -rapid.steps=40
