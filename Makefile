.PHONY: test bench build run demo

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
