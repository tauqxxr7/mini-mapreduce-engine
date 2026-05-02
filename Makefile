.PHONY: tidy fmt build test race docker demo run-master run-worker submit

tidy:
	go mod tidy

fmt:
	gofmt -w .

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

docker:
	docker compose config
	docker compose up --build

demo:
	go test ./test -run TestExampleInputMatchesExpectedOutput -v

run-master:
	go run ./cmd/master -addr=:50051 -storage-root=data

run-worker:
	go run ./cmd/worker -master=localhost:50051 -storage-root=data

submit:
	go run ./cmd/submit -master=localhost:50051 -input=examples/large.txt -output=data/output -reducers=3 -chunk-size=128
