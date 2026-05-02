# Verification

## Audited Commit

The evidence below was collected against:

```text
58f38ae427510225450b113c3655e302985b3086
```

This cleanup commit adds the verification record and a README link to it.

## Commands That Passed Locally

The local machine did not have Go on `PATH`, so validation used an official portable Go 1.22.12 toolchain extracted under `C:\tmp`.

```bash
go mod tidy
gofmt -w .
go build ./...
go test ./...
```

Additional evidence:

```bash
go test ./test -run TestExampleInputMatchesExpectedOutput -count=1 -v
```

This test runs the real in-process master/worker pipeline against `examples/large.txt`, combines reducer part files, sorts the output, and compares it to `examples/expected-output.txt`.

## Commands That Could Not Be Run Locally

```bash
go test -race ./...
```

Blocked locally on Windows because Go race tests require cgo and the host has no C compiler:

```text
cgo: C compiler "gcc" not found
```

```bash
docker compose up --build
```

Blocked locally because Docker Desktop could not start its daemon in this environment. Earlier compose syntax validation succeeded, but the refreshed shell no longer had a working Docker CLI on `PATH`.

```bash
make demo
```

Not run locally because `make` is not installed in the refreshed Windows shell. The target delegates to the passing command:

```bash
go test ./test -run TestExampleInputMatchesExpectedOutput -v
```

## GitHub Actions Status

GitHub reported no workflow runs or commit statuses for the initial pushed commit when checked through the GitHub connector:

```text
workflow_runs: []
statuses: []
```

The repository now includes `.github/workflows/ci.yml`. On push or pull request to `main`, it validates:

- `go mod tidy`
- `gofmt` cleanliness
- `go build ./...`
- `go test ./...`
- `go test -race ./...`
- `docker compose config`

## Reproduce The Demo

### Fast Verification

```bash
make demo
```

This runs `TestExampleInputMatchesExpectedOutput`, which exercises the MapReduce pipeline and verifies the sample output.

### Manual Local Cluster

Terminal 1:

```bash
go run ./cmd/master -addr=:50051 -storage-root=data
```

Terminals 2-4:

```bash
go run ./cmd/worker -master=localhost:50051 -id=worker-1 -storage-root=data
go run ./cmd/worker -master=localhost:50051 -id=worker-2 -storage-root=data
go run ./cmd/worker -master=localhost:50051 -id=worker-3 -storage-root=data
```

Submit the job:

```bash
go run ./cmd/submit -master=localhost:50051 -input=examples/large.txt -output=data/output -reducers=3 -chunk-size=128
```

Inspect output:

```bash
cat data/output/part-*.txt | sort
cat examples/expected-output.txt
```

### Docker Cluster

```bash
docker compose up --build
```

Wait until the master logs `job completed`, then inspect output:

```bash
docker compose exec master sh -c "cat /data/output/part-*.txt | sort"
```
