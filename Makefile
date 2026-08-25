.PHONY: build vet test test-unit test-integration lint tidy

# The integration suite is the only suite: this package is a database driver,
# and every claim it makes is a claim about a real mongod. It starts a
# throwaway container through dockertest, or uses MONGO_URI if it is set.
test: test-integration

build:
	go build ./...

vet:
	go vet ./...
	go vet -tags=integration ./...

# Compiles the integration suite without a server, which is what CI runs when
# Docker is unavailable.
test-unit:
	go test -short -count=1 ./...

test-integration:
	go test -tags=integration -count=1 -timeout 900s ./...

lint: vet
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run ./...; \
	else echo "golangci-lint not installed; ran go vet only"; fi

tidy:
	go mod tidy
