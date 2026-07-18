CACHE_DIR := $(CURDIR)/.cache
GOENV := GOTOOLCHAIN=local GOCACHE=$(CACHE_DIR)/go-build GOPATH=$(CACHE_DIR)/gopath
LINTENV := $(GOENV) GOLANGCI_LINT_CACHE=$(CACHE_DIR)/golangci-lint
GO_FILES := $(shell find . -type f -name '*.go' -not -path './.git/*' -not -path './.cache/*' -not -path './tmp/*')
GOVULNCHECK_VERSION := v1.6.0

.PHONY: verify test test-race test-integration-race vet lint vulncheck examples fmt-check tidy-check

verify: tidy-check fmt-check vet test test-race lint examples

test:
	$(GOENV) go test ./...

test-race:
	$(GOENV) go test -race ./...

test-integration-race:
	$(GOENV) go test -tags=integration -race ./...

vet:
	$(GOENV) go vet ./...

lint:
	$(LINTENV) golangci-lint run --build-tags integration ./...

vulncheck:
	$(GOENV) go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

examples:
	mkdir -p $(CURDIR)/tmp/examples
	$(GOENV) go build -o $(CURDIR)/tmp/examples/ ./examples/...

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))" || { gofmt -l $(GO_FILES); exit 1; }

tidy-check:
	$(GOENV) go mod tidy -diff
