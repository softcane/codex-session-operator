.PHONY: bootstrap verify

export GOTMPDIR := $(CURDIR)/.tmp/go

bootstrap:
	mkdir -p "$(GOTMPDIR)"
	go mod download

verify:
	mkdir -p "$(GOTMPDIR)"
	go test -race -count=1 ./...
	go vet ./...
	go build ./...
