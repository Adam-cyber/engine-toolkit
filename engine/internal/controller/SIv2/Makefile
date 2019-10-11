# BINARY_NAME defaults to the name of the repository
BINARY_NAME := $(notdir $(shell pwd))
DOCKER_IMAGE_NAME ?= stream-ingestor
BUILD_INFO_FLAGS := -X main.BuildTime=$(shell date -u '+%Y-%m-%d_%H:%M:%S') -X main.BuildCommitHash=$(shell git rev-parse HEAD)
STATIC_BUILD_FLAGS := -linkmode external -extldflags -static -w
OSX_BUILD_FLAGS := -s
LIST_NO_VENDOR := $(shell go list ./... | grep -v /vendor/ | grep -v mocks)
GOBIN := $(GOPATH)/bin
CONFIG_FILE ?= config.json
DEFAULT_PAYLOAD_FILE ?= sample_ingestor_payload.json

.PHONY: build build-docker push-dev push-stage push-prod push-ukprod check clean fmt test

default: check fmt deps build

osx: check fmt deps build-osx

deploy: build-docker push-dev push-stage push-prod

build-osx:
	# Build project
	go build -ldflags "$(BUILD_INFO_FLAGS)" -a -o $(BINARY_NAME) .


gen-manifest:
	./gen-manifest.sh

build: gen-manifest
	# Build project
	go build -ldflags "$(BUILD_INFO_FLAGS) $(STATIC_BUILD_FLAGS)" -a -o $(BINARY_NAME) .

build-osx:
	# Build project
	go build -ldflags "$(BUILD_INFO_FLAGS) $(OSX_BUILD_FLAGS)" -a -o $(BINARY_NAME) .


build-docker:
	# Build project
	docker build -t $(DOCKER_IMAGE_NAME) \
		--build-arg GITHUB_ACCESS_TOKEN=$(GITHUB_ACCESS_TOKEN) \
		--build-arg AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
		--build-arg AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
		--build-arg AWS_DEFAULT_REGION=$(AWS_DEFAULT_REGION) \
		--build-arg CONFIG_FILE=$(CONFIG_FILE) \
		--build-arg DEFAULT_PAYLOAD_FILE=$(DEFAULT_PAYLOAD_FILE) .

push-dev:
	docker tag stream-ingestor docker.aws-dev.veritone.com/7682/stream-ingestor
	docker push docker.aws-dev.veritone.com/7682/stream-ingestor

push-stage:
	docker tag stream-ingestor docker.stage.veritone.com/7682/stream-ingestor
	docker push docker.stage.veritone.com/7682/stream-ingestor

push-prod:
	docker tag stream-ingestor docker.veritone.com/7682/stream-ingestor
	docker push docker.veritone.com/7682/stream-ingestor

push-ukprod:
	docker tag stream-ingestor docker.uk.veritone.com/1/stream-ingestor
	docker push docker.uk.veritone.com/1/stream-ingestor

check:
	# Only continue if go is installed
	go version || ( echo "Go not installed, exiting"; exit 1 )

clean:
	go clean -i
	rm -rf ./vendor
	rm -f $(BINARY_NAME)

deps:
	# Install or update govend
	go get -u github.com/govend/govend
	# Fetch vendored dependencies
	rm -rf ./vendor
	$(GOBIN)/govend -v --prune

fmt:
	# Format all Go source files (excluding vendored packages)
	go fmt $(LIST_NO_VENDOR)

generate-deps:
	# Generate vendor.yml
	$(GOBIN)/govend -v -l

test:
	# Run all tests (excluding vendored packages)
	go test -a -v -cover -timeout 30s $(LIST_NO_VENDOR) -covermode=set -coverprofile=coverage.out
