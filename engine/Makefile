githash = $(shell git rev-parse HEAD)
release = v0.1.16

ldflags = -X main.EngineToolkitVersion=$(release) -X main.BuildTime=$(shell date -u '+%Y-%m-%d_%H:%M:%S') -X main.BuildTag=$(githash)

.PHONY: clean test buildrelease release

release: test clean buildrelease


deps:
	# Install or update govend
	go get -u github.com/govend/govend
	# Fetch vendored dependencies
	#rm -rf ./vendor
	govend -v

generate-deps:
	go get -u github.com/govend/govend
	# Generate vendor.yml
	govend -v -l --prune

buildrelease:
	rm -rf dist
	mkdir dist
	#GO111MODULE=on GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	cp -a release dist/engine-toolkit-sdk-$(release)
	cd dist && tar cvvzf engine-toolkit-sdk-$(release).tar.gz engine-toolkit-sdk-$(release)

clean:
	rm -rf dist
	rm -f release/bin/engine

test:
	GO111MODULE=on go test -v ./...


# ---------------- BUILDING DOCKER IMAGES FOR BASE ADAPTERS

BASE_IMAGE = engine-toolkit:base

build-base-docker:
	docker build -t $(BASE_IMAGE)  --build-arg githash=$(githash) .

build-wsa-adapter-docker: # build-base-docker
	cd internal/controller/adapter; docker build -t webstream-adapter --build-arg MANIFEST_JSON=manifest-webstream-adapter.json --build-arg ENGINE_ID="9e611ad7-2d3b-48f6-a51b-0a1ba40feab4" .

build-tv-and-radio-adapter-docker: #build-base-docker
	cd internal/controller/adapter; docker build -t tv-and-radio-adapter --build-arg MANIFEST_JSON=manifest-tv-and-radio-adapter.json --build-arg ENGINE_ID="74dfd76b-472a-48f0-8395-c7e01dd7fd24" .
