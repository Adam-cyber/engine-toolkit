githash = $(shell git rev-parse HEAD)
release = v0.1.16
gitbranch = $(shell git rev-parse --abbrev-ref HEAD)
builddate = $(shell date -u '+%Y-%m-%d_%H:%M:%S')

ldflags = -X main.EngineToolkitVersion=$(release) -X main.BuildTime=$(builddate) -X main.BuildTag=$(githash)

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
	GO111MODULE=auto GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	#GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	cp -a release dist/engine-toolkit-sdk-$(release)
	cd dist && tar cvvzf engine-toolkit-sdk-$(release).tar.gz engine-toolkit-sdk-$(release)

clean:
	rm -rf dist
	rm -f release/bin/engine

test:
	GO111MODULE=on go test -v ./...


# ------------------------ BUILDING DOCKER IMAGES FOR BASE ADAPTERS, ENGINES -------------------------------------------
#
# 1) Run `make build-engine-base-docker` once to get a base Docker image that has ffmpeg, mime-support and streamlink
# 2) `make build-base-docker`  : to build the engine toolkit binary in an alpine Docker image
# 3) `make build-xxx` : build individual Docker image for the adapters, SIv2-related engines
#
# ----------------------------------------------------------------------------------------------------------------------

alpine:
	CGO_ENABLED=0 GO111MODULE=auto GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine

BASE_IMAGE = engine-toolkit:base
BASE_ENGINE_IMAGE = si-adapter:base

WSA_IMAGE = webstream-adapter
TVR_IMAGE = tv-and-radio-adapter
SIV2_PLAYBACK_IMAGE = siv2-playback
SIV2_FFMPEG_IMAGE = siv2-ffmpeg
SIV2_ASSET_CREATOR_IMAGE = siv2-asset-creator

build-all-engines: build-engine-base-docker build-base-docker build-webstream-adapter-docker build-tv-and-radio-adapter-docker build-siv2-playback build-siv2-asset-creator build-siv2-ffmpeg

## set up a base Docker with ffmpeg, mime-support, streamlink
# Must do this at least once.
build-engine-base-docker:
	cd internal/controller/engines/base; docker build -t $(BASE_ENGINE_IMAGE) .

## build a base image for engine-toolkit
build-base-docker:
	docker build -t $(BASE_IMAGE) \
            --build-arg GITHUB_ACCESS_TOKEN=$(GITHUB_ACCESS_TOKEN) \
            --build-arg githash=$(githash) .

build-webstream-adapter-docker: # build-base-docker
	cd internal/controller/engines; docker build -t $(WSA_IMAGE) --build-arg MANIFEST_JSON=manifest-webstream-adapter.json \
            --build-arg ENGINE_ID="9e611ad7-2d3b-48f6-a51b-0a1ba40feab4" \
            --build-arg ENGINE_NAME="Webstream adapter" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
           .

build-tv-and-radio-adapter-docker: #build-base-docker
	cd internal/controller/engines; docker build -t $(TVR_IMAGE) \
            --build-arg MANIFEST_JSON=manifest-tv-and-radio-adapter.json \
            --build-arg ENGINE_ID="74dfd76b-472a-48f0-8395-c7e01dd7fd24" \
            --build-arg ENGINE_NAME="TV & Radio adapter" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

build-siv2-playback: #build-base-docker
	cd internal/controller/engines; docker build -t $(SIV2_PLAYBACK_IMAGE) \
            --build-arg MANIFEST_JSON=manifest-siv2-playback.json \
            --build-arg ENGINE_ID="352556c7-de07-4d55-b33f-74b1cf237f25" \
            --build-arg ENGINE_NAME="SIv2 Playback" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

build-siv2-asset-creator: #build-base-docker
	cd internal/controller/engines; docker build -t $(SIV2_ASSET_CREATOR_IMAGE) \
            --build-arg MANIFEST_JSON=manifest-siv2-asset-creator.json \
            --build-arg ENGINE_ID="75fc943b-b5b0-4fe1-bcb6-9a7e1884257a" \
            --build-arg ENGINE_NAME="SIv2 Asset Creator" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

build-siv2-ffmpeg: #build-base-docker
	cd internal/controller/engines; docker build -t $(SIV2_FFMPEG_IMAGE) \
            --build-arg MANIFEST_JSON=manifest-siv2-ffmpeg.json \
            --build-arg ENGINE_ID="8bdb0e3b-ff28-4f6e-a3ba-887bd06e6440" \
            --build-arg ENGINE_NAME="SIv2 FFMPEG" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .


## -- running and testing
start-kafka:
	docker-compose up -d

stop-kafka:
	docker-compose down


CURPWD:=$(shell pwd)
ENVFILE:=controller-env.txt

run-tv:
	docker run -it --network host --rm --entrypoint=sh --env-file $(ENVFILE) \
		-v ${CURPWD}:/src \
		-v /tmp/cache:/cache \
		-v /tmp:/tmp $(TVR_IMAGE)

run-webstream:
	docker run -it --network host --rm --entrypoint=sh --env-file $(ENVFILE) \
		-v ${CURPWD}:/src \
		-v /tmp/cache:/cache \
		-v /tmp:/tmp  $(WSA_IMAGE)

run-siv2-playback:
	docker run -it --network host --rm --entrypoint=sh --env-file $(ENVFILE) \
		-v ${CURPWD}:/src \
		-v /tmp/cache:/cache \
		-v /tmp:/tmp $(SIV2_PLAYBACK_IMAGE)



## use this to make a new engine executable without having to build docker again
# Then in the target container such as tv, etc. just do
#   cp /src/release/bin/engine /app
#
run-base:
	docker run -it --rm -v /tmp:/tmp -v ${CURPWD}:/go/src/github.com/veritone/engine-toolkit/engine $(BASE_IMAGE)

