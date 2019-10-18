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
	#GO111MODULE=on GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	cp -a release dist/engine-toolkit-sdk-$(release)
	cd dist && tar cvvzf engine-toolkit-sdk-$(release).tar.gz engine-toolkit-sdk-$(release)

clean:
	rm -rf dist
	rm -f release/bin/engine

test:
	GO111MODULE=on go test -v ./...


# ------------------- BUILDING DOCKER IMAGES FOR BASE ADAPTERS

BASE_IMAGE = engine-toolkit:base
WSA_IMAGE = webstream-adapter
TVR_IMAGE = tv-and-radio-adapter
SIV2_PLAYBACK_IMAGE = siv2-playback
SIV2_FFMPEG_IMAGE = siv2-ffmpeg
SIV2_ASSET_CREATOR_IMAGE = siv2-asset-creator

build-base-docker:
	docker build -t $(BASE_IMAGE)  --build-arg githash=$(githash) .

build-webstream-adapter-docker: # build-base-docker
	cd internal/controller/adapter; docker build -t $(WSA_IMAGE) --build-arg MANIFEST_JSON=manifest-webstream-adapter.json \
            --build-arg ENGINE_ID="9e611ad7-2d3b-48f6-a51b-0a1ba40feab4" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
           .

build-tv-and-radio-adapter-docker: #build-base-docker
	cd internal/controller/adapter; docker build -t $(TVR_IMAGE) \
            --build-arg MANIFEST_JSON=manifest-tv-and-radio-adapter.json \
            --build-arg ENGINE_ID="74dfd76b-472a-48f0-8395-c7e01dd7fd24" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

build-siv2-playback: #build-base-docker
	cd internal/controller/engines/siv2-playback; docker build -t $(SIV2_PLAYBACK_IMAGE) \
            --build-arg MANIFEST_JSON=manifest.json \
            --build-arg ENGINE_ID="352556c7-de07-4d55-b33f-74b1cf237f25" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

build-siv2-asset-creator: #build-base-docker
	cd internal/controller/engines/siv2-ffmpeg; docker build -t $(SIV2_FFMPEG_IMAGE) \
            --build-arg MANIFEST_JSON=manifest.json \
            --build-arg ENGINE_ID="75fc943b-b5b0-4fe1-bcb6-9a7e1884257a" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

build-siv2-ffmpeg: #build-base-docker
	cd internal/controller/engines/siv2-ffmpeg; docker build -t $(SIV2_FFMPEG_IMAGE) \
            --build-arg MANIFEST_JSON=manifest.json \
            --build-arg ENGINE_ID="8bdb0e3b-ff28-4f6e-a3ba-887bd06e6440" \
            --build-arg release=$(release) \
            --build-arg builddate=$(builddate) \
            --build-arg gitbranch=$(gitbranch) \
            --build-arg githash=$(githash) \
            .

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
		-v /tmp/test:/tmp/test $(TVR_IMAGE)

run-webstream:
	docker run -it --network host --rm --entrypoint=sh --env-file $(ENVFILE) \
		-v ${CURPWD}:/src \
		-v /tmp/cache:/cache \
		-v /tmp/test:/tmp/test  $(WSA_IMAGE)

run-siv2-playback:
	docker run -it --network host --rm --entrypoint=sh --env-file controller-env.txt \
		-v ${CURPWD}:/src \
		-v /tmp/cache:/cache \
		-v /tmp/test:/tmp/test $(SIV2_PLAYBACK_IMAGE)



## use this to make a new engine executable without having to build docker again
# Then in the target container such as tv, etc. just do
#   cp /src/release/bin/engine /app
#
run-base:
	docker run -it --rm -v ${CURPWD}:/go/src/github.com/veritone/engine-toolkit/engine $(BASE_IMAGE)

