githash = $(shell git rev-parse --short HEAD)
ldflags = -X main.BuildTag=$(githash)

.PHONY: clean test buildrelease release

release: test clean buildrelease

buildrelease:
	rm -rf dist
	mkdir dist
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o release/bin/engine
	cp -a release dist/engine-toolkit-sdk-v0.1.16
	cd dist && tar cvvzf engine-toolkit-sdk-v0.1.16.tar.gz engine-toolkit-sdk-v0.1.16

clean:
	rm -rf dist
	rm -f release/bin/engine

test:
	GO111MODULE=on go test -v ./...
