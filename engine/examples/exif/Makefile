githash = $(shell git rev-parse --short HEAD)
ldflags = -X main.BuildTag=$(githash)

build: clean buildengine builddocker

buildengine:
	GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o ./dist/engine ../../
	GOOS=linux GOARCH=amd64 go build -o ./dist/exif-engine

builddocker:
	docker build -f Dockerfile -t exif-engine .

clean:
	rm -rf ./dist