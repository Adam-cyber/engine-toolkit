githash = $(shell git rev-parse --short HEAD)
ldflags = -X main.BuildTag=$(githash)

build: clean buildengine builddocker
run: down build up

buildengine:
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o ./dist/engine ../../
	GOOS=linux GOARCH=amd64 go build -o ./dist/http-proxy-example

builddocker:
	docker build -f Dockerfile -t http-proxy-example .

up:
	docker run -e "VERITONE_TESTMODE=true" -p 9090:9090 -p 8080:8080  --name http-proxy-example -t http-proxy-example 

down:
	docker rm -f http-proxy-example 2>/dev/null || true


clean:
	rm -rf ./dist
