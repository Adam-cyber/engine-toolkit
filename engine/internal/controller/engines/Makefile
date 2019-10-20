githash = $(shell git rev-parse HEAD)
release = v0.1.16
gitbranch = $(shell git rev-parse --abbrev-ref HEAD)
builddate = $(shell date -u '+%Y-%m-%d_%H:%M:%S')

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

build:
	go build -o samples

