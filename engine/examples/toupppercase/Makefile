githash = $(shell git rev-parse --short HEAD)
ldflags = -X main.BuildTag=$(githash)
current_dir = $(shell pwd)

build: clean buildengine builddocker
run: down buildengine builddocker up
fsrun: down buildengine builddocker fsup
runtestmode: down buildengine builddocker uptestmode

buildengine:
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build -ldflags "${ldflags}" -o ./dist/engine ../../
	GOOS=linux GOARCH=amd64 go build -o ./dist/touppercase-engine

builddocker:
	docker build -f Dockerfile -t touppercase-engine .

up:
	docker run -t touppercase-engine --name touppercase-engine

fsup:
	docker run \
		-v $(current_dir)/testdata/fs-engine-in:/files/in \
		-v $(current_dir)/testdata/fs-engine-completed:/files/out/completed \
		-v $(current_dir)/testdata/fs-engine-errors:/files/out/errors \
		-v $(current_dir)/testdata/fs-engine-results:/files/out/results \
		-e "VERITONE_SELFDRIVING=true" \
		-e "VERITONE_SELFDRIVING_INPUTPATTERN=*.txt" \
		-e "VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN=yyyy/mm/dd" \
		-e "VERITONE_SELFDRIVING_MINIMUM_MODIFIED_DURATION=1s" \
		-t touppercase-engine --name touppercase-engine

down:
	docker rm -f touppercase-engine 2>/dev/null || true

uptestmode:
	docker run -e "VERITONE_TESTMODE=true" -p 9090:9090 -p 8080:8080 --name touppercase-engine -t touppercase-engine 

clean:
	rm -rf ./dist

push:
	docker tag touppercase-engine docker.veritone.com/15170/touppercase-engine:beta
	docker push docker.veritone.com/15170/touppercase-engine:beta
