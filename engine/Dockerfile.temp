#
# Plain and sample - just build the base image with engine toolkit at /app/engine
# this can then be copied into the target final image - e.g. adapter will put its own image
# requiremet, similarly
#
# For other engines, they'll need to make sure that their final Docker image
# has the minimimum package as required by both SI and adapter.
#
# So TODO list the dependency..
#
#
# Building the 
FROM golang:1.12.10-alpine AS builder
ARG GITHUB_ACCESS_TOKEN
ARG githash

ADD . /go/src/github.com/veritone/engine-toolkit/engine
WORKDIR /go/src/github.com/veritone/engine-toolkit/engine

## temporary block for now
#TEMP ENV GITHUB_ACCESS_TOKEN=${GITHUB_ACCESS_TOKEN}
#TEMP RUN echo ${GITHUB_ACCESS_TOKEN} 
#TEMP RUN apk update && apk add -U build-base git curl libstdc++ ca-certificates
#TEMP RUN git config --global url."https://${GITHUB_ACCESS_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"
#TEMP RUN mkdir /app 
#TEMP ENV CGO_ENABLED=0

#TEMP RUN GOPATH=/go githash=${githash} make -e deps
#TEMP RUN GOPATH=/go githash=${githash} make -e -f Makefile.docker alpine && cp release/bin/engine /app/

# TEMPORARY HERE -- do a build outside then copy into this
COPY engine /app/
