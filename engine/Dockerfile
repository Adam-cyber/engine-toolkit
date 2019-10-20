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

ENV GITHUB_ACCESS_TOKEN=${GITHUB_ACCESS_TOKEN}
RUN echo ${GITHUB_ACCESS_TOKEN} 
RUN apk update && apk add -U build-base git curl libstdc++ ca-certificates
RUN git config --global url."https://${GITHUB_ACCESS_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/"
RUN mkdir /app 
ENV CGO_ENABLED=0
RUN GOPATH=/go githash=${githash} make -e deps
RUN GOPATH=/go githash=${githash} make -e alpine && cp release/bin/engine /app/


