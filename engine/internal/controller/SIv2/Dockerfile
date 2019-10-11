FROM golang:1.12.5-alpine3.9 AS builder

ARG GITHUB_ACCESS_TOKEN
ADD . /go/src/github.com/veritone/edge-stream-ingestor

RUN apk update && \
    apk add -U build-base git curl mailcap ffmpeg && \
    cd /go/src/github.com/veritone/edge-stream-ingestor && \
    git config --global url."https://${GITHUB_ACCESS_TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/" && \
    go env && go list all | grep cover && \
    GOPATH=/go make

FROM jrottenberg/ffmpeg:4.1-ubuntu

ARG CONFIG_FILE=./config.json
ARG DEFAULT_PAYLOAD_FILE=./sample_ingestor_payload.json
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION=us-east-1

RUN apt-get update \
    && apt-get install -y ca-certificates mime-support \
    && rm -rf /var/lib/apt/lists

RUN mkdir -p /app/data
WORKDIR /app

COPY --from=builder /go/src/github.com/veritone/edge-stream-ingestor/edge-stream-ingestor ./edge-stream-ingestor
ADD ${DEFAULT_PAYLOAD_FILE} payload.json
ADD ${CONFIG_FILE} config.json
COPY --from=builder /go/src/github.com/veritone/edge-stream-ingestor/manifest.json.generated /var/manifest.json

ENV CONFIG_FILE="/app/config.json"
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION

ENTRYPOINT ["/app/edge-stream-ingestor"]
