
#
# Building the adapter
# will need the right manifest.json file
#
FROM engine-toolkit:base AS builder
FROM jrottenberg/ffmpeg:4.1-alpine
ARG MANIFEST_JSON=manifest.json
ARG ENGINE_ID

RUN apk update && \
    apk add -U ca-certificates gcc musl-dev python3 --no-cache && \
    pip3 install streamlink && \
        apk del gcc musl-dev --no-cache && \
        rm -rf /tmp/* &&  \
    rm -rf /var/lib/apt/lists

RUN mkdir -p /app/adapter
WORKDIR /app

COPY --from=builder /app/engine /app/
ADD ${MANIFEST_JSON} /var/manifest.json
#ADD manifest-tv-and-radio-adaptor.json /app
#ADD manifest-webstream-adapter.json /app
#RUN cp /app/${MANIFEST_JSON} /var/manifest.json

ENV ENGINE_ID=${ENGINE_ID}

ENTRYPOINT ["/app/engine"]
