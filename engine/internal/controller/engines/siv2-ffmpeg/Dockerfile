FROM engine-toolkit:base as builder

FROM jrottenberg/ffmpeg:4.1-ubuntu

ARG CONFIG_FILE=./config.json
ARG DEFAULT_PAYLOAD_FILE=./sample_ingestor_payload.json

ARG MANIFEST_JSON=manifest.json
ARG ENGINE_ID=8bdb0e3b-ff28-4f6e-a3ba-887bd06e6440
ARG ENGINE_NAME=SIv2FFMPEG
ARG gitbranch
ARG githash
ARG release
ARG builddate

RUN apt-get update \
    && apt-get install -y ca-certificates python3 mime-support streamlink \
    && rm -rf /var/lib/apt/lists

RUN mkdir -p /app/data
WORKDIR /app

COPY --from=builder /app/engine /app/
ENV RELEASE_NOTES="ETversion=${release};builddate=${builddate};branch=${gitbranch};commit=${githash}"
ADD ${MANIFEST_JSON} /var/manifest.json
RUN sed -i "s#RELEASE_NOTES#$RELEASE_NOTES#g" /var/manifest.json

ENV ENGINE_ID=${ENGINE_ID}
ENTRYPOINT ["/app/engine"]
