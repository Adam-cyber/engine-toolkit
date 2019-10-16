FROM engine-toolkit:base as builder

FROM jrottenberg/ffmpeg:4.1-ubuntu

ARG CONFIG_FILE=./config.json
ARG DEFAULT_PAYLOAD_FILE=./sample_ingestor_payload.json

ARG MANIFEST_JSON=manifest.json
ARG ENGINE_ID=352556c7-de07-4d55-b33f-74b1cf237f25
ARG gitbranch
ARG githash
ARG release
ARG builddate

RUN apt-get update \
    && apt-get install -y ca-certificates mime-support \
    && rm -rf /var/lib/apt/lists

RUN mkdir -p /app/data
WORKDIR /app

COPY --from=builder /app/engine /app/
ENV RELEASE_NOTES="etversion=${release};builddate=${builddate};branch=${gitbranch};commit=${githash}"
ADD ${MANIFEST_JSON} /var/manifest.json
RUN sed -i "s#RELEASE_NOTES#$RELEASE_NOTES#g" /var/manifest.json

ENV ENGINE_ID=${ENGINE_ID}
ENTRYPOINT ["/app/engine"]
