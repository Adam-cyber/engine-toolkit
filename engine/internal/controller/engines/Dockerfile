
# Template for adapter, engines that based on ffmpeg image
# this is to simplify the common image
#
FROM engine-toolkit:base as builder
FROM si-adapter:base

ARG gitbranch
ARG githash
ARG release
ARG builddate
ARG MANIFEST_JSON=manifest.json
ARG ENGINE_ID
ARG ENGINE_NAME

ENV ENGINE_ID=${ENGINE_ID}
ENV ENGINE_NAME=${ENGINE_NAME}
ENV RELEASE_NOTES="etversion=${release};builddate=${builddate};branch=${gitbranch};commit=${githash}"
LABEL RELEASE_NOTES=${RELEASE_NOTES}

ADD ${MANIFEST_JSON} /var/manifest.json
RUN sed -i "s#RELEASE_NOTES#$RELEASE_NOTES#g" /var/manifest.json

RUN mkdir -p /app/data
WORKDIR /app

COPY --from=builder /app/engine /app/

ENTRYPOINT ["/app/engine"]



