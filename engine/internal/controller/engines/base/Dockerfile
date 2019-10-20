
## common Docker image to be used by adapter / stream ingestors
## this is actually can be a long process, and thus built once reuse many
FROM jrottenberg/ffmpeg:4.1-ubuntu
RUN apt-get update \
    && apt-get install -y ca-certificates python3 mime-support streamlink \
    && rm -rf /var/lib/apt/lists
RUN mkdir -p /app/data
WORKDIR /app



