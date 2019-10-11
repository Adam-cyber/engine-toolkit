edge-stream-ingestor
=============

## Requirements
Requires the following on your local machine to build and run locally:

- Go 1.9.2+
- make
- ffmpeg 4.0+ (`brew install ffmpeg` on Mac OS X)

Or, to build and run as a Docker container, all you need is:
- make
- Docker

## Building and Running
Get the repo:
```
go get github.com/veritone/edge-stream-ingestor
cd $GOPATH/src/github.com/veritone/edge-stream-ingestor
```

To build for linux, run:
```
make
```

To build for osx, run:
```
make osx
```

Obtain a Veritone user token and set it as an environment variable. Also, set your Kafka broker addresses:
```
export VERITONE_API_TOKEN=XXXXXXXXXXXXXXXXXXXXXXXXX
export KAFKA_BROKERS=kafka1:9092,kafka2:9092
```
Note: If your provided payload file contains the `token` field, edge-stream-ingestor will use that token instead. This will be the case when running actual tasks in production.

Then, to run the engine:
```
./edge-stream-ingestor -payload payload_file.json
```

Alternatively, you can specify the payload contents directly using the `PAYLOAD_JSON` environment variable. Example:
```
PAYLOAD_JSON="{\"jobId\": \"job1\", \"taskId\": \"task1\", ...}" ./edge-stream-ingestor
```

The provided config file, `config.json`, will be used by default. To use a custom configuration, create a new JSON config file and specify it using the `-config` flag:
```
./edge-stream-ingestor -config my_config.json -payload payload.json
```

Or, set an environment variable:
```
export CONFIG_FILE=my_config.json
./edge-stream-ingestor -payload payload.json
```

The following environment variables are available and will override any settings in the configuration file:
- ENGINE_ID: ID of this engine
- ENGINE_INSTANCE_ID: a unique uuid representing the current instance (included with each heartbeat)
- KAFKA_BROKERS: comma-separated list of kafka broker address (host:port)
- KAFKA_CHUNK_TOPIC: topic name for outgoing media chunk messages (default: chunk_all)
- KAFKA_HEARTBEAT_TOPIC: topic name for engine hearbeat messages (default: engine_status)
- VERITONE_API_BASE_URL: env-specific base URL for Veritone GraphQL API
- VERITONE_API_TOKEN: must be a Veritone session token
- CHUNK_CACHE_BUCKET: S3 bucket name to cache temporary objects into
- CHUNK_CACHE_AWS_REGION: region of object cache bucket
- STREAM_INPUT_TOPIC: input topic, partition and prefix to read the stream from, using the convention `topic:partition:prefix` (ex: `stream_1:0:1234abcd__`)
- STREAM_OUTPUT_TOPIC: output topic, partition and prefix to write the stream to, using the convention `topic:partition:prefix` (ex: `stream_1:1:1234abcd__`)

## Docker
To build the Docker container image, use:
```
make build-docker
```

Set the `CONFIG_FILE` and `DEFAULT_PAYLOAD_FILE` environment variables with the command above if you would like to build the image with a different default config or payload file.

To run a Kafka/Zookeeper container, execute:
```
docker network create local
docker run -d --net local -p 2181:2181 -p 9092:9092 --rm --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 --name kafka --hostname kafka spotify/kafka
```

Then run the container:
```
export VERITONE_API_TOKEN=xxxxxxxxxxxxxxxxxxx
export STREAM_INPUT_TOPIC=xxxxxxxxxxxxxxxxxxx
docker run -t --rm --env VERITONE_API_TOKEN --env STREAM_INPUT_TOPIC --net local stream-ingestor -payload payload.json
```

This will run stream-ingestor using the default payload file included in the container. To run it using a custom payload, set the `PAYLOAD_JSON` environment variable:
```
export PAYLOAD_JSON=xxxxxxxxxxxxxxxxxxx
docker run -t --rm --env VERITONE_API_TOKEN --env STREAM_INPUT_TOPIC --env PAYLOAD_JSON --net local stream-ingestor
```

Or use docker-compose to launch both edge-sgtream-ingestor and kafka containers:
```
export VERITONE_API_TOKEN=xxxxxxxxxxxxxxxxxxx
export STREAM_INPUT_TOPIC=xxxxxxxxxxxxxxxxxxx
docker-compose up
```


## Payload Fields
| Field | Type | Default | Purpose |
|-------|------|---------|:--------|
| `generateMediaAssets` | Boolean | `false` | Set to `true` to configure Stream Ingestor to generate new media assets on the TDO |
| `outputChunkDuration` | String | `5m` | Duration of generated audio/video chunks. Example: `1m`, `15m`. Set to `0s` to disable output of audio/video chunks |
| `chunkOverlapDuration` | String | `0s` | Duration of overlap on generated audio/video chunks. Example: `15s`, `1m`. |
| `produceAudioChunks` | Boolean | `true` | Set to `false` to disable output of audio chunks |
| `produceVideoChunks` | Boolean | `true` | Set to `false` to disable output of video chunks |
| `extractFramesPerSec` | Number (Float) | `0.0` | Number of frames per second to extract (example: `1.0` for 1 frame per second). For video streams only. |
| `saveRawAsset` | Boolean | `false` | Set to `true` to configure Stream Ingestor to save the raw stream contents (i.e. original file) as a primary asset on the TDO. |
| `outputRawAsset` | Boolean | `false` | Set to `true` to configure Stream Ingestor to generate an output chunk from the raw stream contents. |

## Modes
Stream Ingestor can operate in one or more different modes.
- ingestor: capture a stream (from `STREAM_INPUT_TOPIC`, a URL, or stdin) and create media assets.
  - set the `generateMediaAssets` field in the task payload to `true` to enable this.
- chunk producer: capture a stream (from `STREAM_INPUT_TOPIC`, a URL, or stdin) and create chunk messages for each frame or chunk of audio or video.
  - Use the `extractFramesPerSec` to specify the number of frames per second to extract
  - Use `outputChunkDuration` to specify the duration of audio/video chunks (example: `15s` for 15 second chunks). All video chunks are in MP4 format, while all audio chunks are in WAV format.
  - If the input stream is an image, the settings above have no effect.
- transcode (filter): transcode a stream from the input format to a specific format and write it to the output stream (must use `STREAM_OUTPUT_TOPIC`, stdout, or output file). Include a `filter` block in your task payload with the `format` field specifying a specific FFMPEG format type. The `format` field can specify additional FFMPEG options/flags using semicolons as the delimiter. See below as an example:

```
"filter": {
  "format": "s16le;ac=1;ar=16000"
}
```

### Diagram
![Stream Ingestor](https://drive.google.com/uc?export=view&id=1LCg7nb4T7rTug_IQpIo2yFyQrXMduhMg)


## Tips
To generate a fresh TDO, you can use the `-create-tdo` flag:
```
docker run -t --rm --env VERITONE_API_TOKEN --env PAYLOAD_JSON --env STREAM_INPUT_TOPIC --net local stream-ingestor --create-tdo
```

You can also provide the `id` of any existing TDO using the `-tdo` flag. This will override any `recordingId` in the payload.
```
docker run -t --rm --env VERITONE_API_TOKEN --env PAYLOAD_JSON --env STREAM_INPUT_TOPIC --net local stream-ingestor -tdo 400002001
```

Stream Ingestor can also accept streamed input via stdin by setting the `-stdin` flag. (Use the `-i` flag with `docker run`)
```
docker run -i --rm --env VERITONE_API_TOKEN --env PAYLOAD_JSON --env STREAM_INPUT_TOPIC --net local stream-ingestor -stdin < test.mp4
```

Occasionally, the format of certain file types cannot be automatically determined and a MIME-type or FFMPEG format hint must be provided. Use the `-input-mimetype` or `-input-format` flag to do so:
```
docker run -i --rm --env VERITONE_API_TOKEN --env PAYLOAD_JSON --env STREAM_INPUT_TOPIC --net local stream-ingestor -stdin -input-mimetype video/mp4 < test.mp4
```

Sometimes it is helpful to dump the stream output to a file or stdout. This is especially helpful when debugging transcoded output or streaming it into another engine's stdin. Use the option `-o {filename}` to output the stream to a file or `-stdout` to stream it to stdout.


## Object Cache

Media chunk caching currently requires an S3 bucket, specified in the config, and credentials, which are provided using environment variables. Credentials can be provided either by setting `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION`, environment variables at Docker build time, or by providing these variables as run time `env` variables. Example:

```
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
make build-docker
```

Or,
```
export VERITONE_API_TOKEN=xxxxxxxxxxxxxxxxxxx
export PAYLOAD_JSON=xxxxxxxxxxxxxxxxxxx
export AWS_ACCESS_KEY_ID=xxxxxxxxxxxxxxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
docker run --rm \
	--env VERITONE_API_TOKEN \
	--env PAYLOAD_JSON \
	--env STREAM_INPUT_TOPIC \
	--env AWS_ACCESS_KEY_ID \
	--env AWS_SECRET_ACCESS_KEY \
	--net local -it edge-stream-ingestor
```
