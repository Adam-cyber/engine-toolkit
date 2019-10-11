Performance
===========

Ingestion requires transcoding of the source media (via ffmpeg) and is an expensive operation. Resources, particularly CPU, must be allocated appropriately in order to handle real-time workloads. Performance varies significantly based on the bitrate of the incoming stream and is impacted by factors such as the presence of a video stream, video resolution, framerate, compression level, etc. Furthermore, performance can depend a lot on the underlying hardware of the host running the container, especially when comparing EC2 vs Fargate on AWS.

## Benchmark
Our benchmark test ran on seven 15-minute media files of varying bitrates. The media files were all generated from this same [YouTube video](https://www.youtube.com/watch?v=Bey4XXJAqS8).

| Type    | Format | Size | Bitrate |
| ------- |:------:| ----:|--------:|
| audio only | m4a | 14.2MB | 127 kbps |
| 360p video / 29.97 fps | mp4 | 63.2MB | 562 kbps |
| 480 video / 29.97 fps | mp4 | 69.3MB | 616 kbps |
| 720p video / 29.97 fps | mp4 | 155.2MB | 1.38 mbps |
| 1080p video / 29.97 fps | mp4 | 273.4MB | 2.43 mbps |
| 2K video / 29.97 fps | webm | 621MB | 5.8 mbps |
| 4K video / 29.97 fps | webm | 1.4GB | 13.46 mbps |

Each file was ingested multiple times on a combination of host type, CPU and memory variations. The performance of each run is measured as the time taken to complete the ingestion. The following profiles were tested:

| Type    | vCPU | Memory (MB) |
| ------- |-----:| -----------:|
| Fargate | 0.25 | 512 |
| Fargate | 0.25 | 1024 |
| Fargate | 0.5 | 1024 |
| Fargate | 1.0 | 2048 |
| Fargate | 2.0 | 4096 |
| Fargate | 4.0 | 8192 |
| EC2 (c5.4xlarge)* | 0.125 | 128 |
| EC2 (c5.4xlarge)* | 0.125 | 1024 |
| EC2 (c5.4xlarge) | 0.25 | 512 |
| EC2 (c5.4xlarge) | 0.25 | 1024 |
| EC2 (c5.4xlarge) | 0.5 | 512 |
| EC2 (c5.4xlarge) | 0.5 | 1024 |
| EC2 (c5.4xlarge) | 1.0 | 1024 |
| EC2 (c5.4xlarge) | 1.0 | 2048 |
| EC2 (c5.4xlarge) | 2.0 | 256 |
| EC2 (c5.4xlarge) | 2.0 | 512 |
| EC2 (c5.4xlarge) | 2.0 | 1024 |
| EC2 (c5.4xlarge) | 2.0 | 2048 |
| EC2 (c5.4xlarge) | 2.0 | 4096 |
| EC2 (c5.4xlarge) | 4.0 | 1024 |
| EC2 (c5.4xlarge) | 4.0 | 4096 |
| EC2 (c5.4xlarge) | 8.0 | 8192 |

`*` Only used to test audio ingestion

A sample of 10 ingestion jobs per file were executed on each resource profile. The jobs are generated using a simple GraphQL mutation containing the Webstream Adapter engine:

```
mutation createJob {
    createJob(input: {
        targetId: "..."
        tasks: [{
            engineId: "9e611ad7-2d3b-48f6-a51b-0a1ba40feab4"
            payload: {
                url: "https://s3.amazonaws.com/dev-chunk-cache/perftest/2K-30fps.webm"
            }
        }]
    }) {
        id
    }
}
```

Once complete, the Stream Ingestor task in each job contains some `info` data inside the task's `output` field similar to the following:
```
{
    "chunksSent": {
        "audio/mp4": 1
    },
    "durationIngested": "15m0.021333s",
    "ffmpegFormat": "nut",
    "ingestionTime": "14m52.410984426s",
    "mediaSegmentsWritten": 17,
    "mimeType": "",
    "startOffsetMS": 0,
    "startTime": "2019-02-07T10:45:00Z",
    "substreams": [
        {
            "index": 0,
            "codec_type": "audio",
            "codec_name": "mp3",
            "avg_frame_rate": "0/0",
            "sample_rate": "48000",
            "channels": 1
        }
    ]
}
```

The fields of interest are `durationIngested` and `ingestionTime`. We expect `durationIngested` to always equal `15m0s` for our tests (+/- some small delta). `ingestionTime` measures the duration from the time the first stream bytes are read to the time ingestion has completed. Note that this time also includes the time required to write assets to S3 and perform additional GraphQL calls, however, this time is typically insignificant compared to the transcoding time.

### Results

| Profile (Type/CPU/Mem) | Audio | 360p | 480p | 720p | 1080p | 2K | 4K |
|------------------------|------:|-----:|-----:|-----:|------:|---:|---:|
| Fargate / 0.25 / 512 | 1m15.8s | 27m55.1s | 48m47.3s | 1h38m30.0s | `x` | -- | -- |
| Fargate / 0.25 / 1024 | 1m16.0s | 27m41.1s | 48m29.4s | 1h38m33.9s | 3h1m26.8s | -- | -- |
| Fargate / 0.5 / 1024 | 39.3s | 13m26.2s | 23m18.2s | 47m6.6s | 1h28m49.3s | 2h45m32.1s | `x` |
| Fargate / 1.0 / 2048 | 22.9s | 12m7.9s | 20m30.0s | 43m50.9s | 1h23m5.1s | 2h21m44.7s | `x*` |
| Fargate / 2.0 / 4096 | 22.8s | 6m34.0s | 10m34.2s | 22m25.8s | 42m1.8s | 1h18m1.4s | 2h45m33.5s |
| Fargate / 4.0 / 8192 | 26.9s | 3m28.1s | 5m55.2s | 11m58.6s | 22m29.6s | 40m27.5s | 1h30m52.6s |
| EC2 / 0.125 / 128 | 2m38.3s | -- | -- | -- | -- | -- | -- |
| EC2 / 0.125 / 1024 | 2m38.0s | -- | -- | -- | -- | -- | -- |
| EC2 / 0.25 / 512 | 1m11.9s | 30m3.2s | 51m3.5s | `x` | `x` | -- | -- |
| EC2 / 0.25 / 1024 | 1m9.4s | 31m27.5s | 53m38.6s | 1h38m36.8s | `x` | -- | -- |
| EC2 / 0.5 / 512 | 32.0s | 14m30.5s | 26m39.4s | `x` | `x` | -- | -- |
| EC2 / 0.5 / 1024 | 32.5s | 14m52.6s | 27m33.8s | 55m24.9s | `x` | -- | -- |
| EC2 / 1.0 / 1024 | 21.8s | 7m19.4s | 12m51.6s | 27m8.9s | `x` | -- | -- |
| EC2 / 1.0 / 2048 | 22.2s | 7m32.1s | 13m9.4s | 26m59.0s | 49m25.6s | `x` | `x` |
| EC2 / 2.0 / 256 | 21.8s | 7m37.0s | `x` | `x` | `x` | -- | -- |
| EC2 / 2.0 / 512 | 26.2s | 3m37.0s | 6m36.2s | `x` | `x` | -- | -- |
| EC2 / 2.0 / 1024 | 21.7s | 3m35.3s | 6m36.7s | 13m30.8s | `x` | -- | -- |
| EC2 / 2.0 / 2048 | 21.1s | 3m36.4s | 6m30.8s | 13m50.3s | 24m33.5s | `x` | `x` |
| EC2 / 2.0 / 4096 | 21.4s | 3m43.3s | 6m26.9s | 14m0.4s | 25m35.1s | 46m59.8s | `x` |
| EC2 / 4.0 / 1024 | 30.6s | 1m40.8s | 3m14.2s | 6m40.4s | `x` | `x` | `x` |
| EC2 / 4.0 / 4096 | 22.6s | 1m58.2s | 3m23.9s | 7m4.1s | 13m17.3s | 24m24.2s | `x` |
| EC2 / 8.0 / 8192 | 24.3s | 1m6.2s | 1m54.4s | 3m44.3s | 7m0.4s | 13m23.8s | 28m7.1s |

`x` = ffmpeg process died<br/>
`x*` = ffmpeg process died in half of jobs, stream buffer data expired in other half after 3+ hours

Note: some small variations and abnormalities may occur due to external factors such as GraphQL/S3 response times.

## Summary

| Type    | Chart | Min Memory Required (EC2) | Min Memory Required (Fargate) | Min CPU For Real-Time |
| ------- |:------:| ----:|--------:|--------:|
| audio only | [View](perf/audio-only-chart.png?raw=true) | 128MB | 512MB (lowest) | 0.125 vCPU |
| 360p video | [View](perf/360p-chart.png?raw=true) | 256MB | 512MB | 0.5 vCPU |
| 480 video | [View](perf/480p-chart.png?raw=true) | 256MB | 512MB | 1.0 vCPU |
| 720p video | [View](perf/720p-chart.png?raw=true) | 512/1024MB`*` | 512MB | 2.0 vCPU |
| 1080p video | [View](perf/1080p-chart.png?raw=true) | 2048MB | 1024MB | 4.0 vCPU |
| 2K video | [View](perf/2K-chart.png?raw=true) | 4096MB | 1024MB | 8.0 vCPU |
| 4K video | [View](perf/4K-chart.png?raw=true) | 8192MB | 4096MB | x |

`*` depends on CPU

#### Audio Processing:

<img src="perf/audio-only-chart.png?raw=true" width="800" title="Audio Only Performance" />

* Audio does not require much resources to process - even 0.125 vCPU w/ 128 memory can be used to process audio in real-time.
* No difference between EC2 and Fargate performance.
* More CPU results in faster ingestion of static files, but there's no performance gain from multiple vCPUs.
* Reached it's best time (~20 seconds) with 1.0 vCPU.


**Video Processing**:

<img src="perf/360p-chart.png?raw=true" width="800" title="360p Video Performance" />

* Increasing the number of vCPUs increases performance practically linearly (exception between 0.5 and 1.0 vCPU in Fargate)
* Containers on EC2 are ~40% more performant as on Fargate in most cases. Performance was practically equal when using 0.5 vCPU.
* Memory does not impact performance, but if not given enough, ffmpeg is suddenly killed by the kernal when processing high-bitrate video. See: https://trac.ffmpeg.org/ticket/429
* Fargate can go further with less memory and is, in some cases, capable of processing high-bitrate videos that EC2 will fail on.
* At least 2GB memory is needed on EC2 to successfully ingest 1080p video. 4GB+ for 2K/4K.
* 2K and 4K video demands very high CPU (and memory) resources. EC2 with 8.0 vCPU and 8GB memory can support 2K in real-time. No configuration tested can support 4K in real-time.
