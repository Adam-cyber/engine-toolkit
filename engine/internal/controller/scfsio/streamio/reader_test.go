package streamio

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"github.com/stretchr/testify/suite"
)

const ffprobeOutputPNG = `{
	"streams": [
		{
			"index": 0,
			"codec_name": "png",
			"codec_long_name": "PNG (Portable Network Graphics) image",
			"codec_type": "video",
			"codec_time_base": "0/1",
			"codec_tag_string": "[0][0][0][0]",
			"codec_tag": "0x0000",
			"width": 433,
			"height": 215,
			"coded_width": 433,
			"coded_height": 215,
			"has_b_frames": 0,
			"sample_aspect_ratio": "1:1",
			"display_aspect_ratio": "433:215",
			"pix_fmt": "rgba",
			"level": -99,
			"color_range": "pc",
			"refs": 1,
			"r_frame_rate": "25/1",
			"avg_frame_rate": "0/0",
			"time_base": "1/25",
			"disposition": {
				"default": 0,
				"dub": 0,
				"original": 0,
				"comment": 0,
				"lyrics": 0,
				"karaoke": 0,
				"forced": 0,
				"hearing_impaired": 0,
				"visual_impaired": 0,
				"clean_effects": 0,
				"attached_pic": 0,
				"timed_thumbnails": 0
			}
		}
	],
	"format": {
		"filename": "data/sample/image.png",
		"nb_streams": 1,
		"nb_programs": 0,
		"format_name": "png_pipe",
		"format_long_name": "piped png sequence",
		"size": "126999",
		"probe_score": 99
	}
}`

const ffprobeOutputMOV = `{
	"streams": [
		{
			"index": 0,
			"codec_name": "h264",
			"codec_long_name": "H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10",
			"profile": "Main",
			"codec_type": "video",
			"codec_time_base": "1/120",
			"codec_tag_string": "avc1",
			"codec_tag": "0x31637661",
			"width": 40,
			"height": 40,
			"coded_width": 48,
			"coded_height": 48,
			"has_b_frames": 1,
			"sample_aspect_ratio": "1:1",
			"display_aspect_ratio": "1:1",
			"pix_fmt": "yuv420p",
			"level": 10,
			"color_range": "tv",
			"color_space": "bt709",
			"color_transfer": "bt709",
			"color_primaries": "bt709",
			"chroma_location": "left",
			"refs": 1,
			"is_avc": "true",
			"nal_length_size": "4",
			"r_frame_rate": "60/1",
			"avg_frame_rate": "60/1",
			"time_base": "1/6000",
			"start_pts": 0,
			"start_time": "0.000000",
			"duration_ts": 68200,
			"duration": "11.366667",
			"bit_rate": "23712",
			"bits_per_raw_sample": "8",
			"nb_frames": "682",
			"disposition": {
				"default": 1,
				"dub": 0,
				"original": 0,
				"comment": 0,
				"lyrics": 0,
				"karaoke": 0,
				"forced": 0,
				"hearing_impaired": 0,
				"visual_impaired": 0,
				"clean_effects": 0,
				"attached_pic": 0,
				"timed_thumbnails": 0
			},
			"tags": {
				"creation_time": "2018-04-26T21:39:14.000000Z",
				"language": "und",
				"handler_name": "Core Media Video",
				"encoder": "H.264"
			}
		}
	],
	"format": {
		"filename": "data/sample/white.mov",
		"nb_streams": 1,
		"nb_programs": 0,
		"format_name": "mov,mp4,m4a,3gp,3g2,mj2",
		"format_long_name": "QuickTime / MOV",
		"start_time": "0.000000",
		"duration": "11.366667",
		"size": "44102",
		"bit_rate": "31039",
		"probe_score": 100,
		"tags": {
			"major_brand": "qt  ",
			"minor_version": "0",
			"compatible_brands": "qt  ",
			"creation_time": "2018-04-26T21:39:14.000000Z",
			"com.apple.quicktime.make": "Apple",
			"com.apple.quicktime.model": "MacBookPro14,3",
			"com.apple.quicktime.software": "Mac OS X 10.12.6 (16G1314)",
			"com.apple.quicktime.creationdate": "2018-04-26T14:38:51-0700"
		}
	}
}`

type readerTestSuite struct {
	suite.Suite
}

func (t *readerTestSuite) TestStartStreamReader() {
	runCommandFn = func(cmd *exec.Cmd) error {
		// simulate ffprobe reading part of the stream from stdin
		p := make([]byte, 2*bufferPeekSize)
		_, err := cmd.Stdin.Read(p)
		t.NoError(err)

		_, err = cmd.Stdout.Write([]byte(ffprobeOutputPNG))
		t.NoError(err)

		return nil
	}

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/image.png"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.Failf("failed to open sample file %q: %s", sampleFile, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr := NewFileReader(file, "image/png", "")
	stream, err := StartStreamReader(ctx, cancel, sr, time.Minute)
	if t.NoError(err) {
		if t.NotNil(stream) {
			t.Equal("image/png", stream.MimeType)
			t.Equal("png_pipe", stream.FfmpegFormat.String())
			t.Equal("png_pipe", stream.Container.FormatName)

			if t.Len(stream.Streams, 1) {
				t.Equal("video", stream.Streams[0].CodecType)
				t.Equal("png", stream.Streams[0].Codec)
				t.Equal(433, stream.Streams[0].Width)
				t.Equal(215, stream.Streams[0].Height)
			}

			streamBytes, err := ioutil.ReadAll(stream)
			if t.NoError(err) {
				b, err := ioutil.ReadFile(sampleFile)
				if err != nil {
					t.Failf("failed to read sample file %q: %s", sampleFile, err)
				}

				t.Equal(len(b), len(streamBytes))
				t.Equal(b, streamBytes)
			}
		}
	}
}

func (t *readerTestSuite) TestStartStreamReaderQT() {
	runCommandFn = func(cmd *exec.Cmd) error {
		// simulate ffprobe reading part of the stream from stdin
		p := make([]byte, 2*bufferPeekSize)
		_, err := cmd.Stdin.Read(p)
		t.NoError(err)

		_, err = cmd.Stdout.Write([]byte(ffprobeOutputMOV))
		t.NoError(err)

		return nil
	}

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/white.mov"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.Failf("failed to open sample file %q: %s", sampleFile, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sr := NewFileReader(file, "video/quicktime", "")
	stream, err := StartStreamReader(ctx, cancel, sr, time.Minute)
	if t.NoError(err) {
		if t.NotNil(stream) {
			t.Equal("video/quicktime", stream.MimeType)
			t.Equal("mov", stream.FfmpegFormat.String())
			t.Equal("mov,mp4,m4a,3gp,3g2,mj2", stream.Container.FormatName)

			if t.Len(stream.Streams, 1) {
				t.Equal("video", stream.Streams[0].CodecType)
				t.Equal("h264", stream.Streams[0].Codec)
				t.Equal(40, stream.Streams[0].Width)
				t.Equal(40, stream.Streams[0].Height)
			}

			streamBytes, err := ioutil.ReadAll(stream)
			if t.NoError(err) {
				b, err := ioutil.ReadFile(sampleFile)
				if err != nil {
					t.Failf("failed to read sample file %q: %s", sampleFile, err)
				}

				t.Equal(len(b), len(streamBytes))
				// bytes should have been re-arranged for QT faststart
				t.NotEqual(b, streamBytes)
				t.Equal(b[:10], streamBytes[:10])
			}
		}
	}
}

func (t *readerTestSuite) TestStartStreamReaderQT_mfra_tfra_atom() {
	runCommandFn = func(cmd *exec.Cmd) error {
		// simulate ffprobe reading part of the stream from stdin
		p := make([]byte, 2*bufferPeekSize)
		_, err := cmd.Stdin.Read(p)
		t.NoError(err)

		_, err = cmd.Stdout.Write([]byte(ffprobeOutputMOV))
		t.NoError(err)

		return nil
	}

	cwd, _ := os.Getwd()
	sampleFile := cwd + "/../data/sample/mfra.mp4"
	file, err := os.Open(sampleFile)
	if err != nil {
		t.Failf("failed to open sample file %q: %s", sampleFile, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sr := NewFileReader(file, "video/quicktime", "")
	_, err = StartStreamReader(ctx, cancel, sr, time.Minute)
	t.NoError(err)
}

func (t *readerTestSuite) testCheckWithFFProbe() {
	cwd, _ := os.Getwd()
	audio := cwd + "/../data/sample/audio.mp4"
	file, err := os.Open(audio)
	if err != nil {
		t.Fail(err.Error())
	}
	defer file.Close()

	stream := NewStreamFromReader(file)
	ctx := context.Background()
	res, err := checkWithFFProbe(ctx, stream)
	if t.NoError(err) {
		t.Equal(res.MimeType, "audio/mpeg")
	}

}
