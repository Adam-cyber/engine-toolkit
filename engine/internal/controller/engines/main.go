
package main

import (
	"github.com/veritone/engine-toolkit/engine/internal/controller"
	"github.com/veritone/realtime/modules/engines/wsa_tvr_adapter"
	"fmt"

	"github.com/veritone/realtime/modules/engines/siv2playback"
	"github.com/veritone/realtime/modules/engines/siv2ffmpeg"
	"github.com/veritone/realtime/modules/engines/siv2core"
)

func main () {
	fmt.Println("Sample Controller config: ", controller.SampleVeritoneControllerConfig())
	fmt.Println("Sample Adapter Input: ", adapter.SampleAdapterPayload())
	fmt.Println( "Sample SIv2Playback: ", siv2playback.SampleSI2PlaybackInputs())
	fmt.Println("Sample SIv2FFMPEG: ", siv2ffmpeg.SampleSIv2FFMPEGPayload())
	fmt.Println("Sample SIv2AssetCreator:",siv2core.SampleSiv2AssetCreator())
}
