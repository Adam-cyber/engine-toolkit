package ingest

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
)

type Progress struct {
	sync.Mutex
	bytesWritten int64
	duration     time.Duration
	totalAssets  int
	assetCounts  map[string]int
}

func (pr *Progress) addBytesWritten(n int64) {
	pr.Lock()
	defer pr.Unlock()
	pr.bytesWritten += n
}

func (pr *Progress) addDuration(dur time.Duration) {
	pr.Lock()
	defer pr.Unlock()
	pr.duration += dur
}

func (pr *Progress) addAsset(assetType string) {
	pr.Lock()
	defer pr.Unlock()

	if pr.assetCounts == nil {
		pr.assetCounts = make(map[string]int)
	}
	if _, ok := pr.assetCounts[assetType]; ok {
		pr.assetCounts[assetType]++
	} else {
		pr.assetCounts[assetType] = 1
	}

	pr.totalAssets++
}

func (pr *Progress) BytesWritten() int64 {
	pr.Lock()
	defer pr.Unlock()
	return pr.bytesWritten
}

func (pr *Progress) Duration() time.Duration {
	pr.Lock()
	defer pr.Unlock()
	return pr.duration
}

func (pr *Progress) AssetCounts() map[string]int {
	pr.Lock()
	defer pr.Unlock()
	return pr.assetCounts
}

func (pr *Progress) Write(p []byte) (n int, err error) {
	pr.Lock()
	defer pr.Unlock()
	n = len(p)
	pr.bytesWritten += int64(n)
	return n, nil
}

func (pr *Progress) String() string {
	pr.Lock()
	defer pr.Unlock()

	written := datasize.ByteSize(pr.bytesWritten) * datasize.B
	assetsJSON, _ := json.Marshal(pr.assetCounts)

	return fmt.Sprintf("-- %s written | %s ingested | assets generated: %s",
		written.HR(), pr.duration.String(), assetsJSON)
}
