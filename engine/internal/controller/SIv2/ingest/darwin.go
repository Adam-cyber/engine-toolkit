// +build darwin

package ingest

func init() {
	// use inotify for Mac builds instead of polling
	tailConfig.Poll = false
}
