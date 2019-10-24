# Self driving engines

| WARNING: This is experimental |
| --- |

## Running engines in self-driving mode

- `VERITONE_SELFDRIVING=true` to switch on self-driving from filesystem
- Volume dir for input, mapped internally to `/files/in`
- Volume dir for completed, mapped internally to `/files/out/completed`
- Volume dir for errors, mapped internally to `/files/out/errors`
- Volume dir for results, mapped internally to `/files/out/results`
- Add additional environemnt variables (below) to configure the behaviour of the engine

Example:

```
docker run \
	-v $(current_dir)/testdata/fs-engine-in:/files/in \
	-v $(current_dir)/testdata/fs-engine-completed:/files/out/completed \
	-v $(current_dir)/testdata/fs-engine-errors:/files/out/errors \
	-v $(current_dir)/testdata/fs-engine-results:/files/out/results \
	-e "VERITONE_SELFDRIVING=true" \
	-e "VERITONE_SELFDRIVING_INPUTPATTERN=*.jpg|*.png|*.gif" \
	-e "VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN=yyyy/mm/dd" \
	-t exif-extraction-engine --name exif-extraction-engine
```

## Environment variables

- `VERITONE_SELFDRIVING=true` (default false) enables self-driving from filesytem
- `VERITONE_SELFDRIVING_WAITREADYFILES=false` waits to process the file until a `.ready` file exists
- `VERITONE_SELFDRIVING_INPUTPATTERN=*.jpg|*.png` (defaults to empty and process all the files) gob pattern to filter the input files 
- `VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN=yyyy/mm/dd` (defaults to empty) pattern for output folders, supports some time tokens (such as "yyyy" for year)
- `VERITONE_SELFDRIVING_POLLINTERVAL=5m` (defaults 1m) duration to wait between polling intervals to check for new files to processs
- `VERITONE_SELFDRIVING_MINIMUM_MODIFIED_DURATION=2m` (defaults 1m) duration to wait after a file is last modified before the file will be a cadidate for processing

## Payload

To provide a payload to a self-driving engine, put the a file called `payload.json` inside the input folder.

A typical `payload.json` file might look like this:

```javascript
{
	"applicationId": "applicationId",
	"recordingId": "recordingId",
	"jobId": "jobId",
	"taskId": "taskId",
	"token": "token",
	"mode": "mode",
	"libraryId": "libraryId",
	"libraryEngineModelId": "libraryEngineModelId",
	"veritoneApiBaseUrl": "https://api.veritone.com"
}
```

* Some values will be directly available in the HTTP request made to the Process webhook, for others you will need to unmarshal the `payload` field (which will contain the contents of the `payload.json` file from the input folder).

## More information

Basic flow of Self-Driving:

- Walk a directory collecting all files matching pattern
- Ignores any that are "locked" (not `.ready`, or `.processing`)
- Ignore any files following the input pattern
- Selects file at random and locks the file
- Process a file through the engine (each file is a chunk)
- Save output file (`.json`) to results folder (into time buckets) and set it as `.ready`
- Move the file to complete dir, unlock the file, and set it as `.ready`

In case of error:

- Move file to error dir (so prevents to be processed again)
- Writes a `.error` file with the cause
