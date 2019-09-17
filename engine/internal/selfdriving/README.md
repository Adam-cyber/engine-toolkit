# Self driving engines

| WARNING: This is experimental, use it at your own risk! |
| --- |

Run example:

Minimal to run with docker:

- `VERITONE_SELFDRIVING=true` to tell that is running using the self-driving from filesystem
- Volume dir for input, mapped to `/files/in`
- Volume dir for completed, mapped to `/files/out/completed`
- Volume dir for errors, mapped to `/files/out/errors`
- Volume dir for results, mapped to `/files/out/results`

Example:

```
docker run \
	-v $(current_dir)/testdata/fs-engine-in:/files/in \
	-v $(current_dir)/testdata/fs-engine-completed:/files/out/completed \
	-v $(current_dir)/testdata/fs-engine-errors:/files/out/errors \
	-v $(current_dir)/testdata/fs-engine-results:/files/out/results \
	-e "VERITONE_SELFDRIVING=true" \
	-e "VERITONE_SELFDRIVING_INPUTPATTERN=*.jpg" \
	-e "VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN=yyyy/mm/dd" \
	-t exif-extraction-engine --name exif-extraction-engine
```

Env vars:

- `VERITONE_SELFDRIVING=true` (default false) enables self-driving from filesytem
- `VERITONE_SELFDRIVING_WAITREADYFILES=false` waits to process the file until a `.ready` file exists
- `VERITONE_SELFDRIVING_INPUTPATTERN=*.jpg` (defaults to empty and process all the files) gob pattern to filter the input files 
- `VERITONE_SELFDRIVING_OUTPUT_DIR_PATTERN=yyyy/mm/dd` (defaults to empty) pattern for output folders, supports some time tokens (such as "yyyy" for year)
- `VERITONE_SELFDRIVING_POLLINTERVAL=5m` (defaults 1m) duration to wait between polling intervals to check for new files to processs


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
