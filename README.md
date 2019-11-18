![Engine Toolkit logo](minisite/static/engine-toolkit-logo.png)

Welcome to the Veritone Engine Toolkit, head over to the [project homepage](https://docs.veritone.com/#/developer/engines/toolkit/) to 
get started.

## Releasing

To release a new version, modify the `engine/Makefile` and increase the version number in the `buildrelease` task (use full semver style).

Then run:

```bash
make buildrelease
```

The `engine/dist` folder will contain the release.

Head over to the [releases page on GitHub](https://github.com/veritone/engine-toolkit/releases) and
click *Draft a new release*.

Enter the **same** version as the release title and tag (use full semver style).

Upload the `.tar.gz` file from `engine/dist` as the binary.

Add some notes about what's changed.
