# `engine` tool

This tool is bundled into engines and becomes the entry point. It is more fully explained in [the documentation](https://docs.veritone.com/#/developer/engines/toolkit/).

## Development

### Kafka integration test

Start Kafka with:

```
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
```

Run the tests with:

```
KAFKATEST=true GO111MODULE=on go test -v ./...
```

### Installing developer dependencies

* Install Java with `brew cask install java`
* Install Kafka with `brew install kafka`

## Resources

* https://steel-ventures.atlassian.net/wiki/spaces/VT/pages/522453767/Message+Topics+Formats+and+Schema
* https://steel-ventures.atlassian.net/wiki/spaces/VT/pages/718700686/Edge+Best+Practices+Common+Troubleshootings

