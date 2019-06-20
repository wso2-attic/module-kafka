## Overview

This guide is to help you to test the Ballerina Kafka connector examples.

## Compatibility
This connector supports kafka 1.x.x and 2.0.0 versions.

## Configuration Steps

### Installing the Kafka connector on Ballerina:
Download the [Ballerina Kafka Connector](https://github.com/wso2-ballerina/package-kafka/releases) from the releases.
And extract `wso2-kafka-<version>.zip`. Run the install.{sh/bat} script to install the package.

Now download the Kafka from the [official site](https://kafka.apache.org/downloads).
Then extract the content and do the following steps.

### Start the ZooKeeper server:
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

### Start the Kafka server:
bin/kafka-server-start.sh -daemon config/server.properties

### Create a Topic:
bin/kafka-topics.sh --create --topic test-kafka-topic --zookeeper localhost:2181 --replication-factor 1 --partitions 2

## Testing the samples
Now clone or download the source from this [repository](https://github.com/wso2-ballerina/module-kafka)
 and navigate to module-kafka/samples/ballerina-by-example/examples directory.
There are some samples in this location and each has a .sh file with the command. You can test the sample by invoking the command.

E.g:
Navigate to module-kafka/samples/ballerina-by-example/examples/kafka_message_consumer_simple/ directory and execute the following command.
```
ballerina run kafka_message_consumer_simple.bal
````
