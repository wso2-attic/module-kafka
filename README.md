# **Ballerina Kafka Connector**

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector, Ballerina can act as Kafka Consumers and Kafka Producers. This connector supports both 1.0.0 and 1.1.0 kafka versions.

Steps to configure,
1. Extract `ballerina-kafka-connector-<version>.zip` and copy containing jars in to `<BRE_HOME>/bre/lib/`
`

## Ballerina as a Kafka Consumer

Here service 'kafkaService' is subscribed to topic 'test-topic' on remote Kafka broker cluster. In this example, offsets are manually committed inside the resource
by setting property autoCommit = false at service level annotation.

```ballerina
import wso2/kafka;
import ballerina/io;

endpoint kafka:SimpleConsumer consumer {
    bootstrapServers: "localhost:9092",
    groupId: "group-id",
    topics: ["test-kafka-topic"],
    pollingInterval: 1000
};

service<kafka:Consumer> kafkaService bind consumer {
    onMessage (kafka:ConsumerAction consumerAction, kafka:ConsumerRecord[] records) {
        foreach record in records {
            blob serializedMsg = record.value;
             string msg = serializedMsg.toString("UTF-8");
             io:println("Topic: " + record.topic + " Received Message: " + msg);
        }
    }
}
````

## Ballerina as a Kafka Producer

Here Kafka record is created from serialized string, and then it is published to topic 'test-topic' partition '1' in remote Kafka broker cluster.

```ballerina
import wso2/kafka;

endpoint kafka:SimpleProducer producer {
    bootstrapServers: "localhost:9092, localhost:9093",
    clientID:"basic-producer",
    acks:"all",
    noRetries:3
};

function main (string... args) {
    string msg = "Hello World, Ballerina";
    blob serializedMsg = msg.toBlob("UTF-8");
    kafkaProducer->send(serializedMsg, "test-topic");

}
````

For more Kafka Connector Ballerina configurations please refer to the samples directory.
