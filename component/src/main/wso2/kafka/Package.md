## Package overview

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector, Ballerina can act as Kafka Consumers and Kafka Producers.
This connector supports both 1.0.0 and 1.1.0 kafka versions.

## Samples
### Simple Kafka Consumer

Following is a simple service which is subscribed to a topic 'test-topic' on remote Kafka broker cluster.

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
        // Dispatched set of Kafka records to service and process one by one.
        foreach record in records {
            string msg = record.value.serializedMsg.toString("UTF-8");
            io:println("Topic: " + record.topic + " Received Message: " + msg);
        }
        consumerAction.commit();
    }
}
````

### Simple Kafka Producer

Following is a simple program which publishes a message to 'test-topic' topic in a remote Kafka broker cluster.

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
    producer->send(serializedMsg, "test-topic", partition = 0);
}
````
