## Package overview

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector, Ballerina can act as Kafka Consumers and Kafka Producers.
This connector supports both 1.0.0 and 1.1.0 kafka versions.

## Samples
### Simple Kafka Consumer

Following is a simple service which is subscribed to a topic 'test-kafka-topic' on remote Kafka broker cluster.

```ballerina
import wso2/kafka;
import ballerina/io;

endpoint kafka:SimpleConsumer consumer {
    bootstrapServers:"localhost:9092",
    groupId:"group-id",
    topics:["test-kafka-topic"],
    pollingInterval:1000
};

service<kafka:Consumer> kafkaService bind consumer {

    onMessage(kafka:ConsumerAction consumerAction, kafka:ConsumerRecord[] records) {
        // Dispatched set of Kafka records to service, We process each one by one.
        foreach kafkaRecord in records {
            processKafkaRecord(kafkaRecord);
        }
    }
}

function processKafkaRecord(kafka:ConsumerRecord kafkaRecord) {
    byte[] serializedMsg = kafkaRecord.value;
    string msg = serializedMsg.toString("UTF-8");
    // Print the retrieved Kafka record.
    io:println("Topic: " + kafkaRecord.topic + " Received Message: " + msg);
}
````

### Simple Kafka Producer

Following is a simple program which publishes a message to 'test-kafka-topic' topic in a remote Kafka broker cluster.

```ballerina
import wso2/kafka;

endpoint kafka:SimpleProducer kafkaProducer {
    // Here we create a producer configs with optional parameters client.id - used for broker side logging.
    // acks - number of acknowledgments for request complete,
    // noRetries - number of retries if record send fails.
    bootstrapServers: "localhost:9092",
    clientID:"basic-producer",
    acks:"all",
    noRetries:3
};

function main (string... args) {
    string msg = "Hello World, Ballerina";
    byte[] serializedMsg = msg.toByteArray("UTF-8");
    kafkaProducer->send(serializedMsg, "test-kafka-topic");
}
````
