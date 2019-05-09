## Package overview

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector, Ballerina can act as Kafka Consumers and Kafka Producers.
This connector supports kafka 1.x.x and 2.0.0 versions.

## Samples
### Simple Kafka Consumer

Following is a simple service which is subscribed to a topic 'test-kafka-topic' on remote Kafka broker cluster.

```ballerina
import wso2/kafka;
import ballerina/io;

kafka:ConsumerConfig consumerConfigs = {
    bootstrapServers:"localhost:9092",
    groupId:"group-id",
    topics:["test-kafka-topic"],
    pollingInterval:1000
};

kafka:Listener consumer = new(consumerConfigs);

service kafkaService on consumer {

    resource function onMessage(kafka:ConsumerAction consumerAction, kafka:ConsumerRecord[] records) {
        // Dispatched set of Kafka records to service, We process each one by one.
        foreach var kafkaRecord in records {
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

kafka:ProducerConfig producerConfigs = {
    // Here we create a producer configs with optional parameters client.id - used for broker side logging.
    // acks - number of acknowledgments for request complete,
    // noRetries - number of retries if record send fails.
    bootstrapServers: "localhost:9092",
    clientID:"basic-producer",
    acks:"all",
    noRetries:3
};

kafka:Producer kafkaProducer = new(producerConfigs);

function main (string... args) {
    string msg = "Hello World, Ballerina";
    byte[] serializedMsg = msg.toByteArray("UTF-8");
    kafkaProducer->send(serializedMsg, "test-kafka-topic");
}
````
