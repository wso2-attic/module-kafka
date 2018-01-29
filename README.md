# **Ballerina Kafka Connector**

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector, Ballerina can act as Kafka Consumers and Kafka Producers.

Steps to configure,
1. Extract `ballerina-kafka-connector-<version>.zip` and copy containing jars in to `<BRE_HOME>/bre/lib/`
`

## Ballerina as a Kafka Consumer

Here service 'kafkaService' is subscribed to topic 'test-topic' on remote Kafka broker cluster. In this example, offsets are manually committed inside the resource
by setting property autoCommit = false at service level annotation.

```ballerina
import ballerina.net.kafka;

@Description{value : "Service level annotation to provide Kafka consumer configuration. Here enable.auto.commit = false"}
@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "group-id",
    topics: ["test-topic"],
    pollingInterval: 1000,
    autoCommit: false
}
service<kafka> kafkaService {
    resource onMessage (kafka:Consumer consumer, kafka:ConsumerRecord[] records) {
       // Dispatched set of Kafka records to service, We process each one by one.
       int counter = 0;
       while (counter < lengthof records ) {
             processKafkaRecord(records[counter]);
             counter = counter + 1;
       }
       // Commit offsets returned for returned records, marking them as consumed.
       consumer.commit();
    }
}

function processKafkaRecord(kafka:ConsumerRecord record) {
    blob serializedMsg = record.value;
    string msg = serializedMsg.toString("UTF-8");
    // Print the retrieved Kafka record.
    println("Topic: " + record.topic + " Received Message: " + msg);
}
````

## Ballerina as a Kafka Producer

Here Kafka record is created from serialized string, and then it is published to topic 'test-topic' partition '1' in remote Kafka broker cluster.

```ballerina
import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World Advanced";
    blob serializedMsg = msg.toBlob("UTF-8");
    // We create ProducerRecord which consist of advanced optional parameters.
    // Here we set valid partition number which will be used when sending the record.
    kafka:ProducerRecord record = { value:serializedMsg, topic:"test-topic", partition:1 };

    // We create a producer configs with optional parameters client.id - used for broker side logging.
    // acks - number of acknowledgments for request complete, noRetries - number of retries if record send fails.
    kafka:ProducerConfig producerConfig = { clientID:"basic-producer", acks:"all", noRetries:3};
    kafkaAdvancedProduce(record, producerConfig);
}

function kafkaAdvancedProduce(kafka:ProducerRecord record, kafka:ProducerConfig producerConfig) {
    endpoint<kafka:ProducerClient> kafkaEP {
        create kafka:ProducerClient (["localhost:9092, localhost:9093"], producerConfig);
    }
    kafkaEP.sendAdvanced(record);
    kafkaEP.flush();
    kafkaEP.close();
}
````

For more Kafka Connector Ballerina configurations please refer to the samples directory.
