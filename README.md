# Ballerina Kafka Connector

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector Ballerina can act as Kafka Consumers and Kafka Producers.

Steps to configure,
1. Extract `ballerina-kafka-connector-<version>.zip` and copy containing jars in to `<BRE_HOME>/bre/lib/`
`

Ballerina as a Kafka Consumer

```ballerina
import ballerina.net.kafka;

function main (string[] args) {
    endpoint<kafka:KafkaConsumerConnector> kafkaEP {
        create kafka:KafkaConsumerConnector (getConnectorConfig());
    }

    string[] topics = [];
    topics[0] = "test";
    var e = kafkaEP.subscribe(topics);
    while(true) {
        kafka:ConsumerRecord[] records;
        error err;
        records, err = kafkaEP.poll(1000);
        if (records != null) {
           int counter = 0;
           while (counter < lengthof records ) {
               blob byteMsg = records[counter].value;
               string msg = kafka:deserialize(byteMsg);
               println(msg);
               counter = counter + 1;
           }
        }
    }
}

function getConnectorConfig () (kafka:KafkaConsumerConf) {
    kafka:KafkaConsumerConf conf = {};
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093","group.id": "abc"};
    conf.properties = m;
    return conf;
}
````
Message consuming is coupled with message processing. ( Ordering semantics are preserved - single thread is used to
deliver messages from each polling cycle. Once processing of records from a single polling cycle is done from
a resource dispatch next polling cycle records will be dispatched. This is similar to single thread semantics
of Kafka )

```ballerina
import ballerina.net.kafka;

@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "abc",
    topics: ["test"],
    pollingTimeout: 1000,
    decoupleProcessing: false
}
service<kafka> kafkaService {
    resource onMessage (kafka:ConsumerRecord[] records, kafka:KafkaConsumer consumer) {
       int counter = 0;
       while (counter < lengthof records ) {
             blob byteMsg = records[counter].value;
             string msg = kafka:deserialize(byteMsg);
             println("Topic: " + records[counter].topic + " Received Message: " + msg);
             counter = counter + 1;
       }
    }
}
````

Message consuming is DE-coupled with message processing. ( Ordering semantics are NOT preserved - order
which the messages are processed are not guaranteed since those are handled in separate threads. Offset management
 is hard since it is hard to achieve synchronization among processing threads.)

```ballerina
import ballerina.net.kafka;

@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "abc",
    topics: ["test"],
    pollingTimeout: 1000,
    decoupleProcessing: false
}
service<kafka> kafkaService {
    resource onMessage (kafka:ConsumerRecord[] records, kafka:KafkaConsumer consumer) {
       int counter = 0;
       while (counter < lengthof records ) {
             blob byteMsg = records[counter].value;
             string msg = kafka:deserialize(byteMsg);
             println("Topic: " + records[counter].topic + " Received Message: " + msg);
             counter = counter + 1;
       }
    }
}
````
 
Ballerina as a Kafka Producer

```ballerina
import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World";
    blob byteMsg = kafka:serialize(msg);
    kafka:ProducerRecord record = {};
    record.value = byteMsg;
    record.topic = "test";
    kafkaProduce(record);
}

function kafkaProduce(kafka:ProducerRecord record) {
    endpoint<kafka:KafkaProducerConnector> kafkaEP {
        create kafka:KafkaProducerConnector (getConnectorConfig());
    }
    var e = kafkaEP.send(record);
}

function getConnectorConfig () (kafka:KafkaProducerConf) {
    kafka:KafkaProducerConf conf = {};
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093"};
    conf.properties = m;
    return conf;
}
````


For more Kafka Connector Ballerina configurations please refer to the samples directory.
