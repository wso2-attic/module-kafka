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
