# Ballerina Kafka Connector

Ballerina Kafka Connector is used to connect Ballerina with Kafka Brokers. With the Kafka Connector Ballerina can act as Kafka Consumers and Kafka Producers.

Steps to configure,
1. Extract `ballerina-kafka-connector-<version>.zip` and copy containing jars in to `<BRE_HOME>/bre/lib/`
`

Ballerina as a Kafka Consumer

***Using Ballerina native functions.

```ballerina
import ballerina.net.kafka;

function main (string[] args) {
    kafka:KafkaConsumer con  = getConsumer();
    con.connect();

    string[] topics = [];
    topics[0] = "test";
    var e = con.subscribe(topics);
    while(true) {
        kafka:ConsumerRecord[] records;
        error err;
        records, err = con.poll(1000);
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

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer con = {};
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093","group.id": "abc"};
    con.properties = m;
    return con;
}
````
***Using Ballerina Services.

1. Message consuming is coupled with message processing. ( decoupleProcessing: false)

Ordering semantics are preserved - single thread is used to deliver messages from each polling cycle.
Once processing of records from a single polling cycle is done from a resource dispatch next polling cycle
records will be dispatched. This is similar to single thread semantics of Kafka.

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

enable.auto.commit = false case.

```ballerina
import ballerina.net.kafka;

@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "ab",
    topics: ["test"],
    pollingTimeout: 1000,
    decoupleProcessing: false,
    autoCommit: false
}
service<kafka> kafkaService {
    resource onMessage (kafka:ConsumerRecord[] records, kafka:KafkaConsumer consumer) {
       int counter = 0;
       while (counter < lengthof records ) {
             processRecord(records[counter]);
             counter = counter + 1;
       }
       consumer.commit();
    }
}

function processRecord(kafka:ConsumerRecord record) {
    blob byteMsg = record.value;
    string msg = kafka:deserialize(byteMsg);
    println("Topic: " + record.topic + " Received Message: " + msg);
}
````

2. Message consuming is DE-coupled with message processing. ( decoupleProcessing: true )

Ordering semantics are NOT preserved - order which the messages are processed are not guaranteed since those are
handled in separate threads. Offset management is hard since it is hard to achieve synchronization
 among processing threads.)

```ballerina
import ballerina.net.kafka;

@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "abc",
    topics: ["test"],
    pollingTimeout: 1000,
    decoupleProcessing: true
}
service<kafka> kafkaService {
    resource onMessage (kafka:ConsumerRecord[] record) {
        blob byteMsg = record.value;
        string msg = kafka:deserialize(byteMsg);
        println("Topic: " + record.topic + " Received Message: " + msg);
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

Kafka Producer Transactions

1. Pure produce transaction

```ballerina
import ballerina.net.kafka;

function main (string[] args) {
    string msg1 = "Hello World";
    blob byteMsg1 = kafka:serialize(msg1);
    kafka:ProducerRecord record1 = {};
    record1.value = byteMsg1;
    record1.topic = "test";

    string msg2 = "Hello World 2";
    blob byteMsg2 = kafka:serialize(msg2);
    kafka:ProducerRecord record2 = {};
    record2.value = byteMsg2;
    record2.topic = "test";

    kafkaProduce(record1, record2);

}

function kafkaProduce(kafka:ProducerRecord record1, kafka:ProducerRecord record2) {
    endpoint<kafka:KafkaProducerConnector> kafkaEP {
        create kafka:KafkaProducerConnector (getConnectorConfig());
    }
    transaction {
       kafkaEP.send(record1);
       kafkaEP.send(record2);
    }
}

function getConnectorConfig () (kafka:KafkaProducerConf) {
    kafka:KafkaProducerConf conf = {};
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093", "transactional.id":"test-transactional-id"};
    conf.properties = m;
    return conf;
}
````

2. Consume-transform-produce pattern

```ballerina
import ballerina.net.kafka;

@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "ab",
    topics: ["input"],
    pollingTimeout: 1000,
    decoupleProcessing: false,
    autoCommit: false
}
service<kafka> kafkaService {
    resource onMessage (kafka:ConsumerRecord[] records, kafka:KafkaConsumer consumer) {
       int counter = 0;
       while (counter < lengthof records ) {
             processRecord(records[counter]);
             counter = counter + 1;
       }

       kafka:TopicPartition tp = {};
       tp.topic = "input";
       tp.partition = 0;
       int p;
       p, _ = consumer.getPositionOffset(tp);

       string msg1 = "Hello World";
       blob byteMsg1 = kafka:serialize(msg1);
       kafka:ProducerRecord record1 = {};
       record1.value = byteMsg1;
       record1.topic = "output";

       string msg2 = "Hello World 2";
       blob byteMsg2 = kafka:serialize(msg2);
       kafka:ProducerRecord record2 = {};
       record2.value = byteMsg2;
       record2.topic = "output";

       kafka:Offset[] offsets = [];
       kafka:Offset consumedOffset = {};
       consumedOffset.partition = tp;
       consumedOffset.offset = p;
       offsets[0] = consumedOffset;

       kafkaProduceWithTransaction(record1, record2, offsets);

    }
}

function processRecord(kafka:ConsumerRecord record) {
    blob byteMsg = record.value;
    string msg = kafka:deserialize(byteMsg);
    println("Topic: " + record.topic + " Received Message: " + msg);
}


function kafkaProduceWithTransaction(kafka:ProducerRecord record1, kafka:ProducerRecord record2, kafka:Offset[] offsets) {
    endpoint<kafka:KafkaProducerConnector> kafkaEP {
        create kafka:KafkaProducerConnector (getConnectorConfig());
    }
    transaction {
       kafkaEP.send(record1);
       kafkaEP.send(record2);
       kafkaEP.sendOffsetsTransaction(offsets, "ab");
    }
}

function getConnectorConfig () (kafka:KafkaProducerConf) {
    kafka:KafkaProducerConf conf = {};
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093", "transactional.id":"test-transactional-id"};
    conf.properties = m;
    return conf;
}
````

Kafka Partition Rebalance Listeners

```ballerina
import ballerina.net.kafka;

function main (string[] args) {
    kafka:KafkaConsumer con  = getConsumer();
    var ce = con.connect();

    string[] topics = [];
    topics[0] = "wso2";
    function(kafka:KafkaConsumer consumer, kafka:TopicPartition[] partitions) onAssigned = printOne;
    function(kafka:KafkaConsumer consumer, kafka:TopicPartition[] partitions) onRevocked = printTwo;

    var se = con.subscribeWithPartitionRebalance(topics, onAssigned, onRevocked);
    while(true) {
        kafka:ConsumerRecord[] records;
        error err;
        records, err = con.poll(1000);
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

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer con = {};
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093","group.id": "abc"};
    con.properties = m;
    return con;
}


function printOne(kafka:KafkaConsumer consumer, kafka:TopicPartition[] partitions) {
    println("one");
}

function printTwo(kafka:KafkaConsumer consumer, kafka:TopicPartition[] partitions) {
     println("two");
}
````

For more Kafka Connector Ballerina configurations please refer to the samples directory.
