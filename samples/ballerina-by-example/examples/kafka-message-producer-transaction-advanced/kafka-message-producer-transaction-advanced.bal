import ballerina.net.kafka;

@Description{value : "Service level annotation to provide Kafka consumer configuration. Here it mandatory to make enable.auto.commit = false since these offsets are committed part of transaction"}
@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "group-id",
    topics: ["test-input"],
    pollingTimeout: 1000,
    autoCommit: false
}
service<kafka> kafkaService {
    resource onMessage (kafka:KafkaConsumer consumer, kafka:ConsumerRecord[] records) {
       // Dispatched set of Kafka records to service.
       int counter = 0;
       while (counter < lengthof records ) {
             // Here we process retrieved records.
             processKafkaRecord(records[counter]);
             counter = counter + 1;
       }

       string msg = "Hello World Transaction";
       blob serializedMsg = msg.toBlob("UTF-8");
       // We create ProducerRecord which consist of advanced optional parameters.
       // Here we set valid partition number which will be used when sending the record.
       // First record is sent to partition 0 on test-topic
       kafka:ProducerRecord recordOne = {value:serializedMsg, topic:"test-output", partition:0};
       // Second record is sent to partition 1 on test-topic
       // Specifying invalid partition could simulate transaction to be failed
       kafka:ProducerRecord recordTwo = {value:serializedMsg, topic:"test-output", partition:1};

       kafkaTransactionalCTP(recordOne, recordTwo, consumer);

       // Please not we have omitted calling consumer.commit() ( enable.auto.commit = false ) now this is handled inside the
       // transaction block as these offsets are committed part of transaction.

    }
}

function kafkaTransactionalCTP(kafka:ProducerRecord recordOne, kafka:ProducerRecord recordTwo, kafka:KafkaConsumer consumer) {
    endpoint<kafka:KafkaProducerClient> kafkaEP {
        create kafka:KafkaProducerClient (getConnectorConfig());
    }
    // Here we do several produces and consumer commit atomically.
    transaction {
        kafkaEP.sendAdvanced(recordOne);
        kafkaEP.sendAdvanced(recordTwo);
        // Commits consumer offsets as part of current transaction. These offsets will be considered consumed only if
        // the transaction is committed successfully. ( These offsets will be sent to offsets topic )
        kafkaEP.commitConsumer(consumer);
    } failed {
        println("Rollbacked");
    } committed {
        println("Committed");
    }
}

function processKafkaRecord(kafka:ConsumerRecord record) {
    blob serializedMsg = record.value;
    string msg = serializedMsg.toString("UTF-8");
    // Print the retrieved Kafka record.
    println("Topic: " + record.topic + " Received Message: " + msg);
}

function getConnectorConfig () (kafka:KafkaProducer) {
    kafka:KafkaProducer producer = {};
    // Configuration for the Kafka Producer as key / value pairs.
    // bootstrap.servers specifies host/port pairs to used for establishing the initial connection to the Kafka cluster
    // Ballerina internally registers byte key / value serializers so those are avoided from setting programmatically
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093", "transactional.id":"test-transactional-id"};
    producer.properties = m;
    return producer;
}