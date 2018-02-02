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
    resource onMessage (kafka:Consumer consumer, kafka:ConsumerRecord[] records) {
       // Dispatched set of Kafka records to service.
       int counter = 0;
       while (counter < lengthof records ) {
             // Here we process retrieved records.
             processKafkaRecord(records[counter]);
             counter = counter + 1;
       }

       string msg = "Hello World Advanced Transaction";
       blob serializedMsg = msg.toBlob("UTF-8");

       // We create ProducerRecord which consist of advanced optional parameters.
       // Here we set valid partition number which will be used when sending the record.
       // First record is sent to partition 0 on test-topic.
       kafka:ProducerRecord recordOne = {value:serializedMsg, topic:"test-output", partition:0};
       // Second record is sent to partition 1 on test-topic.
       // Specifying invalid partition could simulate transaction to be failed.
       kafka:ProducerRecord recordTwo = {value:serializedMsg, topic:"test-output", partition:1};

       // Here we create a producer configs with optional parameter transactional.id - enable transactional message production.
       kafka:ProducerConfig producerConfig = { transactionalID:"test-transactional-id"};

       kafkaTransactionalCTP(recordOne, recordTwo, consumer, producerConfig);

       // Please note we have omitted calling consumer.commit() ( enable.auto.commit = false ) now this is handled inside the
       // transaction block as these offsets are committed part of transaction.

    }
}

function kafkaTransactionalCTP(kafka:ProducerRecord recordOne, kafka:ProducerRecord recordTwo, kafka:Consumer consumer, kafka:ProducerConfig producerConfig) {
    endpoint<kafka:ProducerClient> kafkaEP {
        create kafka:ProducerClient (["localhost:9092, localhost:9093"], producerConfig);
    }
    // Here we do several produces and consumer commit atomically.
    boolean transactionSuccess = false;
    transaction {
        kafkaEP.sendAdvanced(recordOne);
        kafkaEP.sendAdvanced(recordTwo);
        // Commits consumer offsets as part of current transaction. These offsets will be considered consumed only if
        // the transaction is committed successfully. ( These offsets will be sent to offsets topic )
        kafkaEP.commitConsumer(consumer);
        transactionSuccess = true;
    } failed {
        println("Transaction failed");
    }

    if (transactionSuccess) {
        println("Transaction committed");
        kafkaEP.close();
    }
}

function processKafkaRecord(kafka:ConsumerRecord record) {
    blob serializedMsg = record.value;
    string msg = serializedMsg.toString("UTF-8");
    // Print the retrieved Kafka record.
    println("Topic: " + record.topic + " Received Message: " + msg);
}