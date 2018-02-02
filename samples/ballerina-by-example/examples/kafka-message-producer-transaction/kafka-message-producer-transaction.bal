import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World Transaction";
    blob serializedMsg = msg.toBlob("UTF-8");

    // We create ProducerRecord which consist of advanced optional parameters.
    // Here we set valid partition number which will be used when sending the record.
    // First record is sent to partition 0 on test-topic.
    kafka:ProducerRecord recordOne = {value:serializedMsg, topic:"test-topic", partition:0};
    // Second record is sent to partition 1 on test-topic.
    // Specifying invalid partition could simulate transaction to be failed.
    kafka:ProducerRecord recordTwo = {value:serializedMsg, topic:"test-topic", partition:1};

    // Here we create a producer configs with optional parameter transactional.id - enable transactional message production.
    kafka:ProducerConfig producerConfig = { transactionalID:"test-transactional-id"};

    kafkaAdvancedTransactionalProduce(recordOne, recordTwo, producerConfig);
}

function kafkaAdvancedTransactionalProduce(kafka:ProducerRecord recordOne, kafka:ProducerRecord recordTwo, kafka:ProducerConfig producerConfig) {
    endpoint<kafka:ProducerClient> kafkaEP {
        create kafka:ProducerClient (["localhost:9092, localhost:9093"], producerConfig);
    }
    // Kafka transactions allows messages to be send multiple partition atomically on KafkaProducerClient. Kafka Local transactions can only be used
    // when you are sending multiple messages using the same KafkaProducerClient instance.
    boolean transactionSuccess = false;
    transaction {
        kafkaEP.sendAdvanced(recordOne);
        kafkaEP.sendAdvanced(recordTwo);
        transactionSuccess = true;
    } failed {
        println("Transaction failed");
    }

    if (transactionSuccess) {
        println("Transaction committed");
        kafkaEP.close();
    }
}