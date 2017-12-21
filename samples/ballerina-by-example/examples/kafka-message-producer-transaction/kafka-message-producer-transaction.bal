import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World Advanced";
    blob serializedMsg = msg.toBlob("UTF-8");
    // We create ProducerRecord which consist of advanced optional parameters.
    // Here we set valid partition number which will be used when sending the record.
    // First record is sent to partition 0 on test-topic
    kafka:ProducerRecord recordOne = {value:serializedMsg, topic:"test-topic", partition:0};
    // Second record is sent to partition 1 on test-topic
    // Specifying invalid partition could simulate transaction to be failed
    kafka:ProducerRecord recordTwo = {value:serializedMsg, topic:"test-topic", partition:1};
    kafkaAdvancedTransactionalProduce(recordOne, recordTwo);
}

function kafkaAdvancedTransactionalProduce(kafka:ProducerRecord recordOne, kafka:ProducerRecord recordTwo) {
    endpoint<kafka:KafkaProducerClient> kafkaEP {
        create kafka:KafkaProducerClient (getConnectorConfig());
    }
    // Kafka transactions allows messages to be send multiple partition atomically on KafkaProducerClient. Kafka Local transactions can only be used
    // when you are sending multiple messages using the same KafkaProducerClient instance.
    transaction {
        kafkaEP.sendAdvanced(recordOne);
        kafkaEP.sendAdvanced(recordTwo);
    } failed {
        println("Rollbacked");
    } committed {
        println("Committed");
    }
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