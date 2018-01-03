import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World Advanced";
    blob serializedMsg = msg.toBlob("UTF-8");
    // We create ProducerRecord which consist of advanced optional parameters.
    // Here we set valid partition number which will be used when sending the record.
    kafka:ProducerRecord record = {value:serializedMsg, topic:"test-topic", partition:1};
    kafkaAdvancedProduce(record);
}

function kafkaAdvancedProduce(kafka:ProducerRecord record) {
    endpoint<kafka:KafkaProducerClient> kafkaEP {
        create kafka:KafkaProducerClient (getConnectorConfig());
    }
    kafkaEP.sendAdvanced(record);
    kafkaEP.flush();
}

function getConnectorConfig () (kafka:KafkaProducer) {
    kafka:KafkaProducer producer = {};
    // Configuration for the Kafka Producer as key / value pairs.
    // bootstrap.servers specifies host/port pairs to used for establishing the initial connection to the Kafka cluster
    // Ballerina internally registers byte key / value serializers so those are avoided from setting programmatically
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093"};
    producer.properties = m;
    return producer;
}