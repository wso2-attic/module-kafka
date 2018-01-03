import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World";
    blob serializedMsg = msg.toBlob("UTF-8");
    kafkaSimpleProduce(serializedMsg, "test-topic");
}

function kafkaSimpleProduce(blob msg, string topic) {
    endpoint<kafka:KafkaProducerClient> kafkaEP {
        create kafka:KafkaProducerClient (getConnectorConfig());
    }
    kafkaEP.send(msg, topic);
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