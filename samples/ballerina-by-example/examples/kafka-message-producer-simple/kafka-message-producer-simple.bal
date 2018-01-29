import ballerina.net.kafka;

function main (string[] args) {
    string msg = "Hello World";
    blob serializedMsg = msg.toBlob("UTF-8");
    // Here we create a producer configs with optional parameters client.id - used for broker side logging.
    // acks - number of acknowledgments for request complete, noRetries - number of retries if record send fails.
    kafka:ProducerConfig producerConfig = { clientID:"basic-producer", acks:"all", noRetries:3};
    kafkaSimpleProduce(serializedMsg, "test-topic", producerConfig);
}

function kafkaSimpleProduce(blob msg, string topic, kafka:ProducerConfig producerConfig) {
    endpoint<kafka:ProducerClient> kafkaEP {
        create kafka:ProducerClient (["localhost:9092, localhost:9093"], producerConfig);
    }
    kafkaEP.send(msg, topic);
    kafkaEP.flush();
    kafkaEP.close();
}