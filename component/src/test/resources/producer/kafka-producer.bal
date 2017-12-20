import ballerina.net.kafka;

function funcTestKafkaProduce() {
    string msg = "Hello World";
    blob byteMsg = msg.toBlob("UTF-8");
    kafka:ProducerRecord record = {};
    record.value = byteMsg;
    record.topic = "test";
    kafkaProduce(record);
    msg = "Hello World 2";
    byteMsg = msg.toBlob("UTF-8");
    kafka:ProducerRecord recordNext = {};
    recordNext.value = byteMsg;
    recordNext.topic = "test";
    kafkaProduce(recordNext);
}

function kafkaProduce(kafka:ProducerRecord record) {
    endpoint<kafka:KafkaProducerClient> kafkaEP {
        create kafka:KafkaProducerClient (getConnectorConfig());
    }
    kafkaEP.sendAdvanced(record);
    kafkaEP.flush();
    kafkaEP.close();
}

function getConnectorConfig () (kafka:KafkaProducer) {
    kafka:KafkaProducer prod = {};
    map m = {"bootstrap.servers":"localhost:9094"};
    prod.properties = m;
    return prod;
}