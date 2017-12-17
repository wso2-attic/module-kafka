import ballerina.net.kafka;

function funcTestKafkaProduce() {
    string msg = "Hello World";
    blob byteMsg = kafka:serialize(msg);
    kafka:ProducerRecord record = {};
    record.value = byteMsg;
    record.topic = "test";
    kafkaProduce(record);
    msg = "Hello World 2";
    byteMsg = kafka:serialize(msg);
    kafka:ProducerRecord recordNext = {};
    recordNext.value = byteMsg;
    recordNext.topic = "test";
    kafkaProduce(recordNext);
}

function kafkaProduce(kafka:ProducerRecord record) {
    endpoint<kafka:KafkaProducerConnector> kafkaEP {
        create kafka:KafkaProducerConnector (getConnectorConfig());
    }
    kafkaEP.sendAdvanced(record);
    kafkaEP.flush();
    kafkaEP.close();
}

function getConnectorConfig () (kafka:KafkaProducerConf) {
    kafka:KafkaProducerConf conf = {};
    map m = {"bootstrap.servers":"localhost:9094"};
    conf.properties = m;
    return conf;
}