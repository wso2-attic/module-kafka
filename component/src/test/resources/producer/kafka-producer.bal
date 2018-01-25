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
    endpoint<kafka:ProducerClient> kafkaEP {
        create kafka:ProducerClient (["localhost:9094"], getProducerConfig());
    }
    kafkaEP.sendAdvanced(record);
    kafkaEP.flush();
    kafkaEP.close();
}

function getProducerConfig () (kafka:ProducerConfig) {
    kafka:ProducerConfig pc = {clientID:"basic-producer"};
    return pc;
}
