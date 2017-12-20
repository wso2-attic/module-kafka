import ballerina.task;
import ballerina.math;
import ballerina.net.kafka;

kafka:KafkaConsumer consumer;

function main (string[] args) {
    // Here we initializes a consumer which connects to remote cluster.
    consumer  = getConsumer();
    var conError = consumer.connect();

    // We subscribes the consumer to topic test-topic
    string[] topics = ["test-topic"];
    var subErr = consumer.subscribe(topics);

    // Consumer poll() function will be called every time the timer goes off.
    function () returns (error) onTriggerFunction = poll;

    // Consumer pollError() error function will be called if an error occurs while consumer poll the topics.
    function (error e) onErrorFunction = pollError;

    // Schedule a timer task which initially starts poll cycle in 500ms from now and there
    //onwards runs every 2000ms.
    var taskId, schedulerError = task:scheduleTimer(onTriggerFunction,
                                         onErrorFunction, {delay:500, interval:2000});
    if (schedulerError != null) {
        println("Kafka poll cycle scheduling failed: " + schedulerError.msg) ;
    } else {
        println("Kafka Poll Task ID:" + taskId);
    }
}

function poll() returns (error e) {
    kafka:ConsumerRecord[] records;
    error err;
    records, err = consumer.poll(1000);
    if (records != null) {
        int counter = 0;
        while (counter < lengthof records ) {
            processKafkaRecord(records[counter]);
            counter = counter + 1;
        }
    } else {
        // We return the error which stop scheduled timer tasks.
        return err;
    }
    consumer.commit();
    return;
}

function processKafkaRecord(kafka:ConsumerRecord record) {
    blob serializedMsg = record.value;
    string msg = serializedMsg.toString("UTF-8");
    // Print the retrieved Kafka record.
    println("Topic: " + record.topic + " Received Message: " + msg);
}

function pollError(error e) {
    // Exception occurred while polling the Kafka consumer. Here we close close consumer and log error.
    var closeError = consumer.close();
    print("[ERROR] Consumer poll failed");
    println(e);
}

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer consumer = {};
    // Configuration for the Kafka Consumer as key / value pairs.
    // We enable manual offset commit with "enable.auto.commit": false
    // Since we are interested in old message once the consumer is connected this is enabled with "auto.offset.reset":"earliest"
    // Ballerina internally registers byte key / value de-serializers so those are avoided from setting programmatically
    map m = {"bootstrap.servers":"localhost:9092, localhost:9093","group.id": "group-id","enable.auto.commit": false, "auto.offset.reset":"earliest"};
    consumer.properties = m;
    return consumer;
}