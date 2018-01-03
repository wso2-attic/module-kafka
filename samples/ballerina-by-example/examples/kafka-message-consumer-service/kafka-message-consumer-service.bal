import ballerina.net.kafka;

@Description{value : "Service level annotation to provide Kafka consumer configuration."}
@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9093",
    groupId: "group-id",
    topics: ["test-topic"],
    pollingInterval: 1000
}
service<kafka> kafkaService {
    resource onMessage (kafka:KafkaConsumer consumer, kafka:ConsumerRecord[] records) {
       // Dispatched set of Kafka records to service, We process each one by one.
       int counter = 0;
       while (counter < lengthof records ) {
             processKafkaRecord(records[counter]);
             counter = counter + 1;
       }
    }
}

function processKafkaRecord(kafka:ConsumerRecord record) {
    blob serializedMsg = record.value;
    string msg = serializedMsg.toString("UTF-8");
    // Print the retrieved Kafka record.
    println("Topic: " + record.topic + " Received Message: " + msg);
}