import ballerina.net.kafka;

@Description{value : "Service level annotation to provide Kafka consumer configuration. Here concurrentConsumers = 2 whereas default value 1.
Half the partition from given topic test-topic will be assigned to each consumer."}
@kafka:configuration {
    bootstrapServers: "localhost:9092, localhost:9092",
    concurrentConsumers: 2,
    groupId: "group-id",
    topics: ["test-topic"],
    pollingInterval: 1000
}
service<kafka> kafkaService {
    resource onMessage (kafka:Consumer consumer, kafka:ConsumerRecord[] records) {
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
    println("Topic: " + record.topic + " Partition: " + record.partition + " Received Message: " + msg);
}