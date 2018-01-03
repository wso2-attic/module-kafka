import ballerina.net.kafka;

function funcTestPartitionInfoRetrieval(string topic) (kafka:TopicPartition[]) {
    return getPartitionInfo(topic);
}

function getPartitionInfo(string topic) (kafka:TopicPartition[]) {
    endpoint<kafka:KafkaProducerClient> kafkaEP {
        create kafka:KafkaProducerClient (getConnectorConfig());
    }
    kafka:TopicPartition[] partitions = kafkaEP.getTopicPartitions(topic);
    kafkaEP.close();
    return partitions;
}

function getConnectorConfig () (kafka:KafkaProducer) {
    kafka:KafkaProducer prod = {};
    map m = {"bootstrap.servers":"localhost:9094"};
    prod.properties = m;
    return prod;
}