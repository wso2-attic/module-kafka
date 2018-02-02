import ballerina.net.kafka;

function funcTestPartitionInfoRetrieval(string topic) (kafka:TopicPartition[]) {
    return getPartitionInfo(topic);
}

function getPartitionInfo(string topic) (kafka:TopicPartition[]) {
    endpoint<kafka:ProducerClient> kafkaEP {
        create kafka:ProducerClient (["localhost:9094"], getProducerConfig());
    }
    kafka:TopicPartition[] partitions = kafkaEP.getTopicPartitions(topic);
    kafkaEP.close();
    return partitions;
}

function getProducerConfig () (kafka:ProducerConfig) {
    kafka:ProducerConfig pc = {clientID:"basic-producer"};
    return pc;
}