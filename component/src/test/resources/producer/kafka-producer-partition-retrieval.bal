import ballerina.net.kafka;

function funcTestPartitionInfoRetrieval(string topic) (kafka:TopicPartition[]) {
    return getPartitionInfo(topic);
}

function getPartitionInfo(string topic) (kafka:TopicPartition[]) {
    endpoint<kafka:KafkaProducerConnector> kafkaEP {
        create kafka:KafkaProducerConnector (getConnectorConfig());
    }
    kafka:TopicPartition[] partitions = kafkaEP.getTopicPartitions(topic);
    kafkaEP.close();
    return partitions;
}

function getConnectorConfig () (kafka:KafkaProducerConf) {
    kafka:KafkaProducerConf conf = {};
    map m = {"bootstrap.servers":"localhost:9094"};
    conf.properties = m;
    return conf;
}