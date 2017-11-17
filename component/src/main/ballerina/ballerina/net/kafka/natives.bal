package ballerina.net.kafka;

public struct KafkaProducer {
}

public struct KafkaConsumer {
}

public struct KafkaProducerConf {
    map properties;
}

public struct KafkaConsumerConf {
    map properties;
}

public struct ProducerRecord {
   blob key;
   blob value;
   string topic;
   int  partition;
   int timestamp;
}

public struct PartitionInfo {
   string  topic;
   int partition;
   int leader;
   int  replicas;
   int  isr;
}


public connector KafkaProducerConnector (KafkaProducerConf conf) {

    KafkaProducer producer = {};

    native action send (ProducerRecord record) (error);

    native action flush () (error);

    native action close () (error);

    native action getTopicPartitions () (PartitionInfo[], error);

}

public struct TopicPartition {
  string topic;
  int partition;
}

public struct ConsumerRecord {
   blob key;
   blob value;
   int offset;
   int  partition;
   int timestamp;
   string topic;
}

public connector KafkaConsumerConnector (KafkaConsumerConf conf) {

    KafkaConsumer consumer = {};

    native action subscribe(string[] topics) (error);

    native action assign(TopicPartition[] partitions) (error);

    native action getPositionOffset(TopicPartition partition) (int, error);

    native action getCommittedOffset(TopicPartition partition) (int, error);

    native action poll(int timeout) (ConsumerRecord[], error);

    native action commit() (error);

    native action commitOffset(TopicPartition partition, int offset) (error);

    native action seek(TopicPartition partition, int offset) (error);

    native action getTopicPartitions (PartitionInfo[], error);

    native action unsubscribe() (error);

    native action close() (error);

}
