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

    public native action send (ProducerRecord record) (error);

    public native action flush () (error);

    public native action close () (error);

    public native action getTopicPartitions () (PartitionInfo[], error);

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

    public native action subscribe(string[] topics) (error);

    public native action assign(TopicPartition[] partitions) (error);

    public native action getPositionOffset(TopicPartition partition) (int, error);

    public native action getCommittedOffset(TopicPartition partition) (int, error);

    public native action poll(int timeout) (ConsumerRecord[], error);

    public native action commit() (error);

    public native action commitOffset(TopicPartition partition, int offset) (error);

    public native action seek(TopicPartition partition, int offset) (error);

    public native action getTopicPartitions (PartitionInfo[], error);

    public native action unsubscribe() (error);

    public native action close() (error);

}
