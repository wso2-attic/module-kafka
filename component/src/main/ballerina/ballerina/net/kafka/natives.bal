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

    map producer = {};

    native action send (ProducerRecord record) ();

    native action flush () (error);

    native action close () (error);

    native action getTopicPartitions (string topic) (PartitionInfo[], error);

    native action sendOffsetsTransaction(Offset[] offsets, string groupID) ();

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

public struct Offset {
  TopicPartition partition;
  int offset;
}

public connector KafkaConsumerConnector (KafkaConsumerConf conf) {

    map consumer = {};

    native action subscribe(string[] topics) (error);

    native action assign(TopicPartition[] partitions) (error);

    native action getPositionOffset(TopicPartition partition) (int, error);

    native action getCommittedOffset(TopicPartition partition) (int, error);

    native action poll(int timeoutValue) (ConsumerRecord[], error);

    native action commit() ();

    native action commitOffset(Offset[] offsets) ();

    native action seek(Offset offset) (error);

    native action getTopicPartitions (string topic) (PartitionInfo[], error);

    native action unsubscribe() (error);

    native action close() (error);

}

public native function serialize (string s) (blob);

public native function deserialize (blob b) (string);