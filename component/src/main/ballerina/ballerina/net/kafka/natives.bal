package ballerina.net.kafka;

public struct KafkaProducer {
}

public struct KafkaConsumer {
   map properties;
}

public struct KafkaProducerConf {
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

public native function <KafkaConsumer consumer> connect();

public native function <KafkaConsumer consumer> subscribe(string[] topics) (error);

public native function <KafkaConsumer consumer> assign(TopicPartition[] partitions) (error);

public native function <KafkaConsumer consumer> getPositionOffset(TopicPartition partition) (int, error);

public native function <KafkaConsumer consumer> getCommittedOffset(TopicPartition partition) (int, error);

public native function <KafkaConsumer consumer> poll(int timeoutValue) (ConsumerRecord[], error);

public native function <KafkaConsumer consumer> commit();

public native function <KafkaConsumer consumer> commitOffset(Offset[] offsets);

public native function <KafkaConsumer consumer> seek(Offset offset) (error);

public native function <KafkaConsumer consumer> getTopicPartitions (string topic) (PartitionInfo[], error);

public native function <KafkaConsumer consumer> unsubscribe() (error);

public native function <KafkaConsumer consumer> close() (error);

public native function serialize (string s) (blob);

public native function deserialize (blob b) (string);

