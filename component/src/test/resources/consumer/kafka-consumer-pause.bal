import ballerina.net.kafka;

function funcKafkaConnect() (kafka:KafkaConsumer) {
  kafka:KafkaConsumer con  = getConsumer();
  var conErr = con.connect();
  string[] topics = [];
  topics[0] = "test";
  var e = con.subscribe(topics);
  return con;
}

function funcKafkaClose(kafka:KafkaConsumer con) (boolean) {
   var conErr = con.close();
   return true;
}

function funcKafkaPoll(kafka:KafkaConsumer con) (int) {
    kafka:ConsumerRecord[] records;
    error err;
    records, err = con.poll(1000);
    return lengthof records;
}

function funcKafkaPause(kafka:KafkaConsumer con, kafka:TopicPartition[] partitions) (error) {
   error e;
   e = con.pause(partitions);
   return e;
}

function funcKafkaResume(kafka:KafkaConsumer con, kafka:TopicPartition[] partitions) (error) {
   error e;
   e = con.resume(partitions);
   return e;
}

function funcKafkaGetPausedPartitions(kafka:KafkaConsumer con) (kafka:TopicPartition[]) {
   kafka:TopicPartition[] partitions;
   error e;
   partitions, e = con.getPausedPartitions();
   return partitions;
}

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer con = {};
    map m = {"bootstrap.servers":"localhost:9094","group.id": "abcd", "auto.offset.reset": "earliest"};
    con.properties = m;
    return con;
}

