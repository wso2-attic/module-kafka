import ballerina.net.kafka;

function funcKafkaConnect() (kafka:Consumer) {
  kafka:Consumer con  = getConsumer();
  var conErr = con.connect();
  string[] topics = [];
  topics[0] = "test";
  var e = con.subscribe(topics);
  return con;
}

function funcKafkaClose(kafka:Consumer con) (boolean) {
   var conErr = con.close();
   return true;
}

function funcKafkaPoll(kafka:Consumer con) (int) {
    kafka:ConsumerRecord[] records;
    error err;
    records, err = con.poll(1000);
    return lengthof records;
}

function funcKafkaPause(kafka:Consumer con, kafka:TopicPartition[] partitions) (error) {
   error e;
   e = con.pause(partitions);
   return e;
}

function funcKafkaResume(kafka:Consumer con, kafka:TopicPartition[] partitions) (error) {
   error e;
   e = con.resume(partitions);
   return e;
}

function funcKafkaGetPausedPartitions(kafka:Consumer con) (kafka:TopicPartition[]) {
   kafka:TopicPartition[] partitions;
   error e;
   partitions, e = con.getPausedPartitions();
   return partitions;
}

function getConsumer () (kafka:Consumer) {
    kafka:Consumer con = {};
    kafka:ConsumerConfig conf = { bootstrapServers:"localhost:9094", groupId:"abcd", offsetReset:"earliest" };
    con.config = conf;
    return con;
}

