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


function funcKafkaGetPositionOffset(kafka:Consumer con, kafka:TopicPartition part) (int, error) {
   int offset;
   error e;
   offset, e = con.getPositionOffset(part);
   return offset, e;
}

function funcKafkaSeekOffset(kafka:Consumer con, kafka:Offset offset) {
   error e;
   e = con.seek(offset);
}

function funcKafkaSeekToBegin(kafka:Consumer con, kafka:TopicPartition[] partitions) {
   error e;
   e = con.seekToBeginning(partitions);
}

function funcKafkaSeekToEnd(kafka:Consumer con, kafka:TopicPartition[] partitions) {
   error e;
   e = con.seekToEnd(partitions);
}

function funcKafkaBeginOffsets(kafka:Consumer con, kafka:TopicPartition[] partitions) (kafka:Offset[]) {
   kafka:Offset[] offsets;
   error e;
   offsets, e = con.getBeginningOffsets(partitions);
   return offsets;
}

function funcKafkaEndOffsets(kafka:Consumer con, kafka:TopicPartition[] partitions) (kafka:Offset[]) {
   kafka:Offset[] offsets;
   error e;
   offsets, e = con.getEndOffsets(partitions);
   return offsets;
}

function getConsumer () (kafka:Consumer) {
    kafka:Consumer con = {};
    kafka:ConsumerConfig conf = { bootstrapServers:"localhost:9094", groupId:"abcd", offsetReset:"earliest" };
    con.config = conf;
    return con;
}
