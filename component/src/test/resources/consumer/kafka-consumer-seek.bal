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


function funcKafkaGetPositionOffset(kafka:KafkaConsumer con, kafka:TopicPartition part) (int, error) {
   int offset;
   error e;
   offset, e = con.getPositionOffset(part);
   return offset, e;
}

function funcKafkaSeekOffset(kafka:KafkaConsumer con, kafka:Offset offset) {
   error e;
   e = con.seek(offset);
}

function funcKafkaSeekToBegin(kafka:KafkaConsumer con, kafka:TopicPartition[] partitions) {
   error e;
   e = con.seekToBeginning(partitions);
}

function funcKafkaSeekToEnd(kafka:KafkaConsumer con, kafka:TopicPartition[] partitions) {
   error e;
   e = con.seekToEnd(partitions);
}

function funcKafkaBeginOffsets(kafka:KafkaConsumer con, kafka:TopicPartition[] partitions) (kafka:Offset[]) {
   kafka:Offset[] offsets;
   error e;
   offsets, e = con.getBeginningOffsets(partitions);
   return offsets;
}

function funcKafkaEndOffsets(kafka:KafkaConsumer con, kafka:TopicPartition[] partitions) (kafka:Offset[]) {
   kafka:Offset[] offsets;
   error e;
   offsets, e = con.getEndOffsets(partitions);
   return offsets;
}

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer con = {};
    map m = {"bootstrap.servers":"localhost:9094","group.id": "abcd", "auto.offset.reset": "earliest"};
    con.properties = m;
    return con;
}
