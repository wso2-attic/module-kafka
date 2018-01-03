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

function funcKafkaGetCommittedOffset(kafka:KafkaConsumer con, kafka:TopicPartition part) (kafka:Offset, error) {
   kafka:Offset offset;
   error e;
   offset, e = con.getCommittedOffset(part);
   return offset, e;
}

function funcKafkaGetPositionOffset(kafka:KafkaConsumer con, kafka:TopicPartition part) (int, error) {
   int offset;
   error e;
   offset, e = con.getPositionOffset(part);
   return offset, e;
}

function funcKafkaCommit(kafka:KafkaConsumer con) {
   con.commit();
}

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer con = {};
    map m = {"bootstrap.servers":"localhost:9094","group.id": "abcd", "auto.offset.reset": "earliest",
                                                                                "enable.auto.commit":"false"};
    con.properties = m;
    return con;
}

