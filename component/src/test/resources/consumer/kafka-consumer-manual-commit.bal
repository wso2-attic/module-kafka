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

function funcKafkaGetCommittedOffset(kafka:Consumer con, kafka:TopicPartition part) (kafka:Offset, error) {
   kafka:Offset offset;
   error e;
   offset, e = con.getCommittedOffset(part);
   return offset, e;
}

function funcKafkaGetPositionOffset(kafka:Consumer con, kafka:TopicPartition part) (int, error) {
   int offset;
   error e;
   offset, e = con.getPositionOffset(part);
   return offset, e;
}

function funcKafkaCommit(kafka:Consumer con) {
   con.commit();
}

function getConsumer () (kafka:Consumer) {
    kafka:Consumer con = {};
    kafka:ConsumerConfig conf = { bootstrapServers:"localhost:9094", groupId:"abcd", offsetReset:"earliest",
                                                   autoCommit:false };
    con.config = conf;
    return con;
}

