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

function funcKafkaGetSubscription(kafka:KafkaConsumer con) (string[]) {
   string[] topics;
   error err;
   topics, err = con.getSubscription();
   return topics;
}

function funcKafkaGetAssignment(kafka:KafkaConsumer con) (kafka:TopicPartition[]) {
   kafka:TopicPartition[] partitions;
   error err;
   partitions, err = con.getAssignment();
   return partitions;
}

function funcKafkaPoll(kafka:KafkaConsumer con) (int) {
    kafka:ConsumerRecord[] records;
    error err;
    records, err = con.poll(1000);
    return lengthof records;
}

function getConsumer () (kafka:KafkaConsumer) {
    kafka:KafkaConsumer con = {};
    map m = {"bootstrap.servers":"localhost:9094","group.id": "abcd", "auto.offset.reset": "earliest"};
    con.properties = m;
    return con;
}

