package ballerina.net.kafka;

import ballerina.util;

@Description { value:"Struct which represents Kafka Producer configuration" }
public struct ProducerConfig {
    string bootstrapServers;                    // BOOTSTRAP_SERVERS_CONFIG 0
    string acks;                                // ACKS_CONFIG 1
    string compressionType;                     // COMPRESSION_TYPE_CONFIG 2
    string clientID;                            // CLIENT_ID_CONFIG 3
    string metricsRecordingLevel;               // METRICS_RECORDING_LEVEL_CONFIG 4
    string metricReporterClasses;               // METRIC_REPORTER_CLASSES_CONFIG 5
    string partitionerClass;                    // PARTITIONER_CLASS_CONFIG 6
    string interceptorClasses;                  // INTERCEPTOR_CLASSES_CONFIG 7
    string transactionalID;                     // TRANSACTIONAL_ID_CONFIG 8

    int bufferMemory = -1;                      // BUFFER_MEMORY_CONFIG 0
    int noRetries = -1;                          // RETRIES_CONFIG 1
    int batchSize = -1;                         // BATCH_SIZE_CONFIG 2
    int linger = -1;                            // LINGER_MS_CONFIG 3
    int sendBuffer = -1;                        // SEND_BUFFER_CONFIG 4
    int receiveBuffer = -1;                     // RECEIVE_BUFFER_CONFIG 5
    int maxRequestSize = -1;                    // MAX_REQUEST_SIZE_CONFIG 6
    int reconnectBackoff = -1;                  // RECONNECT_BACKOFF_MS_CONFIG 7
    int reconnectBackoffMax = -1;               // RECONNECT_BACKOFF_MAX_MS_CONFIG  8
    int retryBackoff = -1;                      // RETRY_BACKOFF_MS_CONFIG 9
    int maxBlock = -1;                          // MAX_BLOCK_MS_CONFIG 10
    int requestTimeout = -1;                    // REQUEST_TIMEOUT_MS_CONFIG  11
    int metadataMaxAge = -1;                    // METADATA_MAX_AGE_CONFIG 12
    int metricsSampleWindow = -1;               // METRICS_SAMPLE_WINDOW_MS_CONFIG 13
    int metricsNumSamples = -1;                 // METRICS_NUM_SAMPLES_CONFIG  14
    int maxInFlightRequestsPerConnection = -1;  // MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION 15
    int connectionsMaxIdle = -1;                // CONNECTIONS_MAX_IDLE_MS_CONFIG 16
    int transactionTimeout = -1;                // TRANSACTION_TIMEOUT_CONFIG 17

    boolean enableIdempotence = false;          // ENABLE_IDEMPOTENCE_CONFIG 0
}

@Description { value:"Struct which represents Kafka Consumer configuration" }
public struct ConsumerConfig {
    string bootstrapServers;                    // BOOTSTRAP_SERVERS_CONFIG 0
    string groupId;                             // GROUP_ID_CONFIG 1
    string offsetReset;                         // AUTO_OFFSET_RESET_CONFIG 2
    string partitionAssignmentStrategy;         // PARTITION_ASSIGNMENT_STRATEGY_CONFIG 3
    string metricsRecordingLevel;               // METRICS_RECORDING_LEVEL_CONFIG 4
    string metricsReporterClasses;              // METRIC_REPORTER_CLASSES_CONFIG 5
    string clientId;                            // CLIENT_ID_CONFIG 6
    string interceptorClasses;                  // INTERCEPTOR_CLASSES_CONFIG 7
    string isolationLevel;                      // ISOLATION_LEVEL_CONFIG 8

    int sessionTimeout = -1;                    // SESSION_TIMEOUT_MS_CONFIG  0
    int heartBeatInterval = -1;                 // HEARTBEAT_INTERVAL_MS_CONFIG 1
    int metadataMaxAge = -1;                    // METADATA_MAX_AGE_CONFIG  2
    int autoCommitInterval = -1;                // AUTO_COMMIT_INTERVAL_MS_CONFIG 3
    int maxPartitionFetchBytes = -1;            // MAX_PARTITION_FETCH_BYTES_CONFIG 4
    int sendBuffer = -1;                        // SEND_BUFFER_CONFIG 5
    int receiveBuffer = -1;                     // RECEIVE_BUFFER_CONFIG 6
    int fetchMinBytes = -1;                     // FETCH_MIN_BYTES_CONFIG 7
    int fetchMaxBytes = -1;                     // FETCH_MAX_BYTES_CONFIG 8
    int fetchMaxWait = -1;                      // FETCH_MAX_WAIT_MS_CONFIG 9
    int reconnectBackoffMax = -1;               // RECONNECT_BACKOFF_MAX_MS_CONFIG 10
    int retryBackoff = -1;                      // RETRY_BACKOFF_MS_CONFIG 11
    int metricsSampleWindow = -1;               // METRICS_SAMPLE_WINDOW_MS_CONFIG 12
    int metricsNumSamples = -1;                 // METRICS_NUM_SAMPLES_CONFIG 13
    int requestTimeout = -1;                    // REQUEST_TIMEOUT_MS_CONFIG 14
    int connectionMaxIdle = -1;                 // CONNECTIONS_MAX_IDLE_MS_CONFIG 15
    int maxPollRecords = -1;                    // MAX_POLL_RECORDS_CONFIG 16
    int maxPollInterval = -1;                   // MAX_POLL_INTERVAL_MS_CONFIG 17

    boolean autoCommit = true;                  // ENABLE_AUTO_COMMIT_CONFIG 0
    boolean checkCRCS = true;                   // CHECK_CRCS_CONFIG 1
    boolean excludeInternalTopics = true;       // EXCLUDE_INTERNAL_TOPICS_CONFIG 2
}

@Description { value:"Struct which represents Kafka producer"}
public struct Producer {
}

@Description { value:"Struct which represents Kafka consumer"}
@Field { value:"config: Consumer configurations Kafka consumer." }
public struct Consumer {
   ConsumerConfig config;
}

@Description { value:"Struct which represents Kafka producer record"}
@Field { value:"key: Record key byte array" }
@Field { value:"value: Record value byte array" }
@Field { value:"topic: Topic record to be sent" }
@Field { value:"partition: Topic partition record to be sent" }
@Field { value:"timestamp: Timestamp to be considered over broker side" }
public struct ProducerRecord {
   blob key;
   blob value;
   string topic;
   int partition = -1;
   int timestamp = -1;
}

@Description { value:"Producer Client Connector for execute producing kafka records to the broker"}
@Param { value:"bootstrapServers: Producer configuration" }
@Param { value:"conf: Producer configuration" }
public connector ProducerClient (string[] bootstrapServers, ProducerConfig conf) {

    map producerHolder = {};
    string connectorID = util:uuid();

    @Description { value:"Simple Send action which produce records to Kafka server"}
    @Param { value:"value: value of Kafka ProducerRecord to be sent." }
    @Param { value:"topic: topic of Kafka ProducerRecord to be sent." }
    native action send (blob value, string topic);

    @Description { value:"Advanced Send action which produce records to Kafka server"}
    @Param { value:"record: ProducerRecord to be sent." }
    native action sendAdvanced (ProducerRecord record);

    @Description { value:"Flush action which flush batch of records"}
    native action flush ();

    @Description { value:"Close action which closes Kafka producer"}
    native action close ();

    @Description { value:"GetTopicPartitions action which returns given topic partition information"}
    @Param { value:"topic: Topic which partition information is given" }
    @Return { value:"TopicPartition[]: Partition for given topic" }
    native action getTopicPartitions (string topic) (TopicPartition[]);

    @Description { value:"CommitConsumer action which commits consumer consumed offsets to offset topic"}
    @Param { value:"consumer: Consumer which needs offsets to be committed" }
    native action commitConsumer (Consumer consumer);

    @Description { value:"CommitConsumerOffsets action which commits consumer offsets in given transaction"}
    @Param { value:"offsets: Consumer offsets to commit for given transaction" }
    @Param { value:"groupID: Consumer group id" }
    native action commitConsumerOffsets (Offset[] offsets, string groupID);

}

@Description { value:"Struct which represents Topic partition"}
@Field { value:"topic: Topic which partition is related" }
@Field { value:"partition: Index for the partition" }
public struct TopicPartition {
  string topic;
  int partition;
}

@Description { value:"Struct which represents Consumer Record which returned from pol cycle"}
@Field { value:"key: Record key byte array" }
@Field { value:"value: Record value byte array" }
@Field { value:"offset: Offset of the Record positioned in partition" }
@Field { value:"partition: Topic partition record to be sent" }
@Field { value:"timestamp: Timestamp to be considered over broker side" }
@Field { value:"topic: Topic record to be sent" }
public struct ConsumerRecord {
   blob key;
   blob value;
   int offset;
   int partition;
   int timestamp;
   string topic;
}

@Description { value:"Struct which represents Topic partition position in which consumed record is stored"}
@Field { value:"partition: TopicPartition which record is related" }
@Field { value:"offset: offset in which record is stored in partition" }
public struct Offset {
  TopicPartition partition;
  int offset;
}

@Description { value:"Connects to consumer to external Kafka broker"}
@Return { value:"error: Error will be returned if connection to broker is failed" }
public native function <Consumer consumer> connect() (error);

@Description { value:"Subscribes to consumer to external Kafka broker topic pattern"}
@Param { value:"regex: Topic pattern to be subscribed" }
@Return { value:"error: Error will be returned if subscription to broker is failed" }
public native function <Consumer consumer> subscribeToPattern(string regex) (error);

@Description { value:"Subscribes to consumer to external Kafka broker topic array"}
@Param { value:"regex: Topic array to be subscribed" }
@Return { value:"error: Error will be returned if subscription to broker is failed" }
public native function <Consumer consumer> subscribe(string[] topics) (error);

@Description { value:"Subscribes to consumer to external Kafka broker topic with rebalance listening is enabled"}
@Param { value:"regex: Topic array to be subscribed" }
@Param { value:"onPartitionsRevoked: Function will be executed if partitions are revoked from this consumer" }
@Param { value:"onPartitionsAssigned: Function will be executed if partitions are assigned this consumer" }
@Return { value:"error: Error will be returned if subscription to broker is failed" }
public native function <Consumer consumer> subscribeWithPartitionRebalance(string[] topics,
  function(Consumer consumer, TopicPartition[] partitions) onPartitionsRevoked,
  function(Consumer consumer, TopicPartition[] partitions) onPartitionsAssigned) (error);

@Description { value:"Assign consumer to external Kafka broker set of topic partitions"}
@Param { value:"partitions: Topic partitions to be assigned" }
@Return { value:"error: Error will be returned if assignment to broker is failed" }
public native function <Consumer consumer> assign(TopicPartition[] partitions) (error);

@Description { value:"Returns current offset position in which consumer is at"}
@Param { value:"partition: Topic partitions in which the position is required" }
@Return { value:"int: Position in which the consumer is at in given Topic partition" }
@Return { value:"error: Error will be returned if position retrieval from broker is failed" }
public native function <Consumer consumer> getPositionOffset(TopicPartition partition) (int, error);

@Description { value:"Returns current assignment of partitions for a consumer"}
@Return { value:"TopicPartition[]: Assigned partitions array for consumer" }
@Return { value:"error: Error will be returned if assignment retrieval from broker is failed" }
public native function <Consumer consumer> getAssignment() (TopicPartition[], error);

@Description { value:"Returns current subscription of topics for a consumer"}
@Return { value:"string[]: Subscribed topic array for consumer" }
@Return { value:"error: Error will be returned if subscription retrieval from broker is failed" }
public native function <Consumer consumer> getSubscription() (string[], error);

@Description { value:"Returns current subscription of topics for a consumer"}
@Param { value:"partition: Partition in which offset is returned for consumer" }
@Return { value:"Offset: Committed offset for consumer for given partition" }
@Return { value:"error: Error will be returned if committed offset retrieval from broker is failed" }
public native function <Consumer consumer> getCommittedOffset(TopicPartition partition) (Offset, error);

@Description { value:"Poll the consumer for external broker for records"}
@Param { value:"timeoutValue: Polling time in milliseconds" }
@Return { value:"ConsumerRecord[]: Consumer record array" }
@Return { value:"error: Error will be returned if record retrieval from broker is failed" }
public native function <Consumer consumer> poll(int timeoutValue) (ConsumerRecord[], error);

@Description { value:"Commits current consumed offsets for consumer"}
public native function <Consumer consumer> commit();

@Description { value:"Commits given offsets for consumer"}
@Param { value:"offsets: Offsets to be commited" }
public native function <Consumer consumer> commitOffset(Offset[] offsets);

@Description { value:"Seek consumer for given offset in a topic partition" }
@Param { value:"offset: Given offset to seek" }
@Return { value:"error: Error will be returned if seeking of position is failed" }
public native function <Consumer consumer> seek(Offset offset) (error);

@Description { value:"Seek consumer for beginning offsets for set of topic partitions"}
@Param { value:"partitions: Set of partitions to seek" }
@Return { value:"error: Error will be returned if seeking of partitions is failed" }
public native function <Consumer consumer> seekToBeginning(TopicPartition[] partitions) (error);

@Description { value:"Seek consumer for end offsets for set of topic partitions"}
@Param { value:"partitions: Set of partitions to seek" }
@Return { value:"error: Error will be returned if seeking of partitions is failed" }
public native function <Consumer consumer> seekToEnd(TopicPartition[] partitions) (error);

@Description { value:"Retrieve the set of partitions which topic belongs"}
@Param { value:"topic: Given topic for partition information is needed" }
@Return { value:"TopicPartition[]: Partition array for given topic" }
@Return { value:"error: Error will be returned if retrieval of partition information is failed" }
public native function <Consumer consumer> getTopicPartitions (string topic) (TopicPartition[], error);

@Description { value:"Un-subscribe consumer from all external broaker topic subscription"}
@Return { value:"error: Error will be returned if unsubscription from topics is failed" }
public native function <Consumer consumer> unsubscribe() (error);

@Description { value:"Closes consumer connection to external Kafka broker"}
@Return { value:"error: Error will be returned if connection close to broker is failed" }
public native function <Consumer consumer> close() (error);

@Description { value:"Pause consumer retrieving messages from set of partitions"}
@Param { value:"partitions: Set of partitions to pause messages retrieval" }
@Return { value:"error: Error will be returned if pausing message retrieval is failed" }
public native function <Consumer consumer> pause(TopicPartition[] partitions) (error);

@Description { value:"Resume consumer retrieving messages from set of partitions which were paused earlier"}
@Param { value:"partitions: Set of partitions to resume messages retrieval" }
@Return { value:"error: Error will be returned if resuming message retrieval is failed" }
public native function <Consumer consumer> resume(TopicPartition[] partitions) (error);

@Description { value:"Returns partitions in which the consumer is paused retrieving messages"}
@Return { value:"TopicPartition[]: Set of partitions paused from message retrieval" }
@Return { value:"error: Error will be returned if paused partitions retrieval is failed" }
public native function <Consumer consumer> getPausedPartitions() (TopicPartition[], error);

@Description { value:"Returns start offsets for given set of partitions"}
@Param { value:"partitions: Set of partitions to return start offsets" }
@Return { value:"Offset[]: Start offsets for partitions" }
@Return { value:"error: Error will be returned if offset retrieval is failed" }
public native function <Consumer consumer> getBeginningOffsets(TopicPartition[] partitions) (Offset[], error);

@Description { value:"Returns last offsets for given set of partitions"}
@Param { value:"partitions: Set of partitions to return last offsets" }
@Return { value:"Offset[]: Last offsets for partitions" }
@Return { value:"error: Error will be returned if offset retrieval is failed" }
public native function <Consumer consumer> getEndOffsets(TopicPartition[] partitions) (Offset[], error);

