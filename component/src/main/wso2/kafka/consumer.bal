// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

# Configuration related to consumer endpoint.
#
# + bootstrapServers - list of remote server endpoints
# + groupId - unique string that identifies the consumer
# + offsetReset - offset reset strategy if no initial offset
# + partitionAssignmentStrategy - strategy class for handle partition assignment among consumers
# + metricsRecordingLevel - metrics recording level
# + metricsReporterClasses - metrics reporter classes
# + clientId - id to be used for server side logging
# + interceptorClasses - interceptor classes to be used before sending records
# + isolationLevel - how the transactional messages are read
# + topics - topics to be subscribed
# + properties - additional properties if required
# + sessionTimeout - timeout used to detect consumer failures when heartbeat threshold is reached
# + heartBeatInterval - expected time between heartbeats
# + metadataMaxAge - max time to force a refresh of metadata
# + autoCommitInterval - offset committing interval
# + maxPartitionFetchBytes - the max amount of data per-partition the server return
# + sendBuffer - size of the TCP send buffer (SO_SNDBUF)
# + receiveBuffer - size of the TCP receive buffer (SO_RCVBUF)
# + fetchMinBytes - minimum amount of data the server should return for a fetch request
# + fetchMaxBytes - maximum amount of data the server should return for a fetch request
# + fetchMaxWait - maximum amount of time the server will block before answering the fetch request
# + reconnectBackoffMax - maximum amount of time in milliseconds to wait when reconnecting
# + retryBackoff - time to wait before attempting to retry a failed request
# + metricsSampleWindow - window of time a metrics sample is computed over
# + metricsNumSamples - number of samples maintained to compute metrics
# + requestTimeout - wait time for response of a request
# + connectionMaxIdle - close idle connections after the number of milliseconds
# + maxPollRecords - maximum number of records returned in a single call to poll
# + maxPollInterval - maximum delay between invocations of poll
# + reconnectBackoff - time to wait before attempting to reconnect
# + pollingTimeout - time out interval of polling
# + pollingInterval - polling interval
# + concurrentConsumers - number of concurrent consumers
# + autoCommit - enables auto commit offsets
# + checkCRCS - check the CRC32 of the records consumed
# + excludeInternalTopics - whether records from internal topics should be exposed to the consumer
# + decoupleProcessing - decouples processing
public type ConsumerConfig record {
    string? bootstrapServers; // BOOTSTRAP_SERVERS_CONFIG 0
    string? groupId; // GROUP_ID_CONFIG 1
    string? offsetReset; // AUTO_OFFSET_RESET_CONFIG 2
    string? partitionAssignmentStrategy; // PARTITION_ASSIGNMENT_STRATEGY_CONFIG 3
    string? metricsRecordingLevel; // METRICS_RECORDING_LEVEL_CONFIG 4
    string? metricsReporterClasses; // METRIC_REPORTER_CLASSES_CONFIG 5
    string? clientId; // CLIENT_ID_CONFIG 6
    string? interceptorClasses; // INTERCEPTOR_CLASSES_CONFIG 7
    string? isolationLevel; // ISOLATION_LEVEL_CONFIG 8

    string[]? topics; // ALIAS_TOPICS 0
    string[]? properties; // PROPERTIES_ARRAY 1

    int sessionTimeout = -1; // SESSION_TIMEOUT_MS_CONFIG  0
    int heartBeatInterval = -1; // HEARTBEAT_INTERVAL_MS_CONFIG 1
    int metadataMaxAge = -1; // METADATA_MAX_AGE_CONFIG  2
    int autoCommitInterval = -1; // AUTO_COMMIT_INTERVAL_MS_CONFIG 3
    int maxPartitionFetchBytes = -1; // MAX_PARTITION_FETCH_BYTES_CONFIG 4
    int sendBuffer = -1; // SEND_BUFFER_CONFIG 5
    int receiveBuffer = -1; // RECEIVE_BUFFER_CONFIG 6
    int fetchMinBytes = -1; // FETCH_MIN_BYTES_CONFIG 7
    int fetchMaxBytes = -1; // FETCH_MAX_BYTES_CONFIG 8
    int fetchMaxWait = -1; // FETCH_MAX_WAIT_MS_CONFIG 9
    int reconnectBackoffMax = -1; // RECONNECT_BACKOFF_MAX_MS_CONFIG 10
    int retryBackoff = -1; // RETRY_BACKOFF_MS_CONFIG 11
    int metricsSampleWindow = -1; // METRICS_SAMPLE_WINDOW_MS_CONFIG 12
    int metricsNumSamples = -1; // METRICS_NUM_SAMPLES_CONFIG 13
    int requestTimeout = -1; // REQUEST_TIMEOUT_MS_CONFIG 14
    int connectionMaxIdle = -1; // CONNECTIONS_MAX_IDLE_MS_CONFIG 15
    int maxPollRecords = -1; // MAX_POLL_RECORDS_CONFIG 16
    int maxPollInterval = -1; // MAX_POLL_INTERVAL_MS_CONFIG 17
    int reconnectBackoff = -1; // RECONNECT_BACKOFF_MAX_MS_CONFIG 18
    int pollingTimeout = -1; // ALIAS_POLLING_TIMEOUT 19
    int pollingInterval = -1; // ALIAS_POLLING_INTERVAL 20
    int concurrentConsumers = -1; // ALIAS_CONCURRENT_CONSUMERS 21

    boolean autoCommit = true; // ENABLE_AUTO_COMMIT_CONFIG 0
    boolean checkCRCS = true; // CHECK_CRCS_CONFIG 1
    boolean excludeInternalTopics = true; // EXCLUDE_INTERNAL_TOPICS_CONFIG 2
    boolean decoupleProcessing;                 // ALIAS_DECOUPLE_PROCESSING
    !...
};

# Type related to consumer record.
#
# + key - Key that is included in the record.
# + value - Record content.
# + offset - Offset value.
# + partition - Partition to which the record is stored.
# + timestamp - Timestamp of the record, in milliseconds since epoch.
# + topic - Topic to which the record belongs to.
public type ConsumerRecord record {
    byte[] key;
    byte[] value;
    int offset;
    int partition;
    int timestamp;
    string topic;
    !...
};

# Kafka consumer service object.
public type Consumer object {

    # Returns the endpoint bound to service.
    #
    # + return - Kafka consumer endpoint bound to the service.
    public function getEndpoint() returns SimpleConsumer {
        SimpleConsumer consumer = new();
        return consumer;
    }
};

# Represent a Kafka consumer endpoint.
#
# + consumerActions - Handle all the actions related to the endpoint.
# + consumerConfig - Used to store configurations related to a Kafka connection.
public type SimpleConsumer object {

    public ConsumerAction consumerActions;
    public ConsumerConfig consumerConfig;

    # Initialize the consumer endpoint.
    #
    # + config - Configurations related to the endpoint.
    public function init(ConsumerConfig config) {
        self.consumerConfig = config;
        self.consumerActions.config = config;
        self.initEndpoint();
    }

    # Registers consumer endpoint in the service.
    #
    # + serviceType - Type descriptor of the service.
    public function register(typedesc serviceType) {
        self.registerListener(serviceType);
    }

    # Starts the consumer endpoint.
    public function start() {}

    # Returns the action object of ConsumerAction.
    #
    # + return - ConsumerAction object of the Caller.
    public function getCallerActions() returns ConsumerAction {
        return consumerActions;
    }

    # Stops the consumer endpoint.
    public function stop() {
        check self.consumerActions.close();
    }

    function initEndpoint() {
        match self.consumerConfig.bootstrapServers {
            () => {
                //do nothing
            }
            string servers => {
                check self.consumerActions.connect();
            }
        }

        match self.consumerConfig.topics {
            () => {
                //do nothing
            }
            string[] topics => {
                check self.consumerActions.subscribe(topics);
            }
        }
    }

    # Registers a listener to the Kafka service.
    extern function registerListener(typedesc serviceType);
};

# Kafka consumer action handling object.
#
# + config - Consumer Configuration.
public type ConsumerAction object {

    public ConsumerConfig config;

    # Assigns consumer to a set of topic partitions.
    #
    # + partitions - Topic partitions to be assigned.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function assign(TopicPartition[] partitions) returns error?;

    # Closes consumer connection to the external Kafka broker.
    #
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function close() returns error?;

    # Commits current consumed offsets for consumer.
    public extern function commit();

    # Commits given offsets and partitions for the given topics, for consumer.
    #
    # + offsets - Offsets to be commited.
    public extern function commitOffset(Offset[] offsets);

    // TODO:
    // Why we need this ?
    # Connects consumer to the provided host in the consumer configs.
    #
    # + return - Returns an error if encounters an error, returns nill otherwise.
    public extern function connect() returns error?;

    # Returns the currently assigned partitions for the consumer.
    #
    # + return - Array of assigned partitions for the consumer if executes successfully, error otherwise.
    public extern function getAssignment() returns TopicPartition[]|error;

    # Returns start offsets for given set of partitions.
    #
    # + partitions - Array of topic partitions to get the starting offsets.
    # + return - Starting offsets for the given partitions if executes successfully, error otherwise.
    public extern function getBeginningOffsets(TopicPartition[] partitions) returns Offset[]|error;

    # Returns last committed offsets for the given topic partitions.
    #
    # + partition - Topic partition in which the committed offset is returned for consumer.
    # + return - Committed offset for the consumer for the given partition if executes successfully, error otherwise.
    public extern function getCommittedOffset(TopicPartition partition) returns Offset|error;

    # Returns last offsets for given set of partitions.
    #
    # + partitions - Set of partitions to get the last offsets.
    # + return - End offsets for the given partitions if executes successfully, error otherwise.
    public extern function getEndOffsets(TopicPartition[] partitions) returns Offset[]|error;

    # Returns the partitions, which are currently paused.
    #
    # + return - Set of partitions paused from message retrieval if executes successfully, error otherwise.
    public extern function getPausedPartitions() returns TopicPartition[]|error;

    # Returns the offset of the next record that will be fetched, if a records exists in that position.
    #
    # + partition - Topic partition in which the position is required.
    # + return - Offset which will be fetched next (if a records exists in that offset).
    public extern function getPositionOffset(TopicPartition partition) returns int|error;

    # Returns set of topics wich are currently subscribed by the consumer.
    #
    # + return - Array of subscribed topics for the consumer if executes successfully, error otherwise.
    public extern function getSubscription() returns string[]|error;

    # Retrieve the set of partitions in which the topic belongs.
    #
    # + topic - Given topic for partition information is needed.
    # + return - Array of partitions for the given topic if executes successfully, error otherwise.
    public extern function getTopicPartitions(string topic) returns TopicPartition[]|error;

    # Pause consumer retrieving messages from set of partitions.
    #
    # + partitions - Set of partitions to pause the retrieval of messages.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function pause(TopicPartition[] partitions) returns error?;

    # Poll the consumer for external broker for records.
    #
    # + timeoutValue - Polling time in milliseconds.
    # + return - Array of consumer records if executes successfully, error otherwise.
    public extern function poll(int timeoutValue) returns ConsumerRecord[]|error;

    # Resume consumer retrieving messages from set of partitions which were paused earlier.
    #
    # + partitions - Set of partitions to resume the retrieval of messages.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function resume(TopicPartition[] partitions) returns error?;

    # Seek the consumer for a given offset in a topic partition.
    #
    # + offset - Offset to seek.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function seek(Offset offset) returns error?;

    # Seek consumer to the beginning of the offsets for the given set of topic partitions.
    #
    # + partitions - Set of topic partitions to seek.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function seekToBeginning(TopicPartition[] partitions) returns error?;

    # Seek consumer for end of the offsets for the given set of topic partitions.
    #
    # + partitions - Set of topic partitions to seek.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function seekToEnd(TopicPartition[] partitions) returns error?;

    # Subscribes the consumer to the provided set of topics.
    #
    # + topics - Array of topics to be subscribed.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function subscribe(string[] topics) returns error?;

    # Subscribes the consumer to the topics which matches to the provided pattern.
    #
    # + regex - Pattern which should be matched with the topics to be subscribed.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function subscribeToPattern(string regex) returns error?;

    # Subscribes to consumer to the provided set of topics with rebalance listening is enabled.
    #
    # + topics - Array of topics to be subscribed.
    # + onPartitionsRevoked - Function which will be executed if partitions are revoked from this consumer.
    # + onPartitionsAssigned - Function which will be executed if partitions are assigned this consumer.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function subscribeWithPartitionRebalance(string[] topics,
    function(ConsumerAction consumerActions, TopicPartition[] partitions) onPartitionsRevoked,
    function(ConsumerAction consumerActions, TopicPartition[] partitions) onPartitionsAssigned) returns error?;

    # Unsubscribe the consumer from all the topic subscriptions.
    #
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function unsubscribe() returns error?;
};
