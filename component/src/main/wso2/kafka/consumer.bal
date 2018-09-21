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
# + key - key that is included in the record
# + value - record content
# + offset - offset value
# + partition - partition to which the record is stored
# + timestamp - timestamp of the record, in milliseconds since epoch
# + topic - topic of the record
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
# + consumerActions - handle all the actions related to the endpoint
# + consumerConfig - used to store configurations related to a Kafka connection
public type SimpleConsumer object {

    public ConsumerAction consumerActions;
    public ConsumerConfig consumerConfig;

    # Initialize the consumer endpoint.
    #
    # + config - configurations related to the endpoint.
    public function init(ConsumerConfig config) {
        self.consumerConfig = config;
        self.consumerActions.config = config;
        self.initEndpoint();
    }

    # Registers consumer endpoint in the service.
    #
    # + serviceType - type descriptor of the service.
    public function register(typedesc serviceType) {
        self.registerListener(serviceType);
    }

    # Starts the consumer endpoint.
    public function start() {}

    # Returns the action object of ConsumerAction.
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

    extern function registerListener(typedesc serviceType);

};

# Kafka consumer action handling object.
#
# + config - Consumer Configuration.
public type ConsumerAction object {

    public ConsumerConfig config;

    public extern function connect() returns error?;

    # Subscribes to consumer to external Kafka broker topic pattern.
    #
    # + regex - topic pattern to be subscribed.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function subscribeToPattern(string regex) returns error?;

    # Subscribes to consumer to external Kafka broker topic array.
    #
    # + topics - tTopic array to be subscribed.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function subscribe(string[] topics) returns error?;

    # Subscribes to consumer to external Kafka broker topic with rebalance listening is enabled.
    #
    # + topics - topic array to be subscribed.
    # + onPartitionsRevoked - function will be executed if partitions are revoked from this consumer.
    # + onPartitionsAssigned - function will be executed if partitions are assigned this consumer.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function subscribeWithPartitionRebalance(string[] topics,
    function(ConsumerAction consumerActions, TopicPartition[] partitions) onPartitionsRevoked,
    function(ConsumerAction consumerActions, TopicPartition[] partitions) onPartitionsAssigned) returns error?;

    # Assign consumer to external Kafka broker set of topic partitions.
    #
    # + partitions - topic partitions to be assigned.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function assign(TopicPartition[] partitions) returns error?;

    # Returns current offset position in which consumer is at.
    #
    # + partition - topic partitions in which the position is required.
    # + return - position in which the consumer is at in given Topic partition if executes successfully, error otherwise.
    public extern function getPositionOffset(TopicPartition partition) returns int|error;

    # Returns current assignment of partitions for a consumer.
    #
    # + return - Assigned partitions array for consumer if executes successfully, error otherwise.
    public extern function getAssignment() returns TopicPartition[]|error;

    # Returns current subscription of topics for a consumer.
    #
    # + return - subscribed topic array for consumer if executes successfully, error otherwise.
    public extern function getSubscription() returns string[]|error;

    # Returns current subscription of topics for a consumer.
    #
    # + partition - partition in which offset is returned for consumer.
    # + return - committed offset for consumer for given partition if executes successfully, error otherwise.
    public extern function getCommittedOffset(TopicPartition partition) returns Offset|error;

    # Poll the consumer for external broker for records.
    #
    # + timeoutValue - polling time in milliseconds.
    # + return - consumer record array if executes successfully, error otherwise.
    public extern function poll(int timeoutValue) returns ConsumerRecord[]|error;

    # Commits current consumed offsets for consumer.
    public extern function commit();

    # Commits given offsets for consumer.
    #
    # + offsets - offsets to be commited.
    public extern function commitOffset(Offset[] offsets);

    # Seek consumer for given offset in a topic partition.
    #
    # + offset - given offset to seek.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function seek(Offset offset) returns error?;

    # Seek consumer for beginning offsets for set of topic partitions.
    #
    # + partitions - set of partitions to seek.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function seekToBeginning(TopicPartition[] partitions) returns error?;

    # Seek consumer for end offsets for set of topic partitions.
    #
    # + partitions - set of partitions to seek.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function seekToEnd(TopicPartition[] partitions) returns error?;

    # Retrieve the set of partitions which topic belongs.
    #
    # + topic - given topic for partition information is needed.
    # + return - partition array for given topic if executes successfully, error otherwise.
    public extern function getTopicPartitions(string topic) returns TopicPartition[]|error;

    # Un-subscribe consumer from all external broaker topic subscription.
    #
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function unsubscribe() returns error?;

    # Closes consumer connection to external Kafka broker.
    #
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function close() returns error?;

    # Pause consumer retrieving messages from set of partitions.
    #
    # + partitions - Set of partitions to pause messages retrieval.
    # + returns - Returns an error if encounters an error, returns nil otherwise.
    public extern function pause(TopicPartition[] partitions) returns error?;

    # Resume consumer retrieving messages from set of partitions which were paused earlier.
    #
    # + partitions - Set of partitions to resume messages retrieval.
    # + return - Returns an error if encounters an error, returns nil otherwise.
    public extern function resume(TopicPartition[] partitions) returns error?;

    # Returns partitions in which the consumer is paused retrieving messages.
    #
    # + return - Set of partitions paused from message retrieval if executes successfully, error otherwise.
    public extern function getPausedPartitions() returns TopicPartition[]|error;

    # Returns start offsets for given set of partitions.
    #
    # + partitions - Set of partitions to return start offsets.
    # + return - Start offsets for partitions if executes successfully, error otherwise.
    public extern function getBeginningOffsets(TopicPartition[] partitions) returns Offset[]|error;

    # Returns last offsets for given set of partitions.
    #
    # + partitions - Set of partitions to return last offsets.
    # + return - Last offsets for partitions if executes successfully, error otherwise.
    public extern function getEndOffsets(TopicPartition[] partitions) returns Offset[]|error;

};
