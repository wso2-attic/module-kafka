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

documentation { Configuration related to consumer endpoint
    F{{bootstrapServers}} list of remote server endpoints
    F{{groupId}} unique string that identifies the consumer
    F{{offsetReset}} offset reset strategy if no initial offset
    F{{partitionAssignmentStrategy}} strategy class for handle partition assignment among consumers
    F{{metricsRecordingLevel}} metrics recording level
    F{{metricsReporterClasses}} metrics reporter classes
    F{{clientId}} id to be used for server side logging
    F{{interceptorClasses}} interceptor classes to be used before sending records
    F{{isolationLevel}} how the transactional messages are read
    F{{topics}} topics to be subscribed
    F{{properties}} additional properties if required
    F{{sessionTimeout}} timeout used to detect consumer failures when heartbeat threshold is reached
    F{{heartBeatInterval}} expected time between heartbeats
    F{{metadataMaxAge}} max time to force a refresh of metadata
    F{{autoCommitInterval}} offset committing interval
    F{{maxPartitionFetchBytes}} the max amount of data per-partition the server return
    F{{sendBuffer}} size of the TCP send buffer (SO_SNDBUF)
    F{{receiveBuffer}} size of the TCP receive buffer (SO_RCVBUF)
    F{{fetchMinBytes}} minimum amount of data the server should return for a fetch request
    F{{fetchMaxBytes}} maximum amount of data the server should return for a fetch request
    F{{fetchMaxWait}} maximum amount of time the server will block before answering the fetch request
    F{{reconnectBackoffMax}} maximum amount of time in milliseconds to wait when reconnecting
    F{{retryBackoff}} time to wait before attempting to retry a failed request
    F{{metricsSampleWindow}} window of time a metrics sample is computed over
    F{{metricsNumSamples}} number of samples maintained to compute metrics
    F{{requestTimeout}} wait time for response of a request
    F{{connectionMaxIdle}} close idle connections after the number of milliseconds
    F{{maxPollRecords}} maximum number of records returned in a single call to poll
    F{{maxPollInterval}} maximum delay between invocations of poll
    F{{reconnectBackoff}} time to wait before attempting to reconnect
    F{{pollingTimeout}} time out interval of polling
    F{{pollingInterval}} polling interval
    F{{concurrentConsumers}} number of concurrent consumers
    F{{autoCommit}} enables auto commit offsets
    F{{checkCRCS}} check the CRC32 of the records consumed
    F{{excludeInternalTopics}} whether records from internal topics should be exposed to the consumer
    F{{decoupleProcessing}} decouples processing
}
public type ConsumerConfig {
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
};

documentation { Type related to consumer record
    F{{key}} key that is included in the record
    F{{value}} record content
    F{{offset}} offset value
    F{{partition}} partition to which the record is stored
    F{{timestamp}} timestamp of the record, in milliseconds since epoch
    F{{topic}} topic of the record
}
public type ConsumerRecord {
    blob key;
    blob value;
    int offset;
    int partition;
    int timestamp;
    string topic;
};

documentation { Kafka consumer service object }
public type Consumer object {

    documentation { Returns the endpoint bound to service
        R{{SimpleConsumer}} Kafka consumer endpoint bound to the service
    }
    public function getEndpoint() returns SimpleConsumer {
        SimpleConsumer consumer = new();
        return consumer;
    }
};

documentation { Represent a Kafka consumer endpoint
    E{{}}
    F{{consumerActions}} handle all the actions related to the endpoint
    F{{consumerConfig}} used to store configurations related to a Kafka connection
}
public type SimpleConsumer object {

    public {
        ConsumerAction consumerActions;
        ConsumerConfig consumerConfig;
    }

    documentation { Initialize the consumer endpoint
        P{{config}} configurations related to the endpoint
    }
    public function init(ConsumerConfig config) {
        self.consumerConfig = config;
        self.consumerActions.config = config;
        self.initEndpoint();
    }

    documentation { Registers consumer endpoint in the service
        P{{serviceType}} type descriptor of the service
    }
    public function register(typedesc serviceType) {
        self.registerListener(serviceType);
    }

    documentation { Starts the consumer endpoint }
    public function start() {}

    documentation { Returns the action object of ConsumerAction }
    public function getCallerActions() returns ConsumerAction {
        return consumerActions;
    }

    documentation { Stops the consumer endpoint }
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

    native function registerListener(typedesc serviceType);

};

documentation { Kafka consumer action handling object }
public type ConsumerAction object {

    public {
        ConsumerConfig config;
    }

    native function connect() returns error?;

    documentation { Subscribes to consumer to external Kafka broker topic pattern
        P{{regex}} topic pattern to be subscribed
    }
    public native function subscribeToPattern(string regex) returns error?;

    documentation { Subscribes to consumer to external Kafka broker topic array
        P{{topics}}tTopic array to be subscribed
    }
    public native function subscribe(string[] topics) returns error?;

    documentation { Subscribes to consumer to external Kafka broker topic with rebalance listening is enabled
        P{{topics}} topic array to be subscribed
        P{{onPartitionsRevoked}} function will be executed if partitions are revoked from this consumer
        P{{onPartitionsAssigned}} function will be executed if partitions are assigned this consumer
    }
    public native function subscribeWithPartitionRebalance(string[] topics,
    function(ConsumerAction consumerActions, TopicPartition[] partitions) onPartitionsRevoked,
    function(ConsumerAction consumerActions, TopicPartition[] partitions) onPartitionsAssigned) returns error?;

    documentation { Assign consumer to external Kafka broker set of topic partitions
        P{{partitions}} topic partitions to be assigned
    }
    public native function assign(TopicPartition[] partitions) returns error?;

    documentation { Returns current offset position in which consumer is at
        P{{partition}} topic partitions in which the position is required
        R{{}} position in which the consumer is at in given Topic partition
    }
    public native function getPositionOffset(TopicPartition partition) returns int|error;

    documentation { Returns current assignment of partitions for a consumer
        R{{}} Assigned partitions array for consumer
    }
    public native function getAssignment() returns TopicPartition[]|error;

    documentation { Returns current subscription of topics for a consumer
        R{{}} subscribed topic array for consumer
    }
    public native function getSubscription() returns string[]|error;

    documentation { Returns current subscription of topics for a consumer
        P{{partition}} partition in which offset is returned for consumer
        R{{}} committed offset for consumer for given partition
    }
    public native function getCommittedOffset(TopicPartition partition) returns Offset|error;

    documentation { Poll the consumer for external broker for records
        P{{timeoutValue}} polling time in milliseconds
        R{{}} consumer record array
    }
    public native function poll(int timeoutValue) returns ConsumerRecord[]|error;

    documentation { Commits current consumed offsets for consumer }
    public native function commit();

    documentation { Commits given offsets for consumer
        P{{offsets}} offsets to be commited
    }
    public native function commitOffset(Offset[] offsets);

    documentation { Seek consumer for given offset in a topic partition
        P{{offset}} given offset to seek
    }
    public native function seek(Offset offset) returns error?;

    documentation { Seek consumer for beginning offsets for set of topic partitions
        P{{partitions}} set of partitions to seek
    }
    public native function seekToBeginning(TopicPartition[] partitions) returns error?;

    documentation { Seek consumer for end offsets for set of topic partitions
        P{{partitions}} set of partitions to seek
    }
    public native function seekToEnd(TopicPartition[] partitions) returns error?;

    documentation { Retrieve the set of partitions which topic belongs
        P{{topic}} given topic for partition information is needed
        R{{}} partition array for given topic
    }
    public native function getTopicPartitions(string topic) returns TopicPartition[]|error;

    documentation { Un-subscribe consumer from all external broaker topic subscription }
    public native function unsubscribe() returns error?;

    documentation { Closes consumer connection to external Kafka broker }
    public native function close() returns error?;

    documentation { Pause consumer retrieving messages from set of partitions
        P{{partitions}} Set of partitions to pause messages retrieval
    }
    public native function pause(TopicPartition[] partitions) returns error?;

    documentation { Resume consumer retrieving messages from set of partitions which were paused earlier
        P{{partitions}} Set of partitions to resume messages retrieval
    }
    public native function resume(TopicPartition[] partitions) returns error?;

    documentation { Returns partitions in which the consumer is paused retrieving messages
        R{{}} Set of partitions paused from message retrieval
    }
    public native function getPausedPartitions() returns TopicPartition[]|error;

    documentation { Returns start offsets for given set of partitions
        P{{partitions}} Set of partitions to return start offsets
        R{{}} Start offsets for partitions
    }
    public native function getBeginningOffsets(TopicPartition[] partitions) returns Offset[]|error;

    documentation { Returns last offsets for given set of partitions
        P{{partitions}} Set of partitions to return last offsets
        R{{}} Last offsets for partitions
    }
    public native function getEndOffsets(TopicPartition[] partitions) returns Offset[]|error;

};
