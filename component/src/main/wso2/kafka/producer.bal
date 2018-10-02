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

import ballerina/system;

# Struct which represents Kafka Producer configuration.
#
# + bootstrapServers - list of remote server endpoints
# + acks - number of acknowledgments
# + compressionType - compression type to be used for
# + clientID - id to be used for server side logging
# + metricsRecordingLevel - metrics recording level
# + metricReporterClasses - metrics reporter classes
# + partitionerClass - partitioner class to be used to select partition the message is sent
# + interceptorClasses - interceptor classes to be used before sending records
# + transactionalID - transactionalId to use for transactional delivery
# + bufferMemory - total bytes of memory the producer can use to buffer records
# + noRetries - number of retries to resend a record
# + batchSize - number of records to be batched for a single request
# + linger - delay to allow other records to be batched
# + sendBuffer - size of the TCP send buffer (SO_SNDBUF)
# + receiveBuffer - size of the TCP receive buffer (SO_RCVBUF)
# + maxRequestSize - the maximum size of a request in bytes
# + reconnectBackoff - time to wait before attempting to reconnect
# + reconnectBackoffMax - maximum amount of time in milliseconds to wait when reconnecting
# + retryBackoff - time to wait before attempting to retry a failed request
# + maxBlock - max block time which the send is blocked if buffer is full
# + requestTimeout - wait time for response of a request
# + metadataMaxAge - max time to force a refresh of metadata
# + metricsSampleWindow - window of time a metrics sample is computed over
# + metricsNumSamples - number of samples maintained to compute metrics
# + maxInFlightRequestsPerConnection - maximum number of unacknowledged requests on a single connection
# + connectionsMaxIdle - close idle connections after the number of milliseconds
# + transactionTimeout - timeout fro transaction status update from the producer
# + enableIdempotence - exactly one copy of each message is written in the stream when enabled
public type ProducerConfig record {
    string? bootstrapServers; // BOOTSTRAP_SERVERS_CONFIG 0
    string? acks; // ACKS_CONFIG 1
    string? compressionType; // COMPRESSION_TYPE_CONFIG 2
    string? clientID; // CLIENT_ID_CONFIG 3
    string? metricsRecordingLevel; // METRICS_RECORDING_LEVEL_CONFIG 4
    string? metricReporterClasses; // METRIC_REPORTER_CLASSES_CONFIG 5
    string? partitionerClass; // PARTITIONER_CLASS_CONFIG 6
    string? interceptorClasses; // INTERCEPTOR_CLASSES_CONFIG 7
    string? transactionalID; // TRANSACTIONAL_ID_CONFIG 8

    int bufferMemory = -1; // BUFFER_MEMORY_CONFIG 0
    int noRetries = -1; // RETRIES_CONFIG 1
    int batchSize = -1; // BATCH_SIZE_CONFIG 2
    int linger = -1; // LINGER_MS_CONFIG 3
    int sendBuffer = -1; // SEND_BUFFER_CONFIG 4
    int receiveBuffer = -1; // RECEIVE_BUFFER_CONFIG 5
    int maxRequestSize = -1; // MAX_REQUEST_SIZE_CONFIG 6
    int reconnectBackoff = -1; // RECONNECT_BACKOFF_MS_CONFIG 7
    int reconnectBackoffMax = -1; // RECONNECT_BACKOFF_MAX_MS_CONFIG  8
    int retryBackoff = -1; // RETRY_BACKOFF_MS_CONFIG 9
    int maxBlock = -1; // MAX_BLOCK_MS_CONFIG 10
    int requestTimeout = -1; // REQUEST_TIMEOUT_MS_CONFIG  11
    int metadataMaxAge = -1; // METADATA_MAX_AGE_CONFIG 12
    int metricsSampleWindow = -1; // METRICS_SAMPLE_WINDOW_MS_CONFIG 13
    int metricsNumSamples = -1; // METRICS_NUM_SAMPLES_CONFIG  14
    int maxInFlightRequestsPerConnection = -1; // MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION 15
    int connectionsMaxIdle = -1; // CONNECTIONS_MAX_IDLE_MS_CONFIG 16
    int transactionTimeout = -1; // TRANSACTION_TIMEOUT_CONFIG 17

    boolean enableIdempotence = false;          // ENABLE_IDEMPOTENCE_CONFIG 0
};

# Represent a Kafka producer endpoint.
#
# + producerActions - handle all the actions related to the endpoint
# + producerConfig - used to store configurations related to a Kafka connection
public type SimpleProducer object {

    public ProducerAction producerActions;
    public ProducerConfig producerConfig;

    # Initialize the producer endpoint.
    #
    # + config - configurations related to the endpoint.
    public function init(ProducerConfig config) {
        self.producerConfig = config;
        self.producerActions.init(config);
    }

    # Registers producer endpoint in the service.
    #
    # + serviceType - type descriptor of the service.
    public function register (typedesc serviceType) {

    }

    # Starts the consumer endpoint.
    public function start () {}

    # Returns the action object of ProducerAction.
    #
    # + return - Producer actions.
    public function getCallerActions () returns ProducerAction {
        return producerActions;
    }

    # Stops the consumer endpoint.
    public function stop () {
        self.producerActions.close();
    }

};

# Kafka producer action handling object.
#
# + producerHolder - List of producers available.
# + connectorID - Unique ID for a particular connector.
public type ProducerAction object {

    public map producerHolder;
    public string connectorID = system:uuid();

    # Simple Send action which produce records to Kafka server.
    #
    # + value - record contents.
    # + topic - topic the record will be appended to.
    # + key - key that will be included in the record.
    # + partition - partition to which the record should be sent.
    # + timestamp - timestamp of the record, in milliseconds since epoch.
    public extern function send(byte[] value, string topic, byte[]? key = (), int? partition = (), int? timestamp = ());

    # Flush action which flush batch of records.
    public extern function flush();

    # Close action which closes Kafka producer.
    public extern function close();

    # GetTopicPartitions action which returns given topic partition information.
    # + topic - topic which partition information is given.
    # + return - partition for given topic.
    public extern function getTopicPartitions(string topic) returns TopicPartition[];

    # CommitConsumer action which commits consumer consumed offsets to offset topic.
    # + consumer - consumer which needs offsets to be committed.
    public extern function commitConsumer(ConsumerAction consumer);

    # CommitConsumerOffsets action which commits consumer offsets in given transaction.
    # + offsets - consumer offsets to commit for given transaction.
    # + groupID - consumer group id.
    public extern function commitConsumerOffsets(Offset[] offsets, string groupID);

    extern function init(ProducerConfig config);

};

public type Producer object {};
