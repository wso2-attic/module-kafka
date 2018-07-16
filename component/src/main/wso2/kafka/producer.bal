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

documentation { value:"Struct which represents Kafka Producer configuration
    F{{bootstrapServers}} list of remote server endpoints
    F{{acks}} number of acknowledgments
    F{{compressionType}} compression type to be used for
    F{{clientID}} id to be used for server side logging
    F{{metricsRecordingLevel}} metrics recording level
    F{{metricReporterClasses}} metrics reporter classes
    F{{partitionerClass}} partitioner class to be used to select partition the message is sent
    F{{interceptorClasses}} interceptor classes to be used before sending records
    F{{transactionalID}} transactionalId to use for transactional delivery
    F{{bufferMemory}} total bytes of memory the producer can use to buffer records
    F{{noRetries}} number of retries to resend a record
    F{{batchSize}} number of records to be batched for a single request
    F{{linger}} delay to allow other records to be batched
    F{{sendBuffer}} size of the TCP send buffer (SO_SNDBUF)
    F{{receiveBuffer}} size of the TCP receive buffer (SO_RCVBUF)
    F{{maxRequestSize}} the maximum size of a request in bytes
    F{{reconnectBackoff}} time to wait before attempting to reconnect
    F{{reconnectBackoffMax}} maximum amount of time in milliseconds to wait when reconnecting
    F{{retryBackoff}} time to wait before attempting to retry a failed request
    F{{maxBlock}} max block time which the send is blocked if buffer is full
    F{{requestTimeout}} wait time for response of a request
    F{{metadataMaxAge}} max time to force a refresh of metadata
    F{{metricsSampleWindow}} window of time a metrics sample is computed over
    F{{metricsNumSamples}} number of samples maintained to compute metrics
    F{{maxInFlightRequestsPerConnection}} maximum number of unacknowledged requests on a single connection
    F{{connectionsMaxIdle}} close idle connections after the number of milliseconds
    F{{transactionTimeout}} timeout fro transaction status update from the producer
    F{{enableIdempotence}} exactly one copy of each message is written in the stream when enabled
}
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

documentation { Represent a Kafka producer endpoint
    E{{}}
    F{{producerActions}} handle all the actions related to the endpoint
    F{{producerConfig}} used to store configurations related to a Kafka connection
}
public type SimpleProducer object {

    public ProducerAction producerActions;
    public ProducerConfig producerConfig;

    documentation { Initialize the producer endpoint
        P{{config}} configurations related to the endpoint
    }
    public function init(ProducerConfig config) {
        self.producerConfig = config;
        self.producerActions.init(config);
    }

    documentation { Registers producer endpoint in the service
        P{{serviceType}} type descriptor of the service
    }
    public function register (typedesc serviceType) {

    }

    documentation { Starts the consumer endpoint }
    public function start () {}

    documentation { Returns the action object of ProducerAction }
    public function getCallerActions () returns (ProducerAction) {
        return producerActions;
    }

    documentation { Stops the consumer endpoint }
    public function stop () {
        self.producerActions.close();
    }

};

documentation { Kafka producer action handling object }
public type ProducerAction object {

    public map producerHolder;
    public string connectorID = system:uuid();

    documentation { value:"Simple Send action which produce records to Kafka server
        P{{value}} record contents
        P{{topic}} topic the record will be appended to
        P{{key}} key that will be included in the record
        P{{partition}} partition to which the record should be sent
        P{{timestamp}} timestamp of the record, in milliseconds since epoch
    }
    public native function send(byte[] value, string topic, byte[]? key = (), int? partition = (), int? timestamp = ());

    documentation { Flush action which flush batch of records }
    public native function flush();

    documentation { Close action which closes Kafka producer }
    public native function close();

    documentation { GetTopicPartitions action which returns given topic partition information
        P{{topic}} topic which partition information is given
        R{{}} partition for given topic
    }
    public native function getTopicPartitions(string topic) returns TopicPartition[];

    documentation { CommitConsumer action which commits consumer consumed offsets to offset topic
        P{{consumer}} consumer which needs offsets to be committed
    }
    public native function commitConsumer(ConsumerAction consumer);

    documentation { CommitConsumerOffsets action which commits consumer offsets in given transaction
        P{{offsets}} consumer offsets to commit for given transaction
        P{{groupID}} consumer group id
    }
    public native function commitConsumerOffsets(Offset[] offsets, string groupID);

    native function init(ProducerConfig config);

};

public type Producer object {};
