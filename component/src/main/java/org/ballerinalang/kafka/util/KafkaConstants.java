/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.kafka.util;

/**
 * Constants related to for Kafka API.
 */
public class KafkaConstants {

    private KafkaConstants() {
    }

    public static final String PATH_SEPARATOR = "/";
    public static final String BLOCK_SEPARATOR = ":";
    public static final String ARRAY_INDICATOR = "[]";

    public static final String ORG_NAME = "wso2";
    public static final String PACKAGE_NAME = "kafka";
    public static final String VERSION = "0.0.0";
    public static final String FULL_PACKAGE_NAME = PACKAGE_NAME + BLOCK_SEPARATOR + VERSION;
    public static final String KAFKA_NATIVE_PACKAGE = ORG_NAME + PATH_SEPARATOR + FULL_PACKAGE_NAME;

    public static final String NATIVE_CONSUMER = "KafkaConsumer";
    public static final String NATIVE_PRODUCER = "KafkaProducer";
    public static final String NATIVE_PRODUCER_CONFIG = "KafkaProducerConfig";

    public static final String TOPIC_PARTITION_STRUCT_NAME = "TopicPartition";
    public static final String OFFSET_STRUCT_NAME = "PartitionOffset";

    public static final String CONSUMER_RECORD_STRUCT_NAME = "ConsumerRecord";
    public static final String CONSUMER_STRUCT_NAME = "Listener";
    public static final String CONSUMER_CONFIG_STRUCT_NAME = "ConsumerConfig";
    public static final String CONSUMER_SERVER_CONNECTOR_NAME = "serverConnector";

    public static final String PARAMETER_CONSUMER_NAME = PACKAGE_NAME + BLOCK_SEPARATOR + CONSUMER_STRUCT_NAME;
    public static final String PARAMETER_RECORD_ARRAY_NAME =
            PACKAGE_NAME + BLOCK_SEPARATOR + CONSUMER_RECORD_STRUCT_NAME + ARRAY_INDICATOR;
    public static final String PARAMETER_PARTITION_OFFSET_ARRAY_NAME =
            PACKAGE_NAME + BLOCK_SEPARATOR + OFFSET_STRUCT_NAME + ARRAY_INDICATOR;

    public static final String KAFKA_RESOURCE_ON_MESSAGE = "onMessage";

    public static final String PRODUCER_STRUCT_NAME = "Producer";

    public static final String PROPERTIES_ARRAY = "properties";

    public static final String ALIAS_CONCURRENT_CONSUMERS = "concurrentConsumers";
    public static final String ALIAS_TOPICS = "topics";
    public static final String ALIAS_POLLING_TIMEOUT = "pollingTimeout";
    public static final String ALIAS_POLLING_INTERVAL = "pollingInterval";
    public static final String ALIAS_DECOUPLE_PROCESSING = "decoupleProcessing";
    public static final String ALIAS_TOPIC = "topic";
    public static final String ALIAS_PARTITION = "partition";
    public static final String ALIAS_OFFSET = "offset";

    public static final String DEFAULT_KEY_DESERIALIZER
            = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String DEFAULT_VALUE_DESERIALIZER
            = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    public static final String DEFAULT_KEY_SERIALIZER
            = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_VALUE_SERIALIZER
            = "org.apache.kafka.common.serialization.ByteArraySerializer";

    // Consumer Configuration.
    public static final String CONSUMER_BOOTSTRAP_SERVERS_CONFIG = "bootstrapServers";
    public static final String CONSUMER_GROUP_ID_CONFIG = "groupId";
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG = "offsetReset";
    public static final String CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partitionAssignmentStrategy";
    public static final String CONSUMER_METRICS_RECORDING_LEVEL_CONFIG = "metricsRecordingLevel";
    public static final String CONSUMER_METRIC_REPORTER_CLASSES_CONFIG = "metricsReporterClasses";
    public static final String CONSUMER_CLIENT_ID_CONFIG = "clientId";
    public static final String CONSUMER_INTERCEPTOR_CLASSES_CONFIG = "interceptorClasses";
    public static final String CONSUMER_ISOLATION_LEVEL_CONFIG = "isolationLevel";
    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "sessionTimeout";
    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "heartBeatInterval";
    public static final String CONSUMER_METADATA_MAX_AGE_CONFIG = "metadataMaxAge";
    public static final String CONSUMER_AUTO_COMMIT_INTERVAL_MS_CONFIG = "autoCommitInterval";
    public static final String CONSUMER_MAX_PARTITION_FETCH_BYTES_CONFIG = "maxPartitionFetchBytes";
    public static final String CONSUMER_SEND_BUFFER_CONFIG = "sendBuffer";
    public static final String CONSUMER_RECEIVE_BUFFER_CONFIG = "receiveBuffer";
    public static final String CONSUMER_FETCH_MIN_BYTES_CONFIG = "fetchMinBytes";
    public static final String CONSUMER_FETCH_MAX_BYTES_CONFIG = "fetchMaxBytes";
    public static final String CONSUMER_FETCH_MAX_WAIT_MS_CONFIG = "fetchMaxWait";
    public static final String CONSUMER_RECONNECT_BACKOFF_MS_CONFIG = "reconnectBackoff";
    public static final String CONSUMER_RETRY_BACKOFF_MS_CONFIG = "retryBackoff";
    public static final String CONSUMER_METRICS_SAMPLE_WINDOW_MS_CONFIG = "metricsSampleWindow";
    public static final String CONSUMER_METRICS_NUM_SAMPLES_CONFIG = "metricsNumSamples";
    public static final String CONSUMER_REQUEST_TIMEOUT_MS_CONFIG = "requestTimeout";
    public static final String CONSUMER_CONNECTIONS_MAX_IDLE_MS_CONFIG = "connectionMaxIdle";
    public static final String CONSUMER_MAX_POLL_RECORDS_CONFIG = "maxPollRecords";
    public static final String CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG = "maxPollInterval";
    public static final String CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnectBackoffMax";
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_CONFIG = "autoCommit";
    public static final String CONSUMER_CHECK_CRCS_CONFIG = "checkCRCS";
    public static final String CONSUMER_EXCLUDE_INTERNAL_TOPICS_CONFIG = "excludeInternalTopics";
    public static final String CONSUMER_DEFAULT_API_TIMEOUT_CONFIG = "defaultApiTimeout";

    // Producer Configuration.
    public static final String PRODUCER_BOOTSTRAP_SERVERS_CONFIG = "bootstrapServers";
    public static final String PRODUCER_ACKS_CONFIG = "acks";
    public static final String PRODUCER_COMPRESSION_TYPE_CONFIG = "compressionType";
    public static final String PRODUCER_CLIENT_ID_CONFIG = "clientID";
    public static final String PRODUCER_METRICS_RECORDING_LEVEL_CONFIG = "metricsRecordingLevel";
    public static final String PRODUCER_METRIC_REPORTER_CLASSES_CONFIG = "metricReporterClasses";
    public static final String PRODUCER_PARTITIONER_CLASS_CONFIG = "partitionerClass";
    public static final String PRODUCER_INTERCEPTOR_CLASSES_CONFIG = "interceptorClasses";
    public static final String PRODUCER_TRANSACTIONAL_ID_CONFIG = "transactionalID";
    public static final String PRODUCER_BUFFER_MEMORY_CONFIG = "bufferMemory";
    public static final String PRODUCER_RETRIES_CONFIG = "noRetries";
    public static final String PRODUCER_BATCH_SIZE_CONFIG = "batchSize";
    public static final String PRODUCER_LINGER_MS_CONFIG = "linger";
    public static final String PRODUCER_SEND_BUFFER_CONFIG = "sendBuffer";
    public static final String PRODUCER_RECEIVE_BUFFER_CONFIG = "receiveBuffer";
    public static final String PRODUCER_MAX_REQUEST_SIZE_CONFIG = "maxRequestSize";
    public static final String PRODUCER_RECONNECT_BACKOFF_MS_CONFIG = "reconnectBackoff";
    public static final String PRODUCER_RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnectBackoffMax";
    public static final String PRODUCER_RETRY_BACKOFF_MS_CONFIG = "retryBackoff";
    public static final String PRODUCER_MAX_BLOCK_MS_CONFIG = "maxBlock";
    public static final String PRODUCER_REQUEST_TIMEOUT_MS_CONFIG = "requestTimeout";
    public static final String PRODUCER_METADATA_MAX_AGE_CONFIG = "metadataMaxAge";
    public static final String PRODUCER_METRICS_SAMPLE_WINDOW_MS_CONFIG = "metricsSampleWindow";
    public static final String PRODUCER_METRICS_NUM_SAMPLES_CONFIG = "metricsNumSamples";
    public static final String PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "maxInFlightRequestsPerConnection";
    public static final String PRODUCER_CONNECTIONS_MAX_IDLE_MS_CONFIG = "connectionsMaxIdle";
    public static final String PRODUCER_TRANSACTION_TIMEOUT_CONFIG = "transactionTimeout";
    public static final String PRODUCER_ENABLE_IDEMPOTENCE_CONFIG = "enableIdempotence";


}
