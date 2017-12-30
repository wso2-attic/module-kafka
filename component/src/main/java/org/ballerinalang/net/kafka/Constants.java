/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.net.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Constants for kafka.
 */
public class Constants {

    private Constants() {
    }

    public static final String PRODUCER_CONNECTOR_NAME = "KafkaProducerClient";
    public static final String NATIVE_CONSUMER = "KafkaConsumer";
    public static final String NATIVE_PRODUCER = "KafkaProducer";
    public static final String KAFKA_NATIVE_PACKAGE = "ballerina.net.kafka";
    public static final String TOPIC_PARTITION_STRUCT_NAME = "TopicPartition";
    public static final String OFFSET_STRUCT_NAME = "Offset";

    public static final String CONSUMER_RECORD_STRUCT_NAME = "ConsumerRecord";
    public static final String CONSUMER_STRUCT_NAME = "KafkaConsumer";
    public static final String PRODUCER_STRUCT_NAME = "KafkaProducer";
    public static final String ANNOTATION_KAFKA_CONFIGURATION = "configuration";
    public static final String PROPERTIES_ARRAY = "properties";

    public static final String ALIAS_BOOTSTRAP_SERVERS_CONFIG = "bootstrapServers";
    public static final String ALIAS_GROUP_ID_CONFIG = "groupId";
    public static final String ALIAS_CONCURRENT_CONSUMERS = "concurrentConsumers";
    public static final String ALIAS_TOPICS = "topics";
    public static final String ALIAS_POLLING_TIMEOUT = "pollingTimeout";
    public static final String ALIAS_POLLING_INTERVAL = "pollingInterval";
    public static final String ALIAS_DECOUPLE_PROCESSING = "decoupleProcessing";
    public static final String ALIAS_ENABLE_AUTO_COMMIT_CONFIG = "autoCommit";

    public static final String ALIAS_AUTO_OFFSET_RESET_CONFIG = "offsetReset";
    public static final String ALIAS_SESSION_TIMEOUT_MS_CONFIG = "sessionTimeout";
    public static final String ALIAS_HEARTBEAT_INTERVAL_MS_CONFIG = "heartBeatInterval";
    public static final String ALIAS_PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partitionAssignmentStrategy";

    public static final String ALIAS_METADATA_MAX_AGE_CONFIG = "metadataMaxAge";
    public static final String ALIAS_AUTO_COMMIT_INTERVAL_MS_CONFIG = "autoCommitInterval";
    public static final String ALIAS_CLIENT_ID_CONFIG = "clientId";
    public static final String ALIAS_MAX_PARTITION_FETCH_BYTES_CONFIG = "maxPartitionFetchBytes";

    public static final String ALIAS_SEND_BUFFER_CONFIG = "sendBuffer";
    public static final String ALIAS_RECEIVE_BUFFER_CONFIG = "receiveBuffer";
    public static final String ALIAS_FETCH_MIN_BYTES_CONFIG = "fetchMinBytes";
    public static final String ALIAS_FETCH_MAX_BYTES_CONFIG = "fetchMaxBytes";

    public static final String ALIAS_FETCH_MAX_WAIT_MS_CONFIG = "fetchMaxWait";
    public static final String ALIAS_RECONNECT_BACKOFF_MS_CONFIG = "reconnectBackoff";
    public static final String ALIAS_RETRY_BACKOFF_MS_CONFIG = "retryBackoff";
    public static final String ALIAS_CHECK_CRCS_CONFIG = "checkCRCS";

    public static final String ALIAS_METRICS_SAMPLE_WINDOW_MS_CONFIG = "metricsSampleWindow";
    public static final String ALIAS_METRICS_NUM_SAMPLES_CONFIG = "metricsNumSamples";
    public static final String ALIAS_METRICS_RECORDING_LEVEL_CONFIG = "metricsRecordingLevel";
    public static final String ALIAS_METRIC_REPORTER_CLASSES_CONFIG = "metricsReporterClasses";

    public static final String ALIAS_REQUEST_TIMEOUT_MS_CONFIG = "requestTimeout";
    public static final String ALIAS_CONNECTIONS_MAX_IDLE_MS_CONFIG = "connectionMaxIdle";
    public static final String ALIAS_INTERCEPTOR_CLASSES_CONFIG = "interceptorClasses";
    public static final String ALIAS_MAX_POLL_RECORDS_CONFIG = "maxPollRecords";

    public static final String ALIAS_MAX_POLL_INTERVAL_MS_CONFIG = "maxPollInterval";
    public static final String ALIAS_EXCLUDE_INTERNAL_TOPICS_CONFIG = "excludeInternalTopics";
    public static final String ALIAS_ISOLATION_LEVEL_CONFIG = "isolationLevel";

    public static final String DEFAULT_KEY_DESERIALIZER
            = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String DEFAULT_VALUE_DESERIALIZER
            = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String PARAM_TRANSACTION_ID = "transactional.id";

    public static final String DEFAULT_KEY_SERIALIZER
            = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_VALUE_SERIALIZER
            = "org.apache.kafka.common.serialization.ByteArraySerializer";

    private static Map<String, String> mappingParameters;

    static {
        mappingParameters = new HashMap<>();
        mappingParameters.put(ALIAS_BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        mappingParameters.put(ALIAS_GROUP_ID_CONFIG, ConsumerConfig.GROUP_ID_CONFIG);
        mappingParameters.put(ALIAS_ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        mappingParameters.put(ALIAS_AUTO_OFFSET_RESET_CONFIG, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        mappingParameters.put(ALIAS_SESSION_TIMEOUT_MS_CONFIG, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        mappingParameters.put(ALIAS_HEARTBEAT_INTERVAL_MS_CONFIG, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        mappingParameters.put(ALIAS_PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);

        mappingParameters.put(ALIAS_METADATA_MAX_AGE_CONFIG, ConsumerConfig.METADATA_MAX_AGE_CONFIG);
        mappingParameters.put(ALIAS_AUTO_COMMIT_INTERVAL_MS_CONFIG, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
        mappingParameters.put(ALIAS_CLIENT_ID_CONFIG, ConsumerConfig.CLIENT_ID_CONFIG);
        mappingParameters.put(ALIAS_MAX_PARTITION_FETCH_BYTES_CONFIG, ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);

        mappingParameters.put(ALIAS_SEND_BUFFER_CONFIG, ConsumerConfig.SEND_BUFFER_CONFIG);
        mappingParameters.put(ALIAS_RECEIVE_BUFFER_CONFIG, ConsumerConfig.RECEIVE_BUFFER_CONFIG);
        mappingParameters.put(ALIAS_FETCH_MIN_BYTES_CONFIG, ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        mappingParameters.put(ALIAS_FETCH_MAX_BYTES_CONFIG, ConsumerConfig.FETCH_MAX_BYTES_CONFIG);

        mappingParameters.put(ALIAS_FETCH_MAX_WAIT_MS_CONFIG, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        mappingParameters.put(ALIAS_RECONNECT_BACKOFF_MS_CONFIG, ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG);
        mappingParameters.put(ALIAS_RETRY_BACKOFF_MS_CONFIG, ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        mappingParameters.put(ALIAS_CHECK_CRCS_CONFIG, ConsumerConfig.CHECK_CRCS_CONFIG);

        mappingParameters.put(ALIAS_METRICS_SAMPLE_WINDOW_MS_CONFIG, ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
        mappingParameters.put(ALIAS_METRICS_NUM_SAMPLES_CONFIG, ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG);
        mappingParameters.put(ALIAS_METRICS_RECORDING_LEVEL_CONFIG, ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG);
        mappingParameters.put(ALIAS_METRIC_REPORTER_CLASSES_CONFIG, ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG);

        mappingParameters.put(ALIAS_REQUEST_TIMEOUT_MS_CONFIG, ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        mappingParameters.put(ALIAS_CONNECTIONS_MAX_IDLE_MS_CONFIG, ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG);
        mappingParameters.put(ALIAS_INTERCEPTOR_CLASSES_CONFIG, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
        mappingParameters.put(ALIAS_MAX_POLL_RECORDS_CONFIG, ConsumerConfig.MAX_POLL_RECORDS_CONFIG);

        mappingParameters.put(ALIAS_MAX_POLL_INTERVAL_MS_CONFIG, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        mappingParameters.put(ALIAS_EXCLUDE_INTERNAL_TOPICS_CONFIG, ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG);
        mappingParameters.put(ALIAS_ISOLATION_LEVEL_CONFIG, ConsumerConfig.ISOLATION_LEVEL_CONFIG);
    }

    public static final Map<String, String> MAPPING_PARAMETERS = Collections.unmodifiableMap(mappingParameters);

}

