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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.BallerinaConnectorException;
import org.ballerinalang.connector.api.ParamDetail;
import org.ballerinalang.connector.api.Resource;
import org.ballerinalang.connector.api.Service;
import org.ballerinalang.model.types.BArrayType;
import org.ballerinalang.model.types.BObjectType;
import org.ballerinalang.model.types.BRecordType;
import org.ballerinalang.model.types.BTypes;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BError;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.util.codegen.ProgramFile;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_CONCURRENT_CONSUMERS;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_DECOUPLE_PROCESSING;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_POLLING_INTERVAL;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_POLLING_TIMEOUT;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_TOPICS;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_CONFIG_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_RECORD_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.DEFAULT_KEY_DESERIALIZER;
import static org.ballerinalang.kafka.util.KafkaConstants.DEFAULT_KEY_SERIALIZER;
import static org.ballerinalang.kafka.util.KafkaConstants.DEFAULT_VALUE_DESERIALIZER;
import static org.ballerinalang.kafka.util.KafkaConstants.DEFAULT_VALUE_SERIALIZER;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.OFFSET_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PROPERTIES_ARRAY;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;

/**
 * Utility class for Kafka Connector Implementation.
 */
public class KafkaUtils {

    public static Resource extractKafkaResource(Service service) throws BallerinaConnectorException {
        Resource[] resources = service.getResources();

        if (resources.length == 0) {
            throw new BallerinaException("No resources found to handle the Kafka records in " + service.getName());
        }

        if (resources.length > 1) {
            throw new BallerinaException("More than one resources found in Kafka service " + service.getName()
                    + ". Kafka Service should only have one resource");
        }

        Resource mainResource = resources[0];
        List<ParamDetail> paramDetails = mainResource.getParamDetails();

        if (paramDetails.size() == 0 || paramDetails.size() == 1) {
            throw new BallerinaException("Kafka resource signature does not comply with param standard sequence.");
        } else {
            validateConsumerParam(paramDetails.get(0));
            validateRecordsParam(paramDetails.get(1));
            if (paramDetails.size() > 2) {
                validateOffsetsParam(paramDetails.get(2));
                if (paramDetails.size() > 3) {
                    validateGroupIDParam(paramDetails.get(3));
                }

            }
        }
        return resources[0];
    }

    private static void validateConsumerParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.OBJECT_TYPE_TAG) {
            BObjectType type = (BObjectType) param.getVarType();
            if (type.getPackagePath().equals(KAFKA_NATIVE_PACKAGE) &&
                    type.getName().equals(CONSUMER_STRUCT_NAME)) {
                return;
            }
        }
        throw new BallerinaException("Resource signature validation failed for param at index: 0.");
    }

    private static void validateRecordsParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.ARRAY_TAG) {
            BArrayType array = (BArrayType) param.getVarType();
            if (array.getElementType().getTag() == TypeTags.RECORD_TYPE_TAG) {
                BRecordType type = (BRecordType) array.getElementType();
                if (type.getPackagePath().equals(KAFKA_NATIVE_PACKAGE) &&
                        type.getName().equals(CONSUMER_RECORD_STRUCT_NAME)) {
                    return;
                }

            }
        }
        throw new BallerinaException("Resource signature validation failed for param at index: 1.");
    }

    private static void validateOffsetsParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.ARRAY_TAG) {
            BArrayType array = (BArrayType) param.getVarType();
            if (array.getElementType().getTag() == TypeTags.RECORD_TYPE_TAG) {
                BRecordType type = (BRecordType) array.getElementType();
                if (type.getPackagePath().equals(KAFKA_NATIVE_PACKAGE) &&
                        type.getName().equals(OFFSET_STRUCT_NAME)) {
                    return;
                }
            }
        }
        throw new BallerinaException("Resource signature validation failed for param at index: 2.");
    }

    private static void validateGroupIDParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.STRING_TAG) {
            return;
        }
        throw new BallerinaException("Resource signature validation failed for param at index: 3.");
    }

    public static BValue[] getSignatureParameters(Resource resource,
                                                  ConsumerRecords<byte[], byte[]> records,
                                                  KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        List<ParamDetail> paramDetails = resource.getParamDetails();
        BValue[] bValues = new BValue[paramDetails.size()];
        if (paramDetails.size() > 0) {
            bValues[0] = createConsumerStruct(resource, kafkaConsumer);
            if (paramDetails.size() > 1) {
                bValues[1] = createRecordStructArray(resource, records);
            }
        }
        return bValues;
    }

    private static BValueArray createRecordStructArray(Resource resource,
                                                          ConsumerRecords<byte[], byte[]> records) {
        // Create records struct array.
        List<BMap<String, BValue>> recordsList = new ArrayList<>();
        ProgramFile programFile = resource.getResourceInfo().getPackageInfo().getProgramFile();

        records.forEach(record -> {
            BMap<String, BValue> recordStruct = BLangConnectorSPIUtil.createBStruct(programFile,
                    KAFKA_NATIVE_PACKAGE,
                    CONSUMER_RECORD_STRUCT_NAME);
            if (record.key() != null) {
                recordStruct.put("key", new BValueArray(record.key()));
            }
            recordStruct.put("value", new BValueArray(record.value()));
            recordStruct.put("offset", new BInteger(record.offset()));
            recordStruct.put("partition", new BInteger(record.partition()));
            recordStruct.put("timestamp", new BInteger(record.timestamp()));
            recordStruct.put("topic", new BString(record.topic()));
            recordsList.add(recordStruct);
        });

        BMap<String, BValue> consumerRecordStruct = BLangConnectorSPIUtil.createBStruct(programFile,
                KAFKA_NATIVE_PACKAGE,
                CONSUMER_RECORD_STRUCT_NAME);
        return new BValueArray(recordsList.toArray(new BRefType[0]), consumerRecordStruct.getType());
    }

    private static BMap<String, BValue> createConsumerStruct(Resource resource,
                                                             KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        // Create consumer struct.
        ProgramFile programFile = resource.getResourceInfo().getPackageInfo().getProgramFile();
        BMap<String, BValue> consumerStruct = BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
              CONSUMER_STRUCT_NAME);
        consumerStruct.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
        return consumerStruct;
    }

    private static BValueArray createOffsetStructArray(Resource resource,
                                                          ConsumerRecords<byte[], byte[]> records) {
        // Create offsets struct array.
        Map<TopicPartition, Long> partitionToUncommittedOffsetMap = new HashMap<>();
        records.forEach(record -> {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            partitionToUncommittedOffsetMap.put(tp, record.offset());
        });

        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> e : partitionToUncommittedOffsetMap.entrySet()) {
            partitionToMetadataMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
        }

        ProgramFile programFile = resource.getResourceInfo().getPackageInfo().getProgramFile();
        List<BMap<String, BValue>> offsetList = new ArrayList<>();
        partitionToMetadataMap.entrySet().forEach(offset -> {
            BMap<String, BValue> offsetStruct = BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
                    OFFSET_STRUCT_NAME);
            BMap<String, BValue> partitionStruct = BLangConnectorSPIUtil.createBStruct(programFile,
                    KAFKA_NATIVE_PACKAGE,
                    TOPIC_PARTITION_STRUCT_NAME);
            partitionStruct.put("topic", new BString(offset.getKey().topic()));
            partitionStruct.put("partition", new BInteger(offset.getKey().partition()));
            offsetStruct.put("partition", partitionStruct);
            offsetStruct.put("offset", new BInteger(offset.getValue().offset()));
            offsetList.add(offsetStruct);
        });

        return new BValueArray(offsetList.toArray(new BRefType[0]),
                BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
                        OFFSET_STRUCT_NAME).getType());
    }

    private static BMap<String, BValue> createConsumerStruct(Resource resource,
                                                             KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                                             String groupId) {
        // Create consumer struct.
        ProgramFile programFile = resource.getResourceInfo().getPackageInfo().getProgramFile();
        BMap<String, BValue> consumerStruct = BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
              CONSUMER_STRUCT_NAME);
        consumerStruct.addNativeData(NATIVE_CONSUMER, kafkaConsumer);

        BMap<String, BValue> consumerConfigStruct = BLangConnectorSPIUtil.createBStruct(
                programFile, KAFKA_NATIVE_PACKAGE, CONSUMER_CONFIG_STRUCT_NAME);

        consumerConfigStruct.put(KafkaConstants.CONSUMER_GROUP_ID_CONFIG, new BString(groupId));

        consumerStruct.put("config", consumerConfigStruct);
        return consumerStruct;
    }

    public static BValue[] getSignatureParameters(Resource resource,
                                                  ConsumerRecords<byte[], byte[]> records,
                                                  KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                                  String groupId) {
        List<ParamDetail> paramDetails = resource.getParamDetails();
        BValue[] bValues = new BValue[paramDetails.size()];
        if (paramDetails.size() > 0) {
            bValues[0] = createConsumerStruct(resource, kafkaConsumer, groupId);
            if (paramDetails.size() > 1) {
                bValues[1] = createRecordStructArray(resource, records);
                if (paramDetails.size() > 2) {
                    bValues[2] = createOffsetStructArray(resource, records);
                    if (paramDetails.size() > 3) {
                        if (groupId == null) {
                            bValues[3] = null;
                        } else {
                            bValues[3] = new BString(groupId);
                        }
                    }
                }
            }
        }

        return bValues;
    }

    public static Properties processKafkaConsumerConfig(BMap<String, BValue> bStruct) {
        Properties configParams = new Properties();

        addStringParamIfPresent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_BOOTSTRAP_SERVERS_CONFIG);
        addStringParamIfPresent(ConsumerConfig.GROUP_ID_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_GROUP_ID_CONFIG);
        addStringParamIfPresent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_AUTO_OFFSET_RESET_CONFIG);
        addStringParamIfPresent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
        addStringParamIfPresent(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_METRICS_RECORDING_LEVEL_CONFIG);
        addStringParamIfPresent(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_METRIC_REPORTER_CLASSES_CONFIG);
        addStringParamIfPresent(ConsumerConfig.CLIENT_ID_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_CLIENT_ID_CONFIG);
        addStringParamIfPresent(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_INTERCEPTOR_CLASSES_CONFIG);
        addStringParamIfPresent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_ISOLATION_LEVEL_CONFIG);

        addStringArrayParamIfPresent(ALIAS_TOPICS, bStruct, configParams, ALIAS_TOPICS);
        addStringArrayParamIfPresent(PROPERTIES_ARRAY, bStruct, configParams, PROPERTIES_ARRAY);

        addIntParamIfPresent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.METADATA_MAX_AGE_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_METADATA_MAX_AGE_CONFIG);
        addIntParamIfPresent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_AUTO_COMMIT_INTERVAL_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_MAX_PARTITION_FETCH_BYTES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.SEND_BUFFER_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_SEND_BUFFER_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RECEIVE_BUFFER_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_RECEIVE_BUFFER_CONFIG);
        addIntParamIfPresent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_FETCH_MIN_BYTES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_FETCH_MAX_BYTES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_FETCH_MAX_WAIT_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_RECONNECT_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_RETRY_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_METRICS_SAMPLE_WINDOW_MS_CONFIG);

        addIntParamIfPresent(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_METRICS_NUM_SAMPLES_CONFIG);
        addIntParamIfPresent(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_REQUEST_TIMEOUT_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_CONNECTIONS_MAX_IDLE_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_MAX_POLL_RECORDS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_MAX_POLL_INTERVAL_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        addIntParamIfPresent(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_DEFAULT_API_TIMEOUT_CONFIG);

        addIntParamIfPresent(ALIAS_POLLING_TIMEOUT, bStruct, configParams, ALIAS_POLLING_TIMEOUT);
        addIntParamIfPresent(ALIAS_POLLING_INTERVAL, bStruct, configParams, ALIAS_POLLING_INTERVAL);
        addIntParamIfPresent(ALIAS_CONCURRENT_CONSUMERS, bStruct, configParams, ALIAS_CONCURRENT_CONSUMERS);

        addBooleanParamIfPresent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, true);
        addBooleanParamIfPresent(ConsumerConfig.CHECK_CRCS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_CHECK_CRCS_CONFIG, true);
        addBooleanParamIfPresent(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, bStruct, configParams,
                KafkaConstants.CONSUMER_EXCLUDE_INTERNAL_TOPICS_CONFIG, true);

        addBooleanParamIfPresent(ALIAS_DECOUPLE_PROCESSING, bStruct, configParams,
                ALIAS_DECOUPLE_PROCESSING, false);

        processDefaultConsumerProperties(configParams);
        return configParams;
    }

    public static Properties processKafkaProducerConfig(BMap<String, BValue> bStruct) {
        Properties configParams = new Properties();

        if (bStruct == null) {
            processDefaultProducerProperties(configParams);
            return configParams;
        }

        addStringParamIfPresent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_BOOTSTRAP_SERVERS_CONFIG);
        addStringParamIfPresent(ProducerConfig.ACKS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_ACKS_CONFIG);
        addStringParamIfPresent(ProducerConfig.COMPRESSION_TYPE_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_COMPRESSION_TYPE_CONFIG);
        addStringParamIfPresent(ProducerConfig.CLIENT_ID_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_CLIENT_ID_CONFIG);
        addStringParamIfPresent(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_METRICS_RECORDING_LEVEL_CONFIG);
        addStringParamIfPresent(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_METRIC_REPORTER_CLASSES_CONFIG);
        addStringParamIfPresent(ProducerConfig.PARTITIONER_CLASS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_PARTITIONER_CLASS_CONFIG);
        addStringParamIfPresent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_INTERCEPTOR_CLASSES_CONFIG);
        addStringParamIfPresent(ProducerConfig.TRANSACTIONAL_ID_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_TRANSACTIONAL_ID_CONFIG);

        addIntParamIfPresent(ProducerConfig.BUFFER_MEMORY_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_BUFFER_MEMORY_CONFIG);
        addIntParamIfPresent(ProducerConfig.RETRIES_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_RETRIES_CONFIG);
        addIntParamIfPresent(ProducerConfig.BATCH_SIZE_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_BATCH_SIZE_CONFIG);
        addIntParamIfPresent(ProducerConfig.LINGER_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_LINGER_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.SEND_BUFFER_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_SEND_BUFFER_CONFIG);
        addIntParamIfPresent(ProducerConfig.RECEIVE_BUFFER_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_RECEIVE_BUFFER_CONFIG);
        addIntParamIfPresent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_MAX_REQUEST_SIZE_CONFIG);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_RECONNECT_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_RECONNECT_BACKOFF_MAX_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_RETRY_BACKOFF_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.MAX_BLOCK_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_MAX_BLOCK_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_REQUEST_TIMEOUT_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.METADATA_MAX_AGE_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_METADATA_MAX_AGE_CONFIG);
        addIntParamIfPresent(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_METRICS_SAMPLE_WINDOW_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_METRICS_NUM_SAMPLES_CONFIG);
        addIntParamIfPresent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, bStruct,
                configParams, KafkaConstants.PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        addIntParamIfPresent(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_CONNECTIONS_MAX_IDLE_MS_CONFIG);
        addIntParamIfPresent(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_TRANSACTION_TIMEOUT_CONFIG);

        addBooleanParamIfPresent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, bStruct,
                configParams, KafkaConstants.PRODUCER_ENABLE_IDEMPOTENCE_CONFIG, false);

        processDefaultProducerProperties(configParams);
        return configParams;
    }

    public static void processDefaultConsumerProperties(Properties configParams) {
        configParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
        configParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
    }

    public static void processDefaultProducerProperties(Properties configParams) {
        configParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
        configParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);
    }

    private static void addStringParamIfPresent(String paramName,
                                                BMap<String, BValue> bStruct,
                                                Properties configParams,
                                                String key) {
        if (Objects.nonNull(bStruct.get(key))) {
            String value = ((BString) bStruct.get(key)).value();
            if (!(value == null || value.equals(""))) {
                configParams.put(paramName, value);
            }
        }
    }

    private static void addStringArrayParamIfPresent(String paramName,
                                                     BMap<String, BValue> bStruct,
                                                     Properties configParams,
                                                     String key) {
        BValueArray bArray = (BValueArray) bStruct.get(key);
        List<String> values = new ArrayList<>();
        if (bArray != null && bArray.size() != 0) {
            for (int i = 0; i < bArray.size(); i++) {
                values.add(bArray.getString(i));
            }
            configParams.put(paramName, values);
        }
    }

    private static void addIntParamIfPresent(String paramName,
                                             BMap<String, BValue> bStruct,
                                             Properties configParams,
                                             String key) {
        long value = ((BInteger) bStruct.get(key)).intValue();
        if (value != -1) {
            configParams.put(paramName, new Long(value).intValue());
        }
    }

    private static void addBooleanParamIfPresent(String paramName,
                                                 BMap<String, BValue> bStruct,
                                                 Properties configParams,
                                                 String key,
                                                 boolean defaultValue) {
        boolean value = ((BBoolean) bStruct.get(key)).value();
        if (value != defaultValue) {
            configParams.put(paramName, value);
        }
    }

    public static BMap<String, BValue> createKafkaPackageStruct(Context context, String structName) {
        return BLangConnectorSPIUtil.createBStruct(context.getProgramFile(),
                KAFKA_NATIVE_PACKAGE,
                structName);
    }

    public static ArrayList<TopicPartition> getTopicPartitionList(BValueArray partitions) {
        ArrayList<TopicPartition> partitionList = new ArrayList<>();
        if (partitions != null) {
            for (int counter = 0; counter < partitions.size(); counter++) {
                BMap<String, BValue> partition = (BMap<String, BValue>) partitions.getRefValue(counter);
                String topic = partition.get("topic").stringValue();
                int partitionValue = ((BInteger) partition.get("partition")).value().intValue();
                partitionList.add(new TopicPartition(topic, partitionValue));
            }
        }
        return partitionList;
    }

    public static List<BMap<String, BValue>> createPartitionList(Context context,
                                                                 Collection<TopicPartition> partitions) {

        List<BMap<String, BValue>> topicPartitionList = new ArrayList<>();
        if (!partitions.isEmpty()) {
            partitions.forEach(assignment -> {
                BMap<String, BValue> partitionStruct = getTopicPartitionStruct(context, assignment);
                topicPartitionList.add(partitionStruct);
            });
        }
        return topicPartitionList;
    }

    public static BMap<String, BValue> getTopicPartitionStruct(Context context, TopicPartition topicPartition) {
        BMap<String, BValue> partitionStruct = KafkaUtils
                .createKafkaPackageStruct(context, TOPIC_PARTITION_STRUCT_NAME);
        partitionStruct.put("topic", new BString(topicPartition.topic()));
        partitionStruct.put("partition", new BInteger(topicPartition.partition()));
        return partitionStruct;
    }

    public static BError createError(Context context, String errorCode, String errorMessage) {
        BMap<String, BValue> error = BLangConnectorSPIUtil.createBStruct(context, PACKAGE_NAME, "KafkaError");
        error.put("message", new BString(errorMessage));
        return BLangVMErrors.createError(context, true, BTypes.typeError, errorCode, error);
    }

    public static BError createError(Context context, String errorMessage) {
        return BLangVMErrors.createError(context, errorMessage);
    }
}
