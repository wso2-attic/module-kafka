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

package org.ballerinalang.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.connector.api.BallerinaConnectorException;
import org.ballerinalang.connector.api.ParamDetail;
import org.ballerinalang.connector.api.Resource;
import org.ballerinalang.connector.api.Service;
import org.ballerinalang.model.types.BArrayType;
import org.ballerinalang.model.types.BObjectType;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.codegen.PackageInfo;
import org.ballerinalang.util.codegen.ProgramFile;
import org.ballerinalang.util.codegen.StructureTypeInfo;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
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
                BObjectType type = (BObjectType) array.getElementType();
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
                BObjectType type = (BObjectType) array.getElementType();
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

    private static BRefValueArray createRecordStructArray(Resource resource,
                                                          ConsumerRecords<byte[], byte[]> records) {
        // Create records struct array.
        List<BStruct> recordsList = new ArrayList<>();
        ProgramFile programFile = resource.getResourceInfo().getServiceInfo().getPackageInfo().getProgramFile();

        records.forEach(record -> {
            BStruct recordStruct = BLangConnectorSPIUtil.createBStruct(programFile,
                                                                       KAFKA_NATIVE_PACKAGE,
                                                                       CONSUMER_RECORD_STRUCT_NAME);
            recordStruct.setBlobField(0, record.key());
            recordStruct.setBlobField(1, record.value());
            recordStruct.setIntField(0, record.offset());
            recordStruct.setIntField(1, record.partition());
            recordStruct.setIntField(2, record.timestamp());
            recordStruct.setStringField(0, record.topic());
            recordsList.add(recordStruct);
        });

        BStruct consumerRecordStruct = BLangConnectorSPIUtil.createBStruct(programFile,
                                                                           KAFKA_NATIVE_PACKAGE,
                                                                           CONSUMER_RECORD_STRUCT_NAME);
        return new BRefValueArray(recordsList.toArray(new BRefType[0]), consumerRecordStruct.getType());
    }

    private static BStruct createConsumerStruct(Resource resource, KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        // Create consumer struct.
        ProgramFile programFile = resource.getResourceInfo().getServiceInfo().getPackageInfo().getProgramFile();
        BStruct consumerStruct = BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
                                                             CONSUMER_STRUCT_NAME);
        consumerStruct.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
        return consumerStruct;
    }

    private static BRefValueArray createOffsetStructArray(Resource resource,
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

        ProgramFile programFile = resource.getResourceInfo().getServiceInfo().getPackageInfo().getProgramFile();
        List<BStruct> offsetList = new ArrayList<>();
        partitionToMetadataMap.entrySet().forEach(offset -> {
            BStruct offsetStruct = BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
                                                OFFSET_STRUCT_NAME);
            BStruct partitionStruct = BLangConnectorSPIUtil.createBStruct(programFile,
                                                                          KAFKA_NATIVE_PACKAGE,
                                                                          TOPIC_PARTITION_STRUCT_NAME);
            partitionStruct.setStringField(0, offset.getKey().topic());
            partitionStruct.setIntField(0, offset.getKey().partition());
            offsetStruct.setRefField(0, partitionStruct);
            offsetStruct.setIntField(0, offset.getValue().offset());
            offsetList.add(offsetStruct);
        });

        return new BRefValueArray(offsetList.toArray(new BRefType[0]),
                                  BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
                                                                      OFFSET_STRUCT_NAME).getType());
    }

    private static BStruct createConsumerStruct(Resource resource,
                                                KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                                String groupId) {
        // Create consumer struct.
        ProgramFile programFile = resource.getResourceInfo().getServiceInfo().getPackageInfo().getProgramFile();
        BStruct consumerStruct = BLangConnectorSPIUtil.createBStruct(programFile, KAFKA_NATIVE_PACKAGE,
                                                                     CONSUMER_STRUCT_NAME);
        consumerStruct.addNativeData(NATIVE_CONSUMER, kafkaConsumer);

        BStruct consumerConfigStruct = BLangConnectorSPIUtil.createBStruct(
                programFile, KAFKA_NATIVE_PACKAGE, CONSUMER_CONFIG_STRUCT_NAME);

        consumerConfigStruct.setStringField(1, groupId);

        consumerStruct.setRefField(0, consumerConfigStruct);
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

    public static Properties processKafkaConsumerConfig(BStruct bStruct) {
        Properties configParams = new Properties();

        addStringParamIfPresent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bStruct, configParams, 0);
        addStringParamIfPresent(ConsumerConfig.GROUP_ID_CONFIG, bStruct, configParams, 1);
        addStringParamIfPresent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, bStruct, configParams, 2);
        addStringParamIfPresent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, bStruct, configParams, 3);
        addStringParamIfPresent(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, bStruct, configParams, 4);
        addStringParamIfPresent(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, bStruct, configParams, 5);
        addStringParamIfPresent(ConsumerConfig.CLIENT_ID_CONFIG, bStruct, configParams, 6);
        addStringParamIfPresent(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, bStruct, configParams, 7);
        addStringParamIfPresent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, bStruct, configParams, 8);

        addStringArrayParamIfPresent(ALIAS_TOPICS, bStruct, configParams, 9);
        addStringArrayParamIfPresent(PROPERTIES_ARRAY, bStruct, configParams, 10);

        addIntParamIfPresent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, bStruct, configParams, 0);
        addIntParamIfPresent(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, bStruct, configParams, 1);
        addIntParamIfPresent(ConsumerConfig.METADATA_MAX_AGE_CONFIG, bStruct, configParams, 2);
        addIntParamIfPresent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, bStruct, configParams, 3);
        addIntParamIfPresent(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, bStruct, configParams, 4);
        addIntParamIfPresent(ConsumerConfig.SEND_BUFFER_CONFIG, bStruct, configParams, 5);
        addIntParamIfPresent(ConsumerConfig.RECEIVE_BUFFER_CONFIG, bStruct, configParams, 6);
        addIntParamIfPresent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, bStruct, configParams, 7);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, bStruct, configParams, 8);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, bStruct, configParams, 9);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, bStruct, configParams, 10);
        addIntParamIfPresent(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, bStruct, configParams, 11);
        addIntParamIfPresent(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, bStruct, configParams, 12);
        addIntParamIfPresent(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, bStruct, configParams, 13);
        addIntParamIfPresent(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, bStruct, configParams, 14);
        addIntParamIfPresent(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, bStruct, configParams, 15);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, bStruct, configParams, 16);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, bStruct, configParams, 17);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, bStruct, configParams, 18);
        addIntParamIfPresent(ALIAS_POLLING_TIMEOUT, bStruct, configParams, 19);
        addIntParamIfPresent(ALIAS_POLLING_INTERVAL, bStruct, configParams, 20);
        addIntParamIfPresent(ALIAS_CONCURRENT_CONSUMERS, bStruct, configParams, 21);

        addBooleanParamIfPresent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, bStruct, configParams, 0, true);
        addBooleanParamIfPresent(ConsumerConfig.CHECK_CRCS_CONFIG, bStruct, configParams, 1, true);
        addBooleanParamIfPresent(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, bStruct, configParams, 2, true);
        addBooleanParamIfPresent(ALIAS_DECOUPLE_PROCESSING, bStruct, configParams, 3, false);

        processDefaultConsumerProperties(configParams);
        return configParams;
    }

    public static Properties processKafkaProducerConfig(BStruct bStruct) {
        Properties configParams = new Properties();

        if (bStruct == null) {
            processDefaultProducerProperties(configParams);
            return configParams;
        }

        addStringParamIfPresent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bStruct, configParams, 0);
        addStringParamIfPresent(ProducerConfig.ACKS_CONFIG, bStruct, configParams, 1);
        addStringParamIfPresent(ProducerConfig.COMPRESSION_TYPE_CONFIG, bStruct, configParams, 2);
        addStringParamIfPresent(ProducerConfig.CLIENT_ID_CONFIG, bStruct, configParams, 3);
        addStringParamIfPresent(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, bStruct, configParams, 4);
        addStringParamIfPresent(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, bStruct, configParams, 5);
        addStringParamIfPresent(ProducerConfig.PARTITIONER_CLASS_CONFIG, bStruct, configParams, 6);
        addStringParamIfPresent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, bStruct, configParams, 7);
        addStringParamIfPresent(ProducerConfig.TRANSACTIONAL_ID_CONFIG, bStruct, configParams, 8);

        addIntParamIfPresent(ProducerConfig.BUFFER_MEMORY_CONFIG, bStruct, configParams, 0);
        addIntParamIfPresent(ProducerConfig.RETRIES_CONFIG, bStruct, configParams, 1);
        addIntParamIfPresent(ProducerConfig.BATCH_SIZE_CONFIG, bStruct, configParams, 2);
        addIntParamIfPresent(ProducerConfig.LINGER_MS_CONFIG, bStruct, configParams, 3);
        addIntParamIfPresent(ProducerConfig.SEND_BUFFER_CONFIG, bStruct, configParams, 4);
        addIntParamIfPresent(ProducerConfig.RECEIVE_BUFFER_CONFIG, bStruct, configParams, 5);
        addIntParamIfPresent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, bStruct, configParams, 6);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, bStruct, configParams, 7);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, bStruct, configParams, 8);
        addIntParamIfPresent(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, bStruct, configParams, 9);
        addIntParamIfPresent(ProducerConfig.MAX_BLOCK_MS_CONFIG, bStruct, configParams, 10);
        addIntParamIfPresent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, bStruct, configParams, 11);
        addIntParamIfPresent(ProducerConfig.METADATA_MAX_AGE_CONFIG, bStruct, configParams, 12);
        addIntParamIfPresent(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, bStruct, configParams, 13);
        addIntParamIfPresent(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, bStruct, configParams, 14);
        addIntParamIfPresent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, bStruct, configParams, 15);
        addIntParamIfPresent(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, bStruct, configParams, 16);
        addIntParamIfPresent(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, bStruct, configParams, 17);

        addBooleanParamIfPresent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, bStruct, configParams, 0, false);

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
                                                BStruct bStruct,
                                                Properties configParams,
                                                int index) {
        if (Objects.nonNull(bStruct.getRefField(index))) {
            String value = ((BString) bStruct.getRefField(index)).value();
            if (!(value == null || value.equals(""))) {
                configParams.put(paramName, value);
            }
        }
    }

    private static void addStringArrayParamIfPresent(String paramName,
                                                BStruct bStruct,
                                                Properties configParams,
                                                int index) {
        BStringArray bArray = (BStringArray) bStruct.getRefField(index);
        List<String> values = new ArrayList<>();
        if (bArray != null && bArray.size() != 0) {
            for (int i = 0; i < bArray.size(); i++) {
                values.add(bArray.get(i));
            }
            configParams.put(paramName, values);
        }
    }

    private static void addIntParamIfPresent(String paramName,
                                             BStruct bStruct,
                                             Properties configParams,
                                             int index) {
        long value = bStruct.getIntField(index);
        if (value != -1) {
            configParams.put(paramName, new Long(value).intValue());
        }
    }

    private static void addBooleanParamIfPresent(String paramName,
                                                 BStruct bStruct,
                                                 Properties configParams,
                                                 int index,
                                                 boolean defaultValue) {
        boolean value = bStruct.getBooleanField(index) == 1;
        if (value != defaultValue) {
            configParams.put(paramName, value);
        }
    }

    public static BStruct createKafkaPackageStruct(Context context, String structName) {
        PackageInfo kafkaPackageInfo = context.getProgramFile()
                .getPackageInfo(KAFKA_NATIVE_PACKAGE);
        StructureTypeInfo structInfo = kafkaPackageInfo
                .getStructInfo(structName);
        return new BStruct(structInfo.getType());
    }

    public static ArrayList<TopicPartition> getTopicPartitionList(BRefValueArray partitions) {
        ArrayList<TopicPartition> partitionList = new ArrayList<TopicPartition>();
        if (partitions != null) {
            for (int counter = 0; counter < partitions.size(); counter++) {
                BStruct partition = (BStruct) partitions.get(counter);
                String topic = partition.getStringField(0);
                int partitionValue = new Long(partition.getIntField(0)).intValue();
                partitionList.add(new TopicPartition(topic, partitionValue));
            }
        }
        return partitionList;
    }

}
