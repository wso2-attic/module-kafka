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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.connector.api.AnnAttrValue;
import org.ballerinalang.connector.api.Annotation;
import org.ballerinalang.connector.api.BallerinaConnectorException;
import org.ballerinalang.connector.api.ConnectorUtils;
import org.ballerinalang.connector.api.ParamDetail;
import org.ballerinalang.connector.api.Resource;
import org.ballerinalang.connector.api.Service;
import org.ballerinalang.model.types.BArrayType;
import org.ballerinalang.model.types.BStructType;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for Kafka.
 */
public class KafkaUtils {

    public static Resource extractKafkaResource(Service service) throws BallerinaConnectorException {
        Resource[] resources = service.getResources();
        if (resources.length == 0) {
            throw new BallerinaException("No resources found to handle the Kafka records in " + service.getName());
        }
        if (resources.length > 1) {
            throw new BallerinaException("More than one resources found in Kafka service " + service.getName()
                    + ".Kafka Service should only have one resource");
        }
        Resource mainResource = resources[0];
        List<ParamDetail> paramDetails = mainResource.getParamDetails();

        if (paramDetails.size() == 0 | paramDetails.size() == 1) {
            throw new BallerinaException("Kafka resource signature does not comply with param standard sequence.");
        } else if (paramDetails.size() == 2) {
            validateConsumerParam(paramDetails.get(0));
            validateRecordsParam(paramDetails.get(1));
        } else if (paramDetails.size() == 3) {
            validateConsumerParam(paramDetails.get(0));
            validateRecordsParam(paramDetails.get(1));
            validateOffsetsParam(paramDetails.get(2));
        } else if (paramDetails.size() == 4) {
            validateConsumerParam(paramDetails.get(0));
            validateRecordsParam(paramDetails.get(1));
            validateOffsetsParam(paramDetails.get(2));
            validateGroupIDParam(paramDetails.get(3));
        }
        return resources[0];
    }

    private static void validateConsumerParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.STRUCT_TAG) {
            BStructType type = (BStructType) param.getVarType();
            if (type.getPackagePath().equals(Constants.KAFKA_NATIVE_PACKAGE) &&
                    type.getName().equals("KafkaConsumer")) {
                return;
            }
        }
        throw new BallerinaException("Resource signature validation failed for param at index: 0.");
    }

    private static void validateRecordsParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.ARRAY_TAG) {
            BArrayType array = (BArrayType) param.getVarType();
            if (array.getElementType().getTag() == TypeTags.STRUCT_TAG) {
                BStructType type = (BStructType) array.getElementType();
                if (type.getPackagePath().equals(Constants.KAFKA_NATIVE_PACKAGE) &&
                        type.getName().equals("ConsumerRecord")) {
                    return;
                }

            }
        }
        throw new BallerinaException("Resource signature validation failed for param at index: 1.");
    }

    private static void validateOffsetsParam(ParamDetail param) {
        if (param.getVarType().getTag() == TypeTags.ARRAY_TAG) {
            BArrayType array = (BArrayType) param.getVarType();
            if (array.getElementType().getTag() == TypeTags.STRUCT_TAG) {
                BStructType type = (BStructType) array.getElementType();
                if (type.getPackagePath().equals(Constants.KAFKA_NATIVE_PACKAGE) &&
                        type.getName().equals("Offset")) {
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
        // Create records struct array.
        List<BStruct> recordsList = new ArrayList<>();
        records.forEach(record -> {
            BStruct recordStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                    Constants.CONSUMER_RECORD_STRUCT_NAME);
            recordStruct.setBlobField(0, record.key());
            recordStruct.setBlobField(1, record.value());
            recordStruct.setIntField(0, record.offset());
            recordStruct.setIntField(1, record.partition());
            recordStruct.setIntField(2, record.timestamp());
            recordStruct.setStringField(0, record.topic());
            recordsList.add(recordStruct);
        });

        // Create consumer struct.
        BStruct consumerStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                Constants.CONSUMER_STRUCT_NAME);
        consumerStruct.addNativeData(Constants.NATIVE_CONSUMER, kafkaConsumer);

        List<ParamDetail> paramDetails = resource.getParamDetails();
        BValue[] bValues = new BValue[paramDetails.size()];
        if (paramDetails.size() == 2 | paramDetails.size() == 3 | paramDetails.size() == 4) {
            bValues[0] = consumerStruct;
            bValues[1] = new BRefValueArray(recordsList.toArray(new BRefType[0]),
                    ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                            Constants.CONSUMER_RECORD_STRUCT_NAME).getType());
        } else if (paramDetails.size() == 1) {
            bValues[0] = consumerStruct;
        }

        return bValues;
    }

    public static BValue[] getSignatureParameters(Resource resource,
                                                  ConsumerRecords<byte[], byte[]> records,
                                                  KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                                  String groupId) {
        // Create records struct array.
        List<BStruct> recordsList = new ArrayList<>();
        Map<TopicPartition, Long> partitionToUncommittedOffsetMap = new HashMap<>();
        records.forEach(record -> {
            BStruct recordStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                    Constants.CONSUMER_RECORD_STRUCT_NAME);
            recordStruct.setBlobField(0, record.key());
            recordStruct.setBlobField(1, record.value());
            recordStruct.setIntField(0, record.offset());
            recordStruct.setIntField(1, record.partition());
            recordStruct.setIntField(2, record.timestamp());
            recordStruct.setStringField(0, record.topic());
            recordsList.add(recordStruct);
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            partitionToUncommittedOffsetMap.put(tp, record.offset());
        });

        // Create offsets struct array.
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> e : partitionToUncommittedOffsetMap.entrySet()) {
            partitionToMetadataMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
        }

        List<BStruct> offsetList = new ArrayList<>();
        partitionToMetadataMap.entrySet().forEach(offset -> {
            BStruct offsetStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                    Constants.OFFSET_STRUCT_NAME);
            BStruct partitionStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                    Constants.TOPIC_PARTITION_STRUCT_NAME);
            partitionStruct.setStringField(0, offset.getKey().topic());
            partitionStruct.setIntField(0, offset.getKey().partition());
            offsetStruct.setRefField(0, partitionStruct);
            offsetStruct.setIntField(0, offset.getValue().offset());
            offsetList.add(offsetStruct);
        });

        // Create consumer struct.
        BStruct consumerStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                Constants.CONSUMER_STRUCT_NAME);
        consumerStruct.addNativeData(Constants.NATIVE_CONSUMER, kafkaConsumer);

        BMap<String, BValue> consumerBalConfig = new BMap();
        consumerBalConfig.put(ConsumerConfig.GROUP_ID_CONFIG, new BString(groupId));
        consumerStruct.setRefField(0, consumerBalConfig);


        List<ParamDetail> paramDetails = resource.getParamDetails();
        BValue[] bValues = new BValue[paramDetails.size()];
        if (paramDetails.size() == 4) {
            bValues[0] = consumerStruct;
            bValues[1] = new BRefValueArray(recordsList.toArray(new BRefType[0]),
                    ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                            Constants.CONSUMER_RECORD_STRUCT_NAME).getType());
            bValues[2] = new BRefValueArray(offsetList.toArray(new BRefType[0]),
                    ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                            Constants.OFFSET_STRUCT_NAME).getType());
            if (groupId == null) {
                bValues[3] = new BString("");
            } else {
                bValues[3] = new BString(groupId);
            }
        } else if (paramDetails.size() == 3) {
            bValues[0] = consumerStruct;
            bValues[1] = new BRefValueArray(recordsList.toArray(new BRefType[0]),
                    ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                            Constants.CONSUMER_RECORD_STRUCT_NAME).getType());
            bValues[2] = new BRefValueArray(offsetList.toArray(new BRefType[0]),
                    ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                            Constants.OFFSET_STRUCT_NAME).getType());
        } else if (paramDetails.size() == 2) {
            bValues[0] = consumerStruct;
            bValues[1] = new BRefValueArray(recordsList.toArray(new BRefType[0]),
                    ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                            Constants.CONSUMER_RECORD_STRUCT_NAME).getType());
        } else if (paramDetails.size() == 1) {
            bValues[0] = consumerStruct;
        }

        return bValues;
    }

    public static Properties processKafkaConsumerConfig(Annotation kafkaConfig) {

        Properties configParams = new Properties();

        addStringArrayParamIfPresent(Constants.ALIAS_TOPICS, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_CONCURRENT_CONSUMERS, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_POLLING_TIMEOUT, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_POLLING_INTERVAL, kafkaConfig, configParams);
        addBooleanParamIfPresent(Constants.ALIAS_DECOUPLE_PROCESSING, kafkaConfig, configParams);

        addStringParamIfPresent(Constants.ALIAS_BOOTSTRAP_SERVERS_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_GROUP_ID_CONFIG, kafkaConfig, configParams);
        addBooleanParamIfPresent(Constants.ALIAS_ENABLE_AUTO_COMMIT_CONFIG, kafkaConfig, configParams);

        addStringParamIfPresent(Constants.ALIAS_AUTO_OFFSET_RESET_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_SESSION_TIMEOUT_MS_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_HEARTBEAT_INTERVAL_MS_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_PARTITION_ASSIGNMENT_STRATEGY_CONFIG, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_METADATA_MAX_AGE_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_CLIENT_ID_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_MAX_PARTITION_FETCH_BYTES_CONFIG, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_SEND_BUFFER_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_RECEIVE_BUFFER_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_FETCH_MIN_BYTES_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_FETCH_MAX_BYTES_CONFIG, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_FETCH_MAX_WAIT_MS_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_RECONNECT_BACKOFF_MS_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_RETRY_BACKOFF_MS_CONFIG, kafkaConfig, configParams);
        addBooleanParamIfPresent(Constants.ALIAS_CHECK_CRCS_CONFIG, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_METRICS_SAMPLE_WINDOW_MS_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_METRICS_NUM_SAMPLES_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_METRICS_RECORDING_LEVEL_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_METRIC_REPORTER_CLASSES_CONFIG, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_REQUEST_TIMEOUT_MS_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_CONNECTIONS_MAX_IDLE_MS_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_INTERCEPTOR_CLASSES_CONFIG, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_MAX_POLL_RECORDS_CONFIG, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_MAX_POLL_INTERVAL_MS_CONFIG, kafkaConfig, configParams);
        addBooleanParamIfPresent(Constants.ALIAS_EXCLUDE_INTERNAL_TOPICS_CONFIG, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_ISOLATION_LEVEL_CONFIG, kafkaConfig, configParams);

        processPropertiesArray(kafkaConfig, configParams);
        updateMappedParameters(configParams);
        processDefaultConsumerProperties(configParams);
        return configParams;
    }

    public static Properties processKafkaConsumerConfig(BMap bMap) {

        Properties configParams = new Properties();

        addStringParamIfPresent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.GROUP_ID_CONFIG, bMap, configParams);
        addBooleanParamIfPresent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.METADATA_MAX_AGE_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.CLIENT_ID_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.SEND_BUFFER_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.RECEIVE_BUFFER_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, bMap, configParams);
        addBooleanParamIfPresent(ConsumerConfig.CHECK_CRCS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, bMap, configParams);
        addBooleanParamIfPresent(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, bMap, configParams);

        processDefaultConsumerProperties(configParams);
        return configParams;
    }

    public static Properties processKafkaProducerConfig(BMap bMap) {

        Properties configParams = new Properties();

        addStringParamIfPresent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.TRANSACTIONAL_ID_CONFIG, bMap, configParams);
        addBooleanParamIfPresent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.BUFFER_MEMORY_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.RETRIES_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.ACKS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.COMPRESSION_TYPE_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.BATCH_SIZE_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.LINGER_MS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.CLIENT_ID_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.SEND_BUFFER_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.RECEIVE_BUFFER_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.MAX_BLOCK_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.METADATA_MAX_AGE_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.PARTITIONER_CLASS_CONFIG, bMap, configParams);
        addStringParamIfPresent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, bMap, configParams);
        addIntParamIfPresent(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, bMap, configParams);

        processDefaultProducerProperties(configParams);
        return configParams;
    }

    public static void processDefaultConsumerProperties(Properties configParams) {
        configParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_KEY_DESERIALIZER);
        configParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Constants.DEFAULT_VALUE_DESERIALIZER);
    }

    public static void processDefaultProducerProperties(Properties configParams) {
        configParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_KEY_SERIALIZER);
        configParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.DEFAULT_VALUE_SERIALIZER);
    }

    private static void processPropertiesArray(Annotation kafkaConfig, Properties configParams) {
        AnnAttrValue attributeValue = kafkaConfig.getAnnAttrValue(Constants.PROPERTIES_ARRAY);
        if (attributeValue != null) {
            AnnAttrValue[] attributeValueArray = attributeValue.getAnnAttrValueArray();
            for (AnnAttrValue annAttributeValue : attributeValueArray) {
                String stringValue = annAttributeValue.getStringValue();
                int index = stringValue.indexOf("=");
                if (index != -1) {
                    String key = stringValue.substring(0, index).trim();
                    String value = stringValue.substring(index + 1).trim();
                    configParams.put(key, value);
                } else {
                    throw new BallerinaException("Invalid " + Constants.PROPERTIES_ARRAY + " provided. Key value"
                            + " pair is not separated by an '='");
                }
            }
        }
    }

    private static void updateMappedParameters(Properties configParams) {
        Iterator<Map.Entry<Object, Object>> iterator = configParams.entrySet().iterator();
        Properties tempProps = new Properties();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> entry = iterator.next();
            String mappedParam = Constants.MAPPING_PARAMETERS.get(entry.getKey());
            if (mappedParam != null) {
                tempProps.put(mappedParam, entry.getValue());
                iterator.remove();
            }
        }
        configParams.putAll(tempProps);
    }

    private static void addStringArrayParamIfPresent(String paramName, Annotation kafkaConfig,
                                                     Properties configParams) {
        AnnAttrValue attributeValue = kafkaConfig.getAnnAttrValue(paramName);
        if (attributeValue != null) {
            AnnAttrValue[] attributeValueArray = attributeValue.getAnnAttrValueArray();
            ArrayList<String> topics = new ArrayList<String>();
            for (AnnAttrValue annAttributeValue : attributeValueArray) {
                String stringValue = annAttributeValue.getStringValue();
                topics.add(stringValue);
            }
            configParams.put(paramName, topics);
        }
    }

    private static void addStringParamIfPresent(String paramName, Annotation kafkaConfig, Properties configParams) {
        AnnAttrValue value = kafkaConfig.getAnnAttrValue(paramName);
        if (value != null && value.getStringValue() != null) {
            configParams.put(paramName, value.getStringValue());
        }
    }

    private static void addIntParamIfPresent(String paramName, Annotation kafkaConfig, Properties configParams) {
        AnnAttrValue value = kafkaConfig.getAnnAttrValue(paramName);
        if (value != null) {
            configParams.put(paramName, ((Long) value.getIntValue()).intValue());
        }
    }

    private static void addBooleanParamIfPresent(String paramName, Annotation kafkaConfig, Properties configParams) {
        AnnAttrValue value = kafkaConfig.getAnnAttrValue(paramName);
        if (value != null) {
            configParams.put(paramName, value.getBooleanValue());
        }
    }

    private static void addStringParamIfPresent(String paramName, BMap bMap, Properties configParams) {
        BValue value = bMap.get(paramName);
        if (value != null && value instanceof BString) {
            configParams.put(paramName, ((BString) value).value());
        }
    }

    private static void addIntParamIfPresent(String paramName, BMap bMap, Properties configParams) {
        BValue value = bMap.get(paramName);
        if (value != null && value instanceof BInteger) {
            configParams.put(paramName, ((BInteger) value).value().intValue());
        }
    }

    private static void addBooleanParamIfPresent(String paramName, BMap bMap, Properties configParams) {
        BValue value = bMap.get(paramName);
        if (value != null && value instanceof BBoolean) {
            configParams.put(paramName, ((BBoolean) value).value().booleanValue());
        }
    }

}
