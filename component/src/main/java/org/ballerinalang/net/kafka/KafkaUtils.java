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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.connector.api.AnnAttrValue;
import org.ballerinalang.connector.api.Annotation;
import org.ballerinalang.connector.api.ConnectorUtils;
import org.ballerinalang.connector.api.ParamDetail;
import org.ballerinalang.connector.api.Resource;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * {@code }
 */
public class KafkaUtils {

    public static BValue[] getSignatureParameters(Resource resource, ConsumerRecord<byte[], byte[]> record) {
        BStruct recordStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                Constants.CONSUMER_RECORD_STRUCT_NAME);
        recordStruct.setBlobField(0, record.key());
        recordStruct.setBlobField(1, record.value());
        recordStruct.setIntField(0, record.partition());
        recordStruct.setIntField(1, record.timestamp());
        recordStruct.setStringField(0, record.topic());

        //TODO validation
        List<ParamDetail> paramDetails = resource.getParamDetails();
        BValue[] bValues = new BValue[paramDetails.size()];
        bValues[0] = recordStruct;

        return bValues;
    }

    public static BValue[] getSignatureParameters(Resource resource,
                                                  ConsumerRecords<byte[], byte[]> records,
                                                  KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        List<BStruct> recordsList = new ArrayList<>();
        records.forEach(record -> {
            //record.
            BStruct recordStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                    Constants.CONSUMER_RECORD_STRUCT_NAME);
            recordStruct.setBlobField(0, record.key());
            recordStruct.setBlobField(1, record.value());
            recordStruct.setIntField(0, record.partition());
            recordStruct.setIntField(1, record.timestamp());
            recordStruct.setStringField(0, record.topic());
            recordsList.add(recordStruct);
        });

        BStruct consumerStruct = ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                Constants.CONSUMER_RECORD_STRUCT_NAME);
        consumerStruct.addNativeData(Constants.NATIVE_CONSUMER, kafkaConsumer);


        //TODO validation
        List<ParamDetail> paramDetails = resource.getParamDetails();
        BValue[] bValues = new BValue[paramDetails.size()];
        bValues[0] = new BRefValueArray(recordsList.toArray(new BRefType[0]),
                ConnectorUtils.createStruct(resource, Constants.KAFKA_NATIVE_PACKAGE,
                        Constants.CONSUMER_RECORD_STRUCT_NAME).getType());
        bValues[1] = consumerStruct;
        return bValues;
    }

    public static Properties processKafkaConfig(Annotation kafkaConfig) {

        Properties configParams = new Properties();

        addStringParamIfPresent(Constants.ALIAS_BOOTSTRAP_SERVERS, kafkaConfig, configParams);
        addStringParamIfPresent(Constants.ALIAS_GROUP_ID, kafkaConfig, configParams);
        addStringArrayParamIfPresent(Constants.ALIAS_TOPICS, kafkaConfig, configParams);

        addIntParamIfPresent(Constants.ALIAS_CONCURRENT_CONSUMERS, kafkaConfig, configParams);
        addIntParamIfPresent(Constants.ALIAS_POLLING_TIMEOUT, kafkaConfig, configParams);

        addBooleanParamIfPresent(Constants.ALIAS_DECOUPLE_PROCESSING, kafkaConfig, configParams);

        processPropertiesArray(kafkaConfig, configParams);
        updateMappedParameters(configParams);
        processDefaultProperties(configParams);
        return configParams;
    }

    private static void processDefaultProperties(Properties configParams) {
        configParams.put(Constants.PARAM_KEY_DESERIALIZER, Constants.DEFAULT_KEY_DESERIALIZER);
        configParams.put(Constants.PARAM_VALUE_DESERIALIZER, Constants.DEFAULT_VALUE_DESERIALIZER);
    }

    private static void processPropertiesArray(Annotation jmsConfig, Properties configParams) {
        AnnAttrValue attributeValue = jmsConfig.getAnnAttrValue(Constants.PROPERTIES_ARRAY);
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

    private static void addStringArrayParamIfPresent(String paramName, Annotation jmsConfig, Properties configParams) {
        AnnAttrValue attributeValue = jmsConfig.getAnnAttrValue(paramName);
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

    private static void addStringParamIfPresent(String paramName, Annotation jmsConfig, Properties configParams) {
        AnnAttrValue value = jmsConfig.getAnnAttrValue(paramName);
        if (value != null && value.getStringValue() != null) {
            configParams.put(paramName, value.getStringValue());
        }
    }

    private static void addIntParamIfPresent(String paramName, Annotation jmsConfig, Properties configParams) {
        AnnAttrValue value = jmsConfig.getAnnAttrValue(paramName);
        if (value != null) {
            configParams.put(paramName, ((Long) value.getIntValue()).intValue());
        }
    }

    private static void addBooleanParamIfPresent(String paramName, Annotation jmsConfig, Properties configParams) {
        AnnAttrValue value = jmsConfig.getAnnAttrValue(paramName);
        if (value != null) {
            configParams.put(paramName, value.getBooleanValue());
        }
    }

}
