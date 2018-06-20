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

/**
 * Constants related to for Kafka API.
 */
public class KafkaConstants {

    private KafkaConstants() {
    }

    public static final String ORG_NAME = "wso2";
    public static final String PACKAGE_NAME = "kafka:0.0.0";
    public static final String KAFKA_NATIVE_PACKAGE = "wso2/kafka:0.0.0";

    public static final String NATIVE_CONSUMER = "KafkaConsumer";
    public static final String NATIVE_PRODUCER = "KafkaProducer";
    public static final String NATIVE_PRODUCER_CONFIG = "KafkaProducerConfig";

    public static final String TOPIC_PARTITION_STRUCT_NAME = "TopicPartition";
    public static final String OFFSET_STRUCT_NAME = "Offset";

    public static final String CONSUMER_RECORD_STRUCT_NAME = "ConsumerRecord";
    public static final String CONSUMER_STRUCT_NAME = "ConsumerAction";
    public static final String CONSUMER_CONFIG_STRUCT_NAME = "ConsumerConfig";
    public static final String CONSUMER_ENDPOINT_STRUCT_NAME = "SimpleConsumer";

    public static final String PRODUCER_STRUCT_NAME = "ProducerAction";
    public static final String PRODUCER_CONFIG_STRUCT_NAME = "ProducerConfig";
    public static final String PRODUCER_RECORD_STRUCT_NAME = "ProducerRecord";

    public static final String PROPERTIES_ARRAY = "properties";

    public static final String ALIAS_CONCURRENT_CONSUMERS = "concurrentConsumers";
    public static final String ALIAS_TOPICS = "topics";
    public static final String ALIAS_POLLING_TIMEOUT = "pollingTimeout";
    public static final String ALIAS_POLLING_INTERVAL = "pollingInterval";
    public static final String ALIAS_DECOUPLE_PROCESSING = "decoupleProcessing";

    public static final String DEFAULT_KEY_DESERIALIZER
            = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    public static final String DEFAULT_VALUE_DESERIALIZER
            = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    public static final String DEFAULT_KEY_SERIALIZER
            = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_VALUE_SERIALIZER
            = "org.apache.kafka.common.serialization.ByteArraySerializer";

}
