/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
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

package org.ballerinalang.net.kafka.impl;


import org.apache.kafka.common.KafkaException;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.api.KafkaListener;
import org.ballerinalang.net.kafka.api.KafkaServerConnector;
import org.ballerinalang.net.kafka.consumer.KafkaRecordConsumer;
import org.ballerinalang.net.kafka.exception.KafkaConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * {@code KafkaServerConnectorImpl} This is the implementation for the {@code KafkaServerConnector} API
 * which provides transport receiver implementation for Kafka.
 */
public class KafkaServerConnectorImpl implements KafkaServerConnector {

    private static final Logger logger = LoggerFactory.getLogger(KafkaServerConnectorImpl.class);

    private String serviceId;
    private KafkaListener kafkaListener;
    private Properties configParams;
    private int numOfConcurrentConsumers = 1;
    private List<KafkaRecordConsumer> messageConsumers;


    public KafkaServerConnectorImpl(String serviceId, Properties configParams,
                                    KafkaListener kafkaListener) throws KafkaConnectorException {
        this.kafkaListener = kafkaListener;
        this.serviceId = serviceId;
        if (configParams.get(Constants.ALIAS_CONCURRENT_CONSUMERS) != null) {
            this.numOfConcurrentConsumers = (Integer) configParams.get(Constants.ALIAS_CONCURRENT_CONSUMERS);
        }
        this.configParams = configParams;
    }

    @Override
    public void start() throws KafkaConnectorException {
        try {
            messageConsumers = new ArrayList<>();
            for (int counter = 0; counter < numOfConcurrentConsumers; counter++) {
                KafkaRecordConsumer consumer = new KafkaRecordConsumer(this.kafkaListener, this.configParams);
                consumer.consume();
            }
        } catch (KafkaException e) {
            throw new KafkaConnectorException("Error creating Kafka consumer to remote " +
                    "broker and subscribe to annotated topics", e);
        }
    }

    @Override
    public boolean stop() throws KafkaConnectorException {
        for (KafkaRecordConsumer consumer : messageConsumers) {
            consumer.stopConsume();
        }
        return true;
    }

}
