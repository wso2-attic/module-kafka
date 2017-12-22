/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ballerinalang.net.kafka.impl;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.connector.api.Executor;
import org.ballerinalang.connector.api.Resource;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.net.kafka.api.KafkaListener;
import org.ballerinalang.net.kafka.future.KafkaPollCycleFutureListener;

/**
 *  Kafka Connector listener for Ballerina.
 */
public class KafkaListenerImpl implements KafkaListener {

    private Resource resource;

    public KafkaListenerImpl(Resource resource) {
        this.resource = resource;
    }

    @Override
    public void onRecordsReceived(ConsumerRecords records,
                                  KafkaConsumer kafkaConsumer) {
        Executor.submit(resource, null, KafkaUtils.getSignatureParameters(resource, records,
                kafkaConsumer, null));
    }

    @Override
    public void onRecordsReceived(ConsumerRecords records,
                                  KafkaConsumer kafkaConsumer,
                                  KafkaPollCycleFutureListener listener,
                                  String groupID) {
        ConnectorFuture future = Executor.submit(resource, null,
                KafkaUtils.getSignatureParameters(resource, records, kafkaConsumer, groupID));
        future.setConnectorFutureListener(listener);

    }
}
