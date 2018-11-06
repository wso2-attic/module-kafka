/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.kafka.impl;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.connector.api.Executor;
import org.ballerinalang.connector.api.Resource;
import org.ballerinalang.kafka.api.KafkaListener;
import org.ballerinalang.kafka.nativeimpl.consumer.KafkaPollCycleFutureListener;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 *  Kafka Connector listener for Ballerina.
 */
public class KafkaListenerImpl implements KafkaListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerImpl.class);
    private Resource resource;
    private ResponseCallback callback;

    public KafkaListenerImpl(Resource resource) {
        this.resource = resource;
        callback = new ResponseCallback();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRecordsReceived(ConsumerRecords records, KafkaConsumer kafkaConsumer) {
        Executor.submit(resource, callback, Collections.emptyMap(), null,
                        KafkaUtils.getSignatureParameters(resource, records, kafkaConsumer));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onRecordsReceived(ConsumerRecords records,
                                  KafkaConsumer kafkaConsumer,
                                  KafkaPollCycleFutureListener listener,
                                  String groupID) {
        Executor.submit(resource, listener, Collections.emptyMap(), null,
                        KafkaUtils.getSignatureParameters(resource, records, kafkaConsumer, groupID));

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable throwable) {
        logger.error("Kafka Ballerina server connector retrieved exception: " + throwable.getMessage(), throwable);
    }

    private static class ResponseCallback implements CallableUnitCallback {

        @Override
        public void notifySuccess() {
            // do nothing
        }

        @Override
        public void notifyFailure(BMap<String, BValue> bStruct) {
            // do nothing
        }
    }
}
