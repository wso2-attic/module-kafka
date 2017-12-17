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

package org.ballerinalang.net.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.net.kafka.api.KafkaListener;
import org.ballerinalang.net.kafka.future.KafkaPollCycleFutureListener;

/**
 * {@code }
 */
public class KafkaMessageHandler {

    private KafkaListener kafkaListener;

    public KafkaMessageHandler(KafkaListener kafkaListener) {
        this.kafkaListener = kafkaListener;
    }

    public void handle(ConsumerRecords<byte[], byte[]> consumerRecords,
                       KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        kafkaListener.onRecordsReceived(consumerRecords, kafkaConsumer);
    }

    public void handle(ConsumerRecords<byte[], byte[]> consumerRecords,
                       KafkaConsumer<byte[], byte[]> kafkaConsumer,
                       KafkaPollCycleFutureListener listener) {
        kafkaListener.onRecordsReceived(consumerRecords, kafkaConsumer, listener);
    }

}
