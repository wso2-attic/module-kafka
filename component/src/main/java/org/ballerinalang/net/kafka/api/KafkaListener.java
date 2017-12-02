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

package org.ballerinalang.net.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.net.kafka.future.KafkaPollCycleFutureListener;

/**
 * {@code }
 */
public interface KafkaListener {

    void onRecordReceived(ConsumerRecord<byte[], byte[]> record);

    void onRecordsReceived(ConsumerRecords<byte[], byte[]> record,
                           KafkaConsumer<byte[], byte[]> kafkaConsumer,
                           KafkaPollCycleFutureListener listener);

    void onErrorReceived(Throwable throwable);

}
