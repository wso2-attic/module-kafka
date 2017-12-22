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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.api.KafkaListener;
import org.ballerinalang.net.kafka.future.KafkaPollCycleFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@code KafkaRecordConsumer} This class represents Runnable flow which periodically poll the remote broker and fetch
 * Kafka records.
 */
public class KafkaRecordConsumer implements Runnable, Thread.UncaughtExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordConsumer.class);

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private int polliungTimeout = 1000;
    private boolean decoupleProcessing = true;
    private String groupID;
    private AtomicBoolean running = new AtomicBoolean(true);
    private KafkaListener kafkaListener;

    public KafkaRecordConsumer(KafkaListener kafkaListener, Properties configParams) {
        this.kafkaConsumer = new KafkaConsumer<byte[], byte[]>(configParams);
        ArrayList<String> topics = (ArrayList<String>) configParams.get(Constants.ALIAS_TOPICS);
        if (configParams.get(Constants.ALIAS_POLLING_TIMEOUT) != null) {
            this.polliungTimeout = (Integer) configParams.get(Constants.ALIAS_POLLING_TIMEOUT);
        }
        this.kafkaConsumer.subscribe(topics);
        this.kafkaListener = kafkaListener;
        if (configParams.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG) != null) {
            this.decoupleProcessing = (Boolean) configParams.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        }
        // this is to override default decouple processing setting if required
        if (configParams.get(Constants.ALIAS_DECOUPLE_PROCESSING) != null) {
            this.decoupleProcessing = (Boolean) configParams.get(Constants.ALIAS_DECOUPLE_PROCESSING);
        }
        this.groupID = (String) configParams.get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<byte[], byte[]> recordsRetrieved = kafkaConsumer.poll(this.polliungTimeout);
                if (!recordsRetrieved.isEmpty()) {
                    // when decoupleProcessing == 'true' Kafka records set will be dispatched and processed in
                    // parallel threads.
                    if (decoupleProcessing) {
                        kafkaListener.onRecordsReceived(recordsRetrieved, kafkaConsumer);
                    } else {
                        Semaphore flowControl = new Semaphore(0);
                        KafkaPollCycleFutureListener pollCycleListener =
                                new KafkaPollCycleFutureListener(flowControl);
                        kafkaListener.onRecordsReceived(recordsRetrieved, kafkaConsumer, pollCycleListener, groupID);
                        flowControl.acquire();
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore WakeupException is thrown due to call on stopConsume() that would close the consumer instance
            // on finally block
            if (this.running.get()) {
                throw new RuntimeException("Error polling Kafka records", e);
            }
        } catch (KafkaException | InterruptedException e) {
            throw new RuntimeException("Error polling Kafka records", e);
        } finally {
            kafkaConsumer.close();
        }
    }

    public void consume() {
        startReceiverThread();
    }

    private void startReceiverThread() {
        Thread thread = new Thread(this, "KafkaConsumerThread");
        thread.setUncaughtExceptionHandler(this);
        thread.start();
    }

    @Override
    public void uncaughtException(Thread thread, Throwable error) {
        running.set(false);
        logger.error("Unexpected error occurred while polling Kafka records", error);
    }

    public void stopConsume() {
        this.running.set(false);
        this.kafkaConsumer.wakeup();
    }
}
