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
 * {@code }
 */
public class KafkaRecordConsumer implements Runnable, Thread.UncaughtExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordConsumer.class);

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private int polliungTimeout = 1000;
    private KafkaMessageHandler kafkaMessageHandler;
    private boolean decoupleProcessing = true;

    private AtomicBoolean running = new AtomicBoolean(true);

    public KafkaRecordConsumer(KafkaListener kafkaListener, String serviceId, Properties configParams) {
        this.kafkaConsumer = new KafkaConsumer<byte[], byte[]>(configParams);
        ArrayList<String> topics = (ArrayList<String>) configParams.get(Constants.ALIAS_TOPICS);
        if (configParams.get(Constants.ALIAS_POLLING_TIMEOUT) != null) {
            this.polliungTimeout = (Integer) configParams.get(Constants.ALIAS_POLLING_TIMEOUT);
        }
        this.kafkaConsumer.subscribe(topics);
        this.kafkaMessageHandler = new KafkaMessageHandler(kafkaListener);
        if (configParams.get(Constants.ALIAS_DECOUPLE_PROCESSING) != null) {
            this.decoupleProcessing = (Boolean) configParams.get(Constants.ALIAS_DECOUPLE_PROCESSING);
        }
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<byte[], byte[]> recordsRetrieved = kafkaConsumer.poll(this.polliungTimeout);
                if (!recordsRetrieved.isEmpty()) {
                    if (decoupleProcessing) {
                        kafkaMessageHandler.handle(recordsRetrieved);
                    } else {
                        Semaphore flowControl = new Semaphore(0);
                        KafkaPollCycleFutureListener pollCycleListener =
                                new KafkaPollCycleFutureListener(flowControl);
                        kafkaMessageHandler.handle(recordsRetrieved, kafkaConsumer, pollCycleListener);
                        flowControl.acquire();
                    }
                }
            }
        } catch (WakeupException e) {
            if (this.running.get()) {
                throw new RuntimeException("Error receiving messages", e);
            }
        } catch (Exception e) {
            //TODO error handling
            throw new RuntimeException("Error receiving messages", e);
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
        //TODO : think semaphore cases
        running.set(false);
        logger.error("Unexpected error occurred while receiving messages", error);
    }

    public void stopMessageReceiver() {
        //TODO : think semaphore cases
        this.running.set(false);
        this.kafkaConsumer.wakeup();
    }
}
