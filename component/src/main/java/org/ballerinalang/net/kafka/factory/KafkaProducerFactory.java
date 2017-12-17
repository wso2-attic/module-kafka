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
package org.ballerinalang.net.kafka.factory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.net.kafka.api.ProducerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code }
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public class KafkaProducerFactory<K, V> implements ProducerFactory<K, V> {

    private static final Log log = LogFactory.getLog(KafkaProducerFactory.class);

    private final Properties configs;

    private final BlockingQueue<CloseSafeProducer<K, V>> cache = new LinkedBlockingQueue<>();

    private volatile CloseSafeProducer<K, V> producer;

    private String transactionId;

    private final AtomicInteger transactionIdSuffix = new AtomicInteger();

    public KafkaProducerFactory(Properties configs) {
        this.configs = configs;
        if (configs.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG) != null) {

        }
    }

    @Override
    public Properties getConfigurationProperties() {
        return configs;
    }

    @Override
    public boolean transactionCapable() {
        return this.transactionId != null;
    }

    public void destroy() throws Exception {
        CloseSafeProducer<K, V> producer = this.producer;
        this.producer = null;
        if (producer != null) {
            producer.delegate.close();
        }
        producer = this.cache.poll();
        while (producer != null) {
            try {
                producer.delegate.close();
            } catch (Exception e) {
                log.error("Exception while closing producer", e);
            }
            producer = this.cache.poll();
        }
    }

    @Override
    public Producer<K, V> createProducer() {
        if (this.transactionId != null) {
            return createTransactionalProducer();
        }
        if (this.producer == null) {
            synchronized (this) {
                if (this.producer == null) {
                    this.producer = new CloseSafeProducer<K, V>(createKafkaProducer());
                }
            }
        }
        return this.producer;
    }

    protected Producer<K, V> createKafkaProducer() {
        return new KafkaProducer<K, V>(this.configs);
    }

    protected Producer<K, V> createTransactionalProducer() {
        Producer<K, V> producer = this.cache.poll();
        if (producer == null) {
            configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    this.transactionId + this.transactionIdSuffix.getAndIncrement());
            producer = new KafkaProducer<K, V>(configs);
            producer.initTransactions();
            return new CloseSafeProducer<K, V>(producer, this.cache);
        } else {
            return producer;
        }
    }

    private static class CloseSafeProducer<K, V> implements Producer<K, V> {

        private final Producer<K, V> delegate;

        private final BlockingQueue<CloseSafeProducer<K, V>> cache;

        CloseSafeProducer(Producer<K, V> delegate) {
            this(delegate, null);
        }

        CloseSafeProducer(Producer<K, V> delegate, BlockingQueue<CloseSafeProducer<K, V>> cache) {
            this.delegate = delegate;
            this.cache = cache;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
            return this.delegate.send(record);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
            return this.delegate.send(record, callback);
        }

        @Override
        public void flush() {
            this.delegate.flush();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return this.delegate.partitionsFor(topic);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return this.delegate.metrics();
        }

        @Override
        public void initTransactions() {
            this.delegate.initTransactions();
        }

        @Override
        public void beginTransaction() {
            this.delegate.beginTransaction();
        }

        @Override
        public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
            this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
        }

        @Override
        public void commitTransaction() {
            this.delegate.commitTransaction();
        }

        @Override
        public void abortTransaction() {
            this.delegate.abortTransaction();
        }

        @Override
        public void close() {
            if (this.cache != null) {
                this.cache.offer(this);
            }
        }

        @Override
        public void close(long timeout, TimeUnit unit) {
            close();
        }

        @Override
        public String toString() {
            return "CloseSafeProducer [delegate=" + this.delegate + "]";
        }

    }

}
