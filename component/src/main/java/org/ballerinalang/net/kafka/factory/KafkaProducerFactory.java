/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ballerinalang.net.kafka.factory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.ballerinalang.net.kafka.api.ProducerFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code }
 */
public class KafkaProducerFactory<K, V> implements ProducerFactory<K, V> {

	private static final int DEFAULT_PHYSICAL_CLOSE_TIMEOUT = 30;

	private static final Log logger = LogFactory.getLog(KafkaProducerFactory.class);

	private final Map<String, Object> configs;

	private final AtomicInteger transactionIdSuffix = new AtomicInteger();

	private final BlockingQueue<CloseSafeProducer<K, V>> cache = new LinkedBlockingQueue<>();

	private volatile CloseSafeProducer<K, V> producer;

	private Serializer<K> keySerializer;

	private Serializer<V> valueSerializer;

	private int physicalCloseTimeout = DEFAULT_PHYSICAL_CLOSE_TIMEOUT;

	private String transactionIdPrefix;

	private volatile boolean running;

	public KafkaProducerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	public KafkaProducerFactory(Map<String, Object> configs, Serializer<K> keySerializer,
								Serializer<V> valueSerializer) {
		this.configs = new HashMap<>(configs);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	public void setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public void setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
	 * The time to wait when physically closing the producer (when {@link #stop()} or {@link #destroy()} is invoked).
	 * Specified in seconds; default {@value #DEFAULT_PHYSICAL_CLOSE_TIMEOUT}.
	 * @param physicalCloseTimeout the timeout in seconds.
	 * @since 1.0.7
	 */
	public void setPhysicalCloseTimeout(int physicalCloseTimeout) {
		this.physicalCloseTimeout = physicalCloseTimeout;
	}

	/**
	 * Set the transactional.id prefix.
	 * @param transactionIdPrefix the prefix.
	 * @since 1.3
	 */
	public void setTransactionIdPrefix(String transactionIdPrefix) {
		this.transactionIdPrefix = transactionIdPrefix;
	}

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 1.3
	 */
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public boolean transactionCapable() {
		return this.transactionIdPrefix != null;
	}

	public void destroy() throws Exception { //NOSONAR
		CloseSafeProducer<K, V> producer = this.producer;
		this.producer = null;
		if (producer != null) {
			producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
		}
		producer = this.cache.poll();
		while (producer != null) {
			try {
				producer.delegate.close(this.physicalCloseTimeout, TimeUnit.SECONDS);
			}
			catch (Exception e) {
				logger.error("Exception while closing producer", e);
			}
			producer = this.cache.poll();
		}
	}

	public void start() {
		this.running = true;
	}


	public void stop() {
		try {
			destroy();
		}
		catch (Exception e) {
			logger.error("Exception while closing producer", e);
		}
	}

	public boolean isRunning() {
		return this.running;
	}

	@Override
	public Producer<K, V> createProducer() {
		if (this.transactionIdPrefix != null) {
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
		return new KafkaProducer<K, V>(this.configs, this.keySerializer, this.valueSerializer);
	}

	protected Producer<K, V> createTransactionalProducer() {
		Producer<K, V> producer = this.cache.poll();
		if (producer == null) {
			Map<String, Object> configs = new HashMap<>(this.configs);
			configs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
					this.transactionIdPrefix + this.transactionIdSuffix.getAndIncrement());
			producer = new KafkaProducer<K, V>(configs, this.keySerializer, this.valueSerializer);
			producer.initTransactions();
			return new CloseSafeProducer<K, V>(producer, this.cache);
		}
		else {
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
		public void beginTransaction() throws ProducerFencedException {
			this.delegate.beginTransaction();
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
				throws ProducerFencedException {
			this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
		}

		@Override
		public void commitTransaction() throws ProducerFencedException {
			this.delegate.commitTransaction();
		}

		@Override
		public void abortTransaction() throws ProducerFencedException {
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
