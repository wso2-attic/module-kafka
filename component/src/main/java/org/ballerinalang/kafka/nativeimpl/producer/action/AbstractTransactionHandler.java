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

package org.ballerinalang.kafka.nativeimpl.producer.action;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.transactions.TransactionLocalContext;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_PRODUCER_CONFIG;

/**
 * {@code AbstractCommitConsumer} is the base class for commit consumers.
 */
public abstract class AbstractTransactionHandler implements NativeCallableUnit {

    protected Context context;
    protected KafkaProducer<byte[], byte[]> producer;
    protected BMap<String, BValue> producerStruct;
    protected BMap<String, BValue> producerConnector;
    protected Properties producerProperties;

    boolean isTransactionalProducer() {
        return Objects.nonNull(
                producerProperties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
                && context.isInTransaction();
    }

    boolean isKafkaTransactionInitiated(TransactionLocalContext localTransactionInfo, String connectorKey) {
        return Objects.nonNull(localTransactionInfo.getTransactionContext(connectorKey));
    }

    void initializeClassVariables() {
        producerConnector = (BMap<String, BValue>) context.getRefArgument(0);
        BMap producerMap = (BMap) producerConnector.get("producerHolder");
        producerStruct = (BMap<String, BValue>) producerMap.get(new BString(NATIVE_PRODUCER));
        producer = (KafkaProducer) producerStruct.getNativeData(NATIVE_PRODUCER);
        producerProperties = (Properties) producerStruct.getNativeData(NATIVE_PRODUCER_CONFIG);
    }

    void commitConsumer(Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap, String groupID) {
        if (isTransactionalProducer()) {
            initiateTransaction();
        }
        producer.sendOffsetsToTransaction(partitionToMetadataMap, groupID);
    }

    void initiateTransaction() {
        String connectorKey = producerConnector.get("connectorID").stringValue();
        TransactionLocalContext localTransactionInfo = context.getLocalTransactionInfo();
        beginTransaction(localTransactionInfo, connectorKey);
    }

    private void beginTransaction(TransactionLocalContext localTransactionInfo, String connectorKey) {
        if (!isKafkaTransactionInitiated(localTransactionInfo, connectorKey)) {
            KafkaTransactionContext txContext = new KafkaTransactionContext(producer);
            localTransactionInfo.registerTransactionContext(connectorKey, txContext);
            producer.beginTransaction();
        }
    }

    @Override
    public boolean isBlocking() {
        return false;
    }
}
