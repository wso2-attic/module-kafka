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

package org.ballerinalang.net.kafka.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.ballerinalang.bre.BallerinaTransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAResource;

/**
 * {@code }
 */
public class KafkaTransactionContext implements BallerinaTransactionContext {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionContext.class);
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public KafkaTransactionContext(KafkaProducer<byte[], byte[]> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void commit() {
        this.kafkaProducer.commitTransaction();
    }

    @Override
    public void rollback() {
        this.kafkaProducer.abortTransaction();
    }

    @Override
    public void close() {
    }

    @Override
    public void done() {
    }

    @Override
    public XAResource getXAResource() {
        return null;
    }

}
