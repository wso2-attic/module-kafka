/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.net.kafka.nativeimpl.actions.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.Properties;

/**
 * Native action ballerina.net.kafka:<init> hidden action which initializes a producer instance for connector.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
                 actionName = "<init>",
                 connectorName = KafkaConstants.PRODUCER_CONNECTOR_NAME,
                 args = {
                         @Argument(name = "c",
                                   type = TypeKind.CONNECTOR)
                 })
public class Init extends AbstractNativeAction {

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector producerConnector = (BConnector) getRefArgument(context, 0);

        BStringArray stringArray = (BStringArray) producerConnector.getRefField(0);
        String bootstrapServers;
        if (stringArray.size() != 1) {
            throw new BallerinaException("Mandatory Kafka bootstrap servers parameter size should be 1.");
        } else {
            bootstrapServers = stringArray.get(0);
        }

        BStruct producerConf = (BStruct) producerConnector.getRefField(1);
        Properties producerProperties = KafkaUtils.processKafkaProducerConfig(producerConf);

        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try {
            KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(producerProperties);
            if (producerProperties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) != null) {
                kafkaProducer.initTransactions();
            }

            BMap producerMap = (BMap) producerConnector.getRefField(2);
            BStruct producerStruct = KafkaUtils.createKafkaPackageStruct(context,
                    KafkaConstants.PRODUCER_STRUCT_NAME);
            producerStruct.addNativeData(KafkaConstants.NATIVE_PRODUCER, kafkaProducer);
            producerStruct.addNativeData(KafkaConstants.NATIVE_PRODUCER_CONFIG, producerProperties);

            producerMap.put(new BString(KafkaConstants.NATIVE_PRODUCER), producerStruct);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to initialize the producer " + e.getMessage(), e, context);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

    @Override
    public boolean isNonBlockingAction() {
        return false;
    }

}

