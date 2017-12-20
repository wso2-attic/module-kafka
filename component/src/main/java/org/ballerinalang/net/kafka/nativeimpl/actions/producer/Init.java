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
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.util.exceptions.BallerinaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Native action ballerina.net.kafka:<init> hidden action which initializes a producer instance for connector.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
                 actionName = "<init>",
                 connectorName = Constants.PRODUCER_CONNECTOR_NAME,
                 args = {
                         @Argument(name = "c",
                                   type = TypeKind.CONNECTOR)
                 })
public class Init extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(Init.class);

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector producerConnector = (BConnector) getRefArgument(context, 0);
        BStruct producerStruct = ((BStruct) producerConnector.getRefField(0));
        BMap<String, BValue> producerBalConfig = (BMap<String, BValue>) producerStruct.getRefField(0);

        Properties producerProperties = KafkaUtils.processKafkaProducerConfig(producerBalConfig);

        //TODO make configurable
        //TODO ADD Kafka Exception
        //TODO change sample

        try {
            KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);

            BMap producerMap = (BMap) producerConnector.getRefField(1);
            producerStruct.addNativeData(Constants.NATIVE_PRODUCER, kafkaProducer);
            producerMap.put(new BString(Constants.NATIVE_PRODUCER), producerStruct);
        } catch (KafkaException e) {
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

