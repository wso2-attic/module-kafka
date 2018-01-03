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

package org.ballerinalang.net.kafka.nativeimpl.actions.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.ballerinalang.bre.BallerinaTransactionContext;
import org.ballerinalang.bre.BallerinaTransactionManager;
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
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.net.kafka.transaction.KafkaTransactionContext;
import org.ballerinalang.util.exceptions.BallerinaException;


/**
 * Native action ballerina.net.kafka:send simple send which produces blob value to given string topic.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "send",
        connectorName = Constants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "value", type = TypeKind.BLOB),
                @Argument(name = "topic", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.NONE)})
public class Send extends AbstractNativeAction {

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector producerConnector = (BConnector) getRefArgument(context, 0);

        BStruct producerConf = ((BStruct) producerConnector.getRefField(0));
        BMap<String, BValue> producerBalConfig = (BMap<String, BValue>) producerConf.getRefField(0);

        BMap producerMap = (BMap) producerConnector.getRefField(1);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(Constants.NATIVE_PRODUCER));

        KafkaProducer kafkaProducer = (KafkaProducer) producerStruct.getNativeData(Constants.NATIVE_PRODUCER);

        String topic = getStringArgument(context, 0);
        byte[] value = getBlobArgument(context, 0);

        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord(topic, value);

        try {
            if (producerBalConfig.get(Constants.PARAM_TRANSACTION_ID) != null
                    && context.isInTransaction()) {
                String connectorKey = producerConnector.getStringField(0);
                BallerinaTransactionManager ballerinaTxManager = context.getBallerinaTransactionManager();
                BallerinaTransactionContext regTxContext = ballerinaTxManager.getTransactionContext(connectorKey);
                if (regTxContext == null) {
                    KafkaTransactionContext txContext = new KafkaTransactionContext(kafkaProducer);
                    ballerinaTxManager.registerTransactionContext(connectorKey, txContext);
                    kafkaProducer.beginTransaction();
                }
            }
            kafkaProducer.send(kafkaRecord);
        } catch (IllegalStateException | KafkaException e) {
            throw new BallerinaException("Failed to send message. " + e.getMessage(), e, context);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}

