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
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code }
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "send",
        connectorName = Constants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "record", type = TypeKind.STRUCT, structType = "ProducerRecord",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = {@ReturnType(type = TypeKind.STRUCT)})
public class Send extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(Send.class);

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector producerConnector = (BConnector) getRefArgument(context, 0);
        //BStruct consumerStruct = ((BStruct) producerConnector.getRefField(1));
        BMap producerMap = (BMap) producerConnector.getRefField(1);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(Constants.NATIVE_PRODUCER));

        KafkaProducer kafkaProducer = (KafkaProducer) producerStruct.getNativeData(Constants.NATIVE_PRODUCER);
        BStruct producerRecord = ((BStruct) getRefArgument(context, 1));


//        public struct ProducerRecord {
//            blob key;
//            blob value;
//            string topic;
//            int  partition;
//            int timestamp;
//        }

        //TODO: validate params
        byte[] key = producerRecord.getBlobField(0);
        byte[] value = producerRecord.getBlobField(1);
        String topic = producerRecord.getStringField(0);


        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<byte[], byte[]>(topic, key, value);

        try {
            kafkaProducer.send(kafkaRecord).get();
        } catch (Exception e) {
            context.getControlStackNew().getCurrentFrame().returnValues[0] =
                    BLangVMErrors.createError(context, 0, e.getMessage());
        }

        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}

