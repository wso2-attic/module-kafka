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

package org.ballerinalang.net.kafka.nativeimpl.actions.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.BStructType;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.util.codegen.PackageInfo;
import org.ballerinalang.util.codegen.StructInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;

/**
 * {@code }
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "poll",
        connectorName = Constants.CONSUMER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "timeout", type = TypeKind.INT)
        },
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRUCT,
                structPackage = "ballerina.net.kafka", structType = "ConsumerRecord"),
                @ReturnType(type = TypeKind.STRUCT)})
public class Poll extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(Poll.class);

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector consumerConnector = (BConnector) getRefArgument(context, 0);
        BStruct consumerStruct = ((BStruct) consumerConnector.getRefField(1));
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(Constants.NATIVE_CONSUMER);
        long timeout = getIntArgument(context, 0);
        List<BStruct> recordsList = new ArrayList<>();

//        public struct ConsumerRecord {
//            blob key;
//            blob value;
//            int offset;
//            int  partition;
//            int timestamp;
//            string topic;
//        }

        try {
            ConsumerRecords<byte[], byte[]> recordsRetrieved = kafkaConsumer.poll(timeout);
            if (!recordsRetrieved.isEmpty()) {
                recordsRetrieved.forEach(record -> {
                    //record.
                    BStruct recordStruct = createRecordStruct(context);
                    recordStruct.setBlobField(0, record.key());
                    recordStruct.setBlobField(1, record.value());
                    recordStruct.setIntField(0, record.partition());
                    recordStruct.setIntField(1, record.timestamp());
                    recordStruct.setStringField(0, record.topic());
                    recordsList.add(recordStruct);
                });
                context.getControlStackNew().getCurrentFrame().returnValues[0] =
                        new BRefValueArray(recordsList.toArray(new BRefType[0]), createRecordStruct(context).getType());

            }
        } catch (Exception e) {
            context.getControlStackNew().getCurrentFrame().returnValues[1] =
                    BLangVMErrors.createError(context, 0, e.getMessage());
        }

        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

    private BStruct createRecordStruct(Context context) {
        PackageInfo kafkaPackageInfo = context.getProgramFile()
                .getPackageInfo(Constants.KAFKA_NATIVE_PACKAGE);
        StructInfo consumerRecordStructInfo = kafkaPackageInfo.getStructInfo(Constants.CONSUMER_RECORD_STRUCT_NAME);
        BStructType structType = consumerRecordStructInfo.getType();
        BStruct bStruct = new BStruct(structType);
        return bStruct;
    }

}


