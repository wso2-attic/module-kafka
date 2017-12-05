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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.BStructType;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.*;
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
        actionName = "getTopicPartitions",
        connectorName = Constants.CONSUMER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR)
        },
        returnType = { @ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRUCT, structType = "PartitionInfo",
                structPackage = "ballerina.net.kafka"),
                @ReturnType(type = TypeKind.STRUCT)})
public class GetTopicPartitions extends AbstractNativeAction {
    private static final Logger log = LoggerFactory.getLogger(GetTopicPartitions.class);

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector consumerConnector = (BConnector) getRefArgument(context, 0);
        String topic = getStringArgument(context, 0);

        BMap consumerMap = (BMap) consumerConnector.getRefField(1);
        BStruct consumerStruct = (BStruct) consumerMap.get(new BString(Constants.NATIVE_CONSUMER));

        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(Constants.NATIVE_CONSUMER);
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        List<BStruct> infoList = new ArrayList<>();
        if (!partitionInfos.isEmpty()) {
            partitionInfos.forEach(partitionInfo -> {
//                public struct PartitionInfo {
//                    string  topic;
//                    int partition;
//                    int leader;
//                    int  replicas;
//                    int  isr;
//                }
                BStruct infoStruct = createRecordStruct(context);
                infoStruct.setStringField(0, partitionInfo.topic());
                infoStruct.setIntField(0, partitionInfo.partition());
                infoStruct.setIntField(1, partitionInfo.leader().id());
                infoStruct.setIntField(2, partitionInfo.replicas().length);
                infoStruct.setIntField(3, partitionInfo.inSyncReplicas().length);
                infoList.add(infoStruct);
            });
            context.getControlStackNew().getCurrentFrame().returnValues[0] =
                    new BRefValueArray(infoList.toArray(new BRefType[0]), createRecordStruct(context).getType());

        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

    private BStruct createRecordStruct(Context context) {
        PackageInfo kafkaPackageInfo = context.getProgramFile()
                .getPackageInfo(Constants.KAFKA_NATIVE_PACKAGE);
        StructInfo consumerRecordStructInfo = kafkaPackageInfo
                .getStructInfo(Constants.CONSUMER_PARTITION_INFO_STRUCT_NAME);
        BStructType structType = consumerRecordStructInfo.getType();
        BStruct bStruct = new BStruct(structType);
        return bStruct;
    }

}
