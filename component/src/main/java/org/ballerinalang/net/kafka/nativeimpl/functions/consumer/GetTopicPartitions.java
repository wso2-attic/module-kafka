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

package org.ballerinalang.net.kafka.nativeimpl.functions.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;

import org.ballerinalang.model.types.BStructType;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.AbstractNativeFunction;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.natives.annotations.Receiver;
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
@BallerinaFunction(packageName = "ballerina.net.kafka",
        functionName = "getTopicPartitions",
        receiver = @Receiver(type = TypeKind.STRUCT, structType = "KafkaConsumer",
                structPackage = "ballerina.net.kafka"),
        args = {
                @Argument(name = "c",
                        type = TypeKind.STRUCT, structType = "KafkaConsumer",
                        structPackage = "ballerina.net.kafka")
        },
        returnType = { @ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRUCT, structType = "PartitionInfo",
                structPackage = "ballerina.net.kafka"),
                @ReturnType(type = TypeKind.STRUCT)},
        isPublic = true)
public class GetTopicPartitions extends AbstractNativeFunction {
    private static final Logger log = LoggerFactory.getLogger(GetTopicPartitions.class);

    @Override
    public BValue[] execute(Context context) {

        BConnector consumerConnector = (BConnector) getRefArgument(context, 0);
        String topic = getStringArgument(context, 0);

        BMap consumerMap = (BMap) consumerConnector.getRefField(1);
        BStruct consumerStruct = (BStruct) consumerMap.get(new BString(Constants.NATIVE_CONSUMER));

        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct
                .getNativeData(Constants.NATIVE_CONSUMER);

        try {
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
            }
            return getBValues(new BRefValueArray(infoList.toArray(new BRefType[0]),
                    createRecordStruct(context).getType()));
        } catch (KafkaException e) {
            return getBValues(null, BLangVMErrors.createError(context, 0, e.getMessage()));
        }
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
