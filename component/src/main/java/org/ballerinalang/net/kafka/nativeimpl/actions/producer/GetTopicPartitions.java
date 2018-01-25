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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.connector.api.AbstractNativeAction;
import org.ballerinalang.connector.api.ConnectorFuture;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BConnector;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.nativeimpl.actions.ClientConnectorFuture;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaAction;
import org.ballerinalang.natives.annotations.ReturnType;
import org.ballerinalang.net.kafka.KafkaConstants;
import org.ballerinalang.net.kafka.KafkaUtils;
import org.ballerinalang.util.exceptions.BallerinaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Native action ballerina.net.kafka:getTopicPartitions retrieves partitions for given Topic via remote call.
 */
@BallerinaAction(packageName = "ballerina.net.kafka",
        actionName = "getTopicPartitions",
        connectorName = KafkaConstants.PRODUCER_CONNECTOR_NAME,
        args = {
                @Argument(name = "c",
                        type = TypeKind.CONNECTOR),
                @Argument(name = "topic", type = TypeKind.STRING)
        },
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRUCT, structType = "TopicPartition",
                structPackage = "ballerina.net.kafka")})
public class GetTopicPartitions extends AbstractNativeAction {

    @Override
    public ConnectorFuture execute(Context context) {

        BConnector producerConnector = (BConnector) getRefArgument(context, 0);
        String topic = getStringArgument(context, 0);

        BMap producerMap = (BMap) producerConnector.getRefField(2);
        BStruct producerStruct = (BStruct) producerMap.get(new BString(KafkaConstants.NATIVE_PRODUCER));

        try {

            KafkaProducer<byte[], byte[]> kafkaProducer =
                    (KafkaProducer) producerStruct.getNativeData(KafkaConstants.NATIVE_PRODUCER);
            List<PartitionInfo> partitionInfos = kafkaProducer.partitionsFor(topic);
            List<BStruct> infoList = new ArrayList<>();
            if (!partitionInfos.isEmpty()) {
                partitionInfos.forEach(partitionInfo -> {
                    BStruct infoStruct = KafkaUtils.createKafkaPackageStruct(context,
                            KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
                    infoStruct.setStringField(0, partitionInfo.topic());
                    infoStruct.setIntField(0, partitionInfo.partition());
                    infoList.add(infoStruct);
                });
                context.getControlStackNew().getCurrentFrame().returnValues[0] =
                        new BRefValueArray(infoList.toArray(new BRefType[0]),
                                KafkaUtils.createKafkaPackageStruct(context,
                                        KafkaConstants.TOPIC_PARTITION_STRUCT_NAME).getType());
            }
        } catch (KafkaException e) {
            throw new BallerinaException("Failed to fetch partitions from the producer " + e.getMessage(), e, context);
        }
        ClientConnectorFuture future = new ClientConnectorFuture();
        future.notifySuccess();
        return future;
    }

}
