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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.util.HashMap;
import java.util.Map;

import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_OFFSET;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_PARTITION;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_TOPIC;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PRODUCER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native action commits the consumer for given offsets in transaction.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "commitConsumerOffsets",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = PRODUCER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE)
)
public class CommitConsumerOffsets extends AbstractTransactionHandler {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        initializeClassVariables();
        BValueArray offsets = ((BValueArray) context.getRefArgument(1));
        String groupID = context.getStringArgument(0);
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();

        for (int counter = 0; counter < offsets.size(); counter++) {
            BMap<String, BValue> offset = (BMap<String, BValue>) offsets.getRefValue(counter);
            int offsetValue = ((BInteger) offset.get(ALIAS_OFFSET)).value().intValue();
            TopicPartition topicPartition = createTopicPartitionFromPartitionOffset(offset);
            partitionToMetadataMap.put(topicPartition, new OffsetAndMetadata(offsetValue));
        }
        try {
            commitConsumer(partitionToMetadataMap, groupID);
        } catch (IllegalStateException | KafkaException e) {
            context.setReturnValues(createError(context, "Failed to commit consumer offsets. " + e.getMessage()));
        }
        callableUnitCallback.notifySuccess();
    }

    private TopicPartition createTopicPartitionFromPartitionOffset(BMap<String, BValue> offset) {
        BMap<String, BValue> partition = (BMap<String, BValue>) offset.get(ALIAS_PARTITION);
        String topic = partition.get(ALIAS_TOPIC).stringValue();
        int partitionValue = ((BInteger) partition.get(ALIAS_PARTITION)).value().intValue();

        return new TopicPartition(topic, partitionValue);
    }
}
