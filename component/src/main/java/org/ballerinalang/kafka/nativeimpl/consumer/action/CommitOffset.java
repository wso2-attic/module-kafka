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

package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native function commits given offsets of consumer to offset topic.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "commitOffset",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true
)
public class CommitOffset extends AbstractApisWithDuration {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        this.consumer = getKafkaConsumer();

        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = getPartitionToMetadataMap();

        long apiTimeout = context.getIntArgument(0);
        long defaultApiTimeout = getDefaultApiTimeout();

        try {
            if (apiTimeout > DURATION_UNDEFINED_VALUE) { // API timeout should given the priority over the default value
                consumerCommitSyncWithDuration(partitionToMetadataMap, apiTimeout);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                consumerCommitSyncWithDuration(partitionToMetadataMap, defaultApiTimeout);
            } else {
                this.consumer.commitSync(partitionToMetadataMap);
            }
        } catch (IllegalArgumentException | KafkaException e) {
            context.setReturnValues(createError(context, "Failed to commit offsets. " + e.getMessage()));
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getPartitionToMetadataMap() {
        Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
        BRefValueArray offsets = ((BRefValueArray) this.context.getRefArgument(1));
        BMap<String, BValue> offset;
        int offsetValue;
        TopicPartition topicPartition;

        if (Objects.nonNull(offsets)) {
            for (int counter = 0; counter < offsets.size(); counter++) {
                offset = (BMap<String, BValue>) offsets.get(counter);
                offsetValue = ((BInteger) offset.get("offset")).value().intValue();
                topicPartition = getTopicPartitionFromOffsetBMap(offset);
                partitionToMetadataMap.put(topicPartition, new OffsetAndMetadata(offsetValue));
            }
        }
        return partitionToMetadataMap;
    }

    private TopicPartition getTopicPartitionFromOffsetBMap(BMap<String, BValue> offset) {
        BMap<String, BValue> partition;
        partition = (BMap<String, BValue>) offset.get("partition");
        return getTopicPartition(partition);
    }

    private void consumerCommitSyncWithDuration(Map<TopicPartition, OffsetAndMetadata> metadataMap, long timeout) {
        Duration duration = getDurationFromLong(timeout);
        this.consumer.commitSync(metadataMap, duration);
    }
}
