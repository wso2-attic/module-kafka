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
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.time.Duration;
import java.util.HashMap;
import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_OFFSET;
import static org.ballerinalang.kafka.util.KafkaConstants.ALIAS_PARTITION;
import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.FULL_PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.OFFSET_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native function returns committed offset for given partition.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = FULL_PACKAGE_NAME,
        functionName = "getCommittedOffset",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true
)
public class GetCommittedOffset extends AbstractApisWithDuration {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        this.consumer = getKafkaConsumer();

        long apiTimeout = context.getIntArgument(0);
        long defaultApiTimeout = getDefaultApiTimeout();

        BMap<String, BValue> partition = (BMap<String, BValue>) context.getRefArgument(1);
        TopicPartition topicPartition = getTopicPartition(partition);

        try {
            OffsetAndMetadata offsetAndMetadata;
            if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                offsetAndMetadata = getOffsetAndMetadataWithDuration(topicPartition, apiTimeout);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                offsetAndMetadata = getOffsetAndMetadataWithDuration(topicPartition, defaultApiTimeout);
            } else {
                offsetAndMetadata = this.consumer.committed(topicPartition);
            }
            BMap<String, BValue> offset = KafkaUtils.createKafkaPackageStruct(context, OFFSET_STRUCT_NAME);
            offset.put(ALIAS_PARTITION, partition.copy(new HashMap<>()));

            if (Objects.nonNull(offsetAndMetadata)) {
                offset.put(ALIAS_OFFSET, new BInteger(offsetAndMetadata.offset()));
            }
            context.setReturnValues(offset);
        } catch (KafkaException e) {
            context.setReturnValues(createError(context, "Failed to get committed offsets: " + e.getMessage()));
        }
    }

    private OffsetAndMetadata getOffsetAndMetadataWithDuration(TopicPartition topicPartition, long timeout) {
        Duration duration = getDurationFromLong(timeout);
        return this.consumer.committed(topicPartition, duration);
    }
}
