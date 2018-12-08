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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.OFFSET_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native function returns beginning offsets for given partition array.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getBeginningOffsets",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true
)
public class GetBeginningOffsets extends AbstractGetOffsets {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        this.context = context;
        this.consumer = getKafkaConsumer();
        long apiTimeout = context.getIntArgument(0);
        long defaultApiTimeout = getDefaultApiTimeout();

        try {
            BValueArray partitions = ((BValueArray) context.getRefArgument(1));
            ArrayList<TopicPartition> partitionList = KafkaUtils.getTopicPartitionList(partitions);
            Map<TopicPartition, Long> offsetMap;

            if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                offsetMap = getBeginningOffsetsWithDuration(partitionList, apiTimeout);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                offsetMap = getBeginningOffsetsWithDuration(partitionList, defaultApiTimeout);
            } else {
                offsetMap = consumer.beginningOffsets(partitionList);
            }
            List<BMap<String, BValue>> offsetList = super.getOffsetList(offsetMap);
            context.setReturnValues(new BValueArray(offsetList.toArray(new BRefType[0]),
                    KafkaUtils.createKafkaPackageStruct(context, OFFSET_STRUCT_NAME).getType()));
        } catch (KafkaException e) {
            context.setReturnValues(createError(context, "Failed to get beginning offsets: " + e.getMessage()));
        }
    }

    private Map<TopicPartition, Long> getBeginningOffsetsWithDuration(
            ArrayList<TopicPartition> partitions, long timeout) {
        Duration duration = getDurationFromLong(timeout);
        return this.consumer.beginningOffsets(partitions, duration);
    }
}
