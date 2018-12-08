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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.BTypes;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;

/**
 * Native function returns given partition assignment for consumer.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getAvailableTopics",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true
)

public class GetAvailableTopics extends AbstractApisWithDuration {

    @Override
    public void execute(Context context, CallableUnitCallback callback) {
        this.context = context;
        this.consumer = getKafkaConsumer();

        Map<String, List<PartitionInfo>> topics;

        long apiTimeout = context.getIntArgument(0);
        long defaultApiTimeout = getDefaultApiTimeout();

        try {
            if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                topics = getAvailableTopicWithDuration(this.consumer, apiTimeout);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                topics = getAvailableTopicWithDuration(this.consumer, defaultApiTimeout);
            } else {
                topics = this.consumer.listTopics();
            }
            BValueArray availableTopics = getBValueArrayFromMap(topics);
            context.setReturnValues(availableTopics);
        } catch (KafkaException e) {
            context.setReturnValues(createError(context, "Failed to retrieve available topics: " + e.getMessage()));
        }
    }

    private Map<String, List<PartitionInfo>> getAvailableTopicWithDuration(
            KafkaConsumer<byte[], byte[]> kafkaConsumer, long timeout) {
        Duration duration = getDurationFromLong(timeout);
        return kafkaConsumer.listTopics(duration);
    }

    private BValueArray getBValueArrayFromMap(Map<String, List<PartitionInfo>> map) {
        BValueArray bValueArray = new BValueArray(BTypes.typeString);
        if (!map.keySet().isEmpty()) {
            int i = 0;
            for (String topic : map.keySet()) {
                bValueArray.add(i++, topic);
            }
        }
        return bValueArray;
    }
}
