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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BVMExecutor;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BFunctionPointer;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.util.codegen.FunctionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.FULL_PACKAGE_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaUtils.createError;
import static org.ballerinalang.kafka.util.KafkaUtils.createPartitionList;

/**
 * Native function subscribes to given topic array
 * with given function pointers to on revoked / on assigned events.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = FULL_PACKAGE_NAME,
        functionName = "subscribeWithPartitionRebalance",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        isPublic = true
)
public class SubscribeWithPartitionRebalance implements NativeCallableUnit {

    @Override
    public void execute(Context context, CallableUnitCallback callableUnitCallback) {
        BMap<String, BValue> consumerStruct = (BMap<String, BValue>) context.getRefArgument(0);
        BValueArray topicArray = (BValueArray) context.getRefArgument(1);
        ArrayList<String> topics = new ArrayList<>();
        for (int counter = 0; counter < topicArray.size(); counter++) {
            topics.add(topicArray.getString(counter));
        }

        FunctionInfo onPartitionsRevoked = null;
        FunctionInfo onPartitionsAssigned = null;
        BValue partitionsRevoked = context.getRefArgument(2);
        BValue partitionsAssigned = context.getRefArgument(3);

        if (Objects.nonNull(partitionsRevoked) && partitionsRevoked instanceof BFunctionPointer) {
            onPartitionsRevoked = ((BFunctionPointer) context.getRefArgument(2)).value();
        } else {
            context.setReturnValues(createError(context,
                    "The onPartitionsRevoked function is not provided."));
        }

        if (Objects.nonNull(partitionsAssigned) && partitionsAssigned instanceof BFunctionPointer) {
            onPartitionsAssigned = ((BFunctionPointer) context.getRefArgument(3)).value();
        } else {
            context.setReturnValues(createError(context,
                    "The onPartitionsAssigned function is not provided."));
        }

        ConsumerRebalanceListener listener = new KafkaRebalanceListener(context, onPartitionsRevoked,
                onPartitionsAssigned, consumerStruct);

        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct.getNativeData(NATIVE_CONSUMER);

        try {
            kafkaConsumer.subscribe(topics, listener);
        } catch (IllegalArgumentException | IllegalStateException | KafkaException e) {
            context.setReturnValues(createError(context, e.getMessage()));
        }
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    /**
     * Implementation for {@link ConsumerRebalanceListener} interface from connector side.
     * We register this listener at subscription.
     * <p>
     * {@inheritDoc}
     */
    class KafkaRebalanceListener implements ConsumerRebalanceListener {

        private Context context;
        private FunctionInfo onPartitionsRevoked;
        private FunctionInfo onPartitionsAssigned;
        private BMap<String, BValue> consumerStruct;

        KafkaRebalanceListener(Context context,
                               FunctionInfo onPartitionsRevoked,
                               FunctionInfo onPartitionsAssigned,
                               BMap<String, BValue> consumerStruct) {
            this.context = context;
            this.onPartitionsRevoked = onPartitionsRevoked;
            this.onPartitionsAssigned = onPartitionsAssigned;
            this.consumerStruct = consumerStruct;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            BValue[] returnArgs = new BValue[]{consumerStruct, getPartitionsArray(partitions)};
            BVMExecutor.executeFunction(
                    onPartitionsRevoked.getPackageInfo().getProgramFile(),
                    onPartitionsRevoked,
                    returnArgs
            );
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            BValue[] returnArgs = new BValue[]{consumerStruct, getPartitionsArray(partitions)};
            BVMExecutor.executeFunction(
                    onPartitionsAssigned.getPackageInfo().getProgramFile(),
                    onPartitionsAssigned,
                    returnArgs
            );
        }

        private BValueArray getPartitionsArray(Collection<TopicPartition> partitions) {
            List<BMap<String, BValue>> assignmentList = createPartitionList(context, partitions);
            return new BValueArray(assignmentList.toArray(new BRefType[0]),
                    KafkaUtils.createKafkaPackageStruct(context, TOPIC_PARTITION_STRUCT_NAME).getType());
        }
    }
}

