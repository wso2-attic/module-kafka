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
import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.ballerinalang.kafka.util.KafkaConstants.OFFSET_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.TOPIC_PARTITION_STRUCT_NAME;

/**
 * {@code AbstractGetOffsets} is the base class for getting beginning/end offsets for a particular consumer.
 */
public abstract class AbstractGetOffsets extends AbstractApisWithDuration {

    protected Context context;
    protected KafkaConsumer<byte[], byte[]> consumer;

    public List<BMap<String, BValue>> getOffsetList(Map<TopicPartition, Long> offsetMap) {
        List<BMap<String, BValue>> offsetList = new ArrayList<>();
        if (!offsetMap.entrySet().isEmpty()) {
            for (Map.Entry<TopicPartition, Long> offset : offsetMap.entrySet()) {
                BMap<String, BValue> partitionStruct = getPartitionStruct(offset);
                BMap<String, BValue> offsetStruct = getOffsetStruct(partitionStruct, offset);
                offsetList.add(offsetStruct);
            }
        }
        return offsetList;
    }

    private BMap<String, BValue> getPartitionStruct(Map.Entry<TopicPartition, Long> offset) {
        BMap<String, BValue> partitionStruct = KafkaUtils.createKafkaPackageStruct(context,
                TOPIC_PARTITION_STRUCT_NAME);
        partitionStruct.put("topic", new BString(offset.getKey().topic()));
        partitionStruct.put("partition", new BInteger(offset.getKey().partition()));
        return partitionStruct;
    }

    private BMap<String, BValue> getOffsetStruct(BMap<String, BValue> partitionStruct,
                                                 Map.Entry<TopicPartition, Long> offset) {
        BMap<String, BValue> offsetStruct = KafkaUtils.createKafkaPackageStruct(context, OFFSET_STRUCT_NAME);
        offsetStruct.put("partition", partitionStruct);
        offsetStruct.put("offset", new BInteger(offset.getValue()));
        return offsetStruct;
    }
}
