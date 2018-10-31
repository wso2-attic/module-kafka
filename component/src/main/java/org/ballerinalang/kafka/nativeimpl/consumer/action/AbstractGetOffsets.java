package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.common.TopicPartition;
import org.ballerinalang.bre.Context;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
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

    private Context context;

    public Context getContext() {

        return context;
    }

    public void setContext(Context context) {

        this.context = context;
    }

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
