package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.bre.bvm.CallableUnitCallback;
import org.ballerinalang.model.types.TypeKind;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.natives.annotations.Argument;
import org.ballerinalang.natives.annotations.BallerinaFunction;
import org.ballerinalang.natives.annotations.Receiver;
import org.ballerinalang.natives.annotations.ReturnType;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.ballerinalang.kafka.util.KafkaConstants.CONSUMER_STRUCT_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;
import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;
import static org.ballerinalang.kafka.util.KafkaConstants.ORG_NAME;
import static org.ballerinalang.kafka.util.KafkaConstants.PACKAGE_NAME;

/**
 * Native function returns given partition assignment for consumer.
 */
@BallerinaFunction(
        orgName = ORG_NAME,
        packageName = PACKAGE_NAME,
        functionName = "getAvailableTopics",
        receiver = @Receiver(type = TypeKind.OBJECT, structType = CONSUMER_STRUCT_NAME,
                structPackage = KAFKA_NATIVE_PACKAGE),
        args = @Argument(name = "duration", type = TypeKind.INT),
        returnType = {@ReturnType(type = TypeKind.ARRAY, elementType = TypeKind.STRING),
                @ReturnType(type = TypeKind.RECORD)},
        isPublic = true)

public class GetAvailableTopics extends AbstractApisWithDuration {

    @Override
    public void execute(Context context, CallableUnitCallback callback) {
        setContext(context);
        BMap<String, BValue> consumerStruct = (BMap<String, BValue>) context.getRefArgument(0);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = (KafkaConsumer) consumerStruct.getNativeData(NATIVE_CONSUMER);

        try {
            Map<String, List<PartitionInfo>> topics;
            BStringArray availableTopics = new BStringArray();

            long apiTimeout = context.getIntArgument(0);
            long defaultApiTimeout = getDefaultApiTimeout(consumerStruct);

            if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                Duration duration = getDurationFromLong(apiTimeout);
                topics = kafkaConsumer.listTopics(duration);
            } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                Duration duration = getDurationFromLong(defaultApiTimeout);
                topics = kafkaConsumer.listTopics(duration);
            } else {
                topics = kafkaConsumer.listTopics();
            }

            if (!topics.isEmpty()) {
                int i = 0;
                for (String topic : topics.keySet()) {
                    availableTopics.add(i++, topic);
                }
            }
            context.setReturnValues(availableTopics);
        } catch (KafkaException e) {
            context.setReturnValues(BLangVMErrors.createError(context, e.getMessage()));
        }

    }
}
