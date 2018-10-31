package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ballerinalang.bre.Context;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.ballerinalang.kafka.util.KafkaConstants.NATIVE_CONSUMER;

public abstract class AbstractApisWithDuration implements NativeCallableUnit {

    private Context context;
    protected static final long durationUndefinedValue = -1;

    public void setContext(Context context) {
        this.context = context;
    }

    public Context getContext() {
        return context;
    }

    protected Duration getDurationFromLong(long value) {
        Duration duration = Duration.ofMillis(value);
        return duration;
    }

    protected long getDefaultApiTimeout(BMap<String, BValue> consumerStruct) {
        long duration;
        Properties consumerProperties = (Properties) consumerStruct.getNativeData(NATIVE_CONSUMER);
        if (isDefaultApiTimeoutDefined(consumerProperties)) {
            String apiTimeoutValue = getDefaultApiTimeoutConsumerConfig(consumerProperties);
            duration = Long.parseLong(apiTimeoutValue);
        } else {
            duration = -1;
        }
        return duration;
    }

    protected String getDefaultApiTimeoutConsumerConfig(Properties consumerProperties) {
        String apiTimeoutValue = (String) consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        return apiTimeoutValue;
    }

    protected boolean isDefaultApiTimeoutDefined(Properties consumerProperties) {
        Object defaultApiTimeout = consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        return Objects.nonNull(defaultApiTimeout);
    }

    @Override
    public boolean isBlocking() {

        return true;
    }
}
