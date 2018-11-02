package org.ballerinalang.kafka.nativeimpl.consumer.action;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.ballerinalang.bre.Context;
import org.ballerinalang.bre.bvm.BLangVMErrors;
import org.ballerinalang.kafka.util.KafkaUtils;
import org.ballerinalang.model.NativeCallableUnit;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BValue;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * {@code AbstractApisWithDuration} is the base class for handle APIs with optional duration parameter.
 */
public abstract class AbstractApisWithDuration implements NativeCallableUnit {

    private Context context;
    protected static final long DURATION_UNDEFINED_VALUE = -1;

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

        Properties consumerProperties = getConsumerProperties(consumerStruct);
        if (isDefaultApiTimeoutDefined(consumerProperties)) {
            duration = getDefaultApiTimeoutConsumerConfig(consumerProperties);
        } else {
            duration = -1;
        }
        return duration;
    }

    protected int getDefaultApiTimeoutConsumerConfig(Properties consumerProperties) {
        int apiTimeoutValue = (int) consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        return apiTimeoutValue;
    }

    protected boolean isDefaultApiTimeoutDefined(Properties consumerProperties) {
        Object defaultApiTimeout = consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        return Objects.nonNull(defaultApiTimeout);
    }

    private Properties getConsumerProperties(BMap<String, BValue> consumerStruct) {
        BMap<String, BValue> consumerConfig = (BMap<String, BValue>) consumerStruct.get("config");
        // Check whether consumer configuration is available.
        if (Objects.isNull(consumerConfig)) {
            context.setReturnValues(BLangVMErrors.
                    createError(context,
                            "Kafka consumer is not initialized with consumer configuration."));
        }
        Properties consumerProperties = KafkaUtils.processKafkaConsumerConfig(consumerConfig);
        return consumerProperties;
    }

    @Override
    public boolean isBlocking() {

        return true;
    }
}
