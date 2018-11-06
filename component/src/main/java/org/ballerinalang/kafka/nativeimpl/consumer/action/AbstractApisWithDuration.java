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
 * <p>
 * APIs which extends this class are now have a default parameter, `duration` which is used as the timeout of these APIs
 * to execute. In the consumer config, there's a value `defaultApiTimeout` which is used as the default value, if user
 * does not provide a duration. It can be overridden using `duration` parameter. If you do not want to provide a
 * duration, simply set the value of configuration `defaultApiTimeout` to a negative int.
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
        return Duration.ofMillis(value);
    }

    protected long getDefaultApiTimeout(BMap<String, BValue> consumerStruct) {
        long duration;
        Properties consumerProperties = getConsumerProperties(consumerStruct);
        duration = isDefaultApiTimeoutDefined(consumerProperties) ?
                getDefaultApiTimeoutConsumerConfig(consumerProperties) : DURATION_UNDEFINED_VALUE;
        return duration;
    }

    protected int getDefaultApiTimeoutConsumerConfig(Properties consumerProperties) {
        return (int) consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
    }

    protected boolean isDefaultApiTimeoutDefined(Properties consumerProperties) {
        return Objects.nonNull(consumerProperties.get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
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
