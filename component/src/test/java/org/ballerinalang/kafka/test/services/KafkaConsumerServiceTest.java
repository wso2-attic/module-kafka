/*
 *   Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.kafka.test.services;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.BServiceUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.ballerinalang.kafka.test.utils.Utils.KAFKA_BROKER_PORT;
import static org.ballerinalang.kafka.test.utils.Utils.ZOOKEEPER_PORT_1;

/**
 * Test cases for ballerina kafka consumer endpoint bind to a service .
 */
public class KafkaConsumerServiceTest {

    private CompileResult compileResult;
    private static File dataDir;
    private static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
    }

    @Test(description = "Test endpoint bind to a service")
    public void testKafkaServiceEndpoint() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service.bal");
        BServiceUtil.runService(compileResult);
        BRunUtil.invokeStateful(compileResult, "funcKafkaProduce");

        try {
            await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] returnBValues = BRunUtil.invokeStateful(compileResult, "funcKafkaGetResultText");
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BBoolean);
                return ((BBoolean) returnBValues[0]).booleanValue();
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(
            description = "Test endpoint bind to a service returning invalid return type",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".* Invalid return type for the resource function.*"
    )
    public void testKafkaServiceInvalidReturnType() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_invalid_return_type.bal");
    }

    @Test(
            description = "Test kafka service with an invalid resource name",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*Kafka service has invalid resource.*"
    )
    public void testKafkaServiceInvalidResourceName() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_invalid_resource_name.bal");
    }

    @Test(
            description = "Test kafka service with an invalid input parameter type",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*Resource parameter .* is invalid. Expected.*"
    )
    public void testKafkaServiceInvalidParameterType() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_invalid_parameter_type.bal");
    }

    @Test(description = "Test endpoint bind to a service returning valid return type")
    public void testKafkaServiceValidateReturnType() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_validate_return_type.bal");
        BServiceUtil.runService(compileResult);
        BRunUtil.invokeStateful(compileResult, "funcKafkaProduce");

        try {
            await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] returnBValues = BRunUtil.invokeStateful(compileResult, "funcKafkaGetResultText");
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BBoolean);
                return ((BBoolean) returnBValues[0]).booleanValue();
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(description = "Test kafka service stop() function")
    public void testKafkaServiceStop() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_stop.bal");
        BServiceUtil.runService(compileResult);
        BRunUtil.invokeStateful(compileResult, "funcKafkaProduce");
        try {
            await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] results = BRunUtil.invokeStateful(compileResult, "funcKafkaGetResult");
                Assert.assertEquals(results.length, 1);
                Assert.assertTrue(results[0] instanceof BBoolean);
                return ((BBoolean) results[0]).booleanValue();
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test(description = "Test endpoint bind to a service returning custom error type")
    public void testKafkaServiceValidateCustomErrorType() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_custom_error_return_type_validation.bal");
    }

    @Test(
            description = "Test endpoint bind to a service with no resources",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*No resources found to handle the Kafka records in.*",
            enabled = false // Disabled this test as currently no resources will not handle by kafka compiler plugin
    )
    public void testKafkaServiceNoResources() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_no_resources.bal");
    }

    @Test(
            description = "Test endpoint bind to a service with more than one resource",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*More than one resources found in Kafka service.*"
    )
    public void testKafkaServiceMoreThanOneResource() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_more_than_one_resource.bal");
    }

    @Test(
            description = "Test endpoint bind to a service with invalid number of arguments in resource function",
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*Invalid number of input parameters found in resource.*"
    )
    public void testKafkaServiceInvalidNumberOfArguments() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_invalid_number_of_arguments.bal");
    }

    @Test(description = "Test endpoint bind to a service")
    public void testKafkaAdvancedService() {
        compileResult = BCompileUtil.compileAndSetup("services/kafka_service_advanced.bal");
        BServiceUtil.runService(compileResult);
        BRunUtil.invokeStateful(compileResult, "funcKafkaProduce");

        try {
            await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> {
                BValue[] returnBValues = BRunUtil.invokeStateful(compileResult, "funcKafkaGetResultText");
                Assert.assertEquals(returnBValues.length, 1);
                Assert.assertTrue(returnBValues[0] instanceof BBoolean);
                return ((BBoolean) returnBValues[0]).booleanValue();
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }
    }

    @AfterClass
    public void tearDown() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
            boolean delete = dataDir.delete();
            // If files are still locked and a test fails: delete on exit to allow subsequent test execution
            if (!delete) {
                dataDir.deleteOnExit();
            }
        }
    }

    private static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-consumer-service-test");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(ZOOKEEPER_PORT_1, KAFKA_BROKER_PORT);
        return kafkaCluster;
    }
}
