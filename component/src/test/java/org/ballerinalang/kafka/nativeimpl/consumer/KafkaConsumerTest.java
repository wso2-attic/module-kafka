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

package org.ballerinalang.kafka.nativeimpl.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.types.TypeTags;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Test cases for ballerina.kafka consumer native functions.
 */
public class KafkaConsumerTest {

    private CompileResult result;
    private static File dataDir;
    private static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        result = BCompileUtil.compile("consumer/kafka_consumer.bal");
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
        kafkaCluster.createTopic("test", 2, 1);
    }

    @Test(description = "Test Basic consumer polling with subscription and assignment retrieval")
    public void testKafkaConsume() {
        CountDownLatch completion = new CountDownLatch(1);
        kafkaCluster.useTo().produceStrings("test", 10, completion::countDown, () -> "test_string");
        try {
            completion.await();
        } catch (Exception ex) {
            //Ignore
        }
        BValue[] inputBValues = {};
        BValue[] returnBValues = BRunUtil.invoke(result, "funcKafkaConnect", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BMap);
        // adding kafka endpoint as the input parameter
        inputBValues = new BValue[]{returnBValues[0]};
        BValue[] inputs = new BValue[]{returnBValues[0]};

        try {
            await().atMost(10000, TimeUnit.MILLISECONDS).until(() -> {
                int msgCount = 0;
                while (true) {
                    BValue[] results = BRunUtil.invoke(result, "funcKafkaPoll", inputs);
                    Assert.assertEquals(results.length, 1);
                    Assert.assertTrue(results[0] instanceof BInteger);
                    msgCount = msgCount + new Long(((BInteger) results[0]).intValue()).intValue();
                    if (msgCount == 10) {
                        return true;
                    }
                }
            });
        } catch (Throwable e) {
            Assert.fail(e.getMessage());
        }

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetSubscription", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BStringArray);
        Assert.assertEquals(((BStringArray) returnBValues[0]).size(), 1);
        Assert.assertEquals(((BStringArray) returnBValues[0]).get(0), "test");

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetAssignment", inputBValues);

        Assert.assertEquals(returnBValues.length, 2);
        Assert.assertEquals((returnBValues[0]).getType().getTag(), TypeTags.RECORD_TYPE_TAG);
        Assert.assertEquals(((BMap) returnBValues[0]).get("topic").stringValue(), "test");

        returnBValues = BRunUtil.invoke(result, "funcKafkaClose", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BBoolean);
        Assert.assertEquals(((BBoolean) returnBValues[0]).booleanValue(), true);

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
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-consumer");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2185, 9094);
        return kafkaCluster;
    }

}
