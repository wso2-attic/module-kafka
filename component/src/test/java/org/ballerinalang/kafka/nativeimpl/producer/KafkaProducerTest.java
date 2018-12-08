/*
 *   Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.ballerinalang.kafka.nativeimpl.producer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BString;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test cases for ballerina.net.kafka producer connector.
 */
@Test(singleThreaded = true)
public class KafkaProducerTest {

    private CompileResult result;
    private static File dataDir;
    protected static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
    }

    @Test(description = "Test Producer close() action")
    public void testKafkaProducerClose() {
        result = BCompileUtil.compile("producer/kafka_producer_close.bal");
        BValue[] inputBValues = {};
        BValue[] returnBValue = BRunUtil.invoke(result, "funcTestKafkaClose", inputBValues);
        Assert.assertEquals(returnBValue.length, 1);
        Assert.assertTrue(returnBValue[0] instanceof BBoolean);
        Assert.assertTrue(((BBoolean) returnBValue[0]).booleanValue());
    }

    @Test(description = "Test producer flush function")
    public void testKafkaProducerFlushRecords() {
        result = BCompileUtil.compileAndSetup("producer/kafka_producer_flush_records.bal");
        BValue[] returnBValues = BRunUtil.invokeStateful(result, "funcKafkaTestFlush");
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BBoolean);
        Assert.assertTrue(((BBoolean) returnBValues[0]).booleanValue());
    }

    @Test(description = "Test Basic produce")
    public void testKafkaProducer() {
        result = BCompileUtil.compileAndSetup("producer/kafka_producer.bal");
        String topic = "producer-test-topic";
        BValue[] inputBValues = {};
        BRunUtil.invokeStateful(result, "funcTestKafkaProduce", inputBValues);

        final CountDownLatch completion = new CountDownLatch(1);
        final AtomicLong messagesRead = new AtomicLong(0);

        kafkaCluster.useTo().consumeStrings(topic, 2, 10, TimeUnit.SECONDS, completion::countDown, (key, value) -> {
            messagesRead.incrementAndGet();
            return true;
        });
        try {
            completion.await();
        } catch (Exception ex) {
            //Ignore
        }
        Assert.assertEquals(messagesRead.get(), 2);
    }

    @Test(description = "Test producer topic partition retrieval")
    public void testKafkaTopicPartitionRetrieval() {
        String topic1 = "partition-retrieval-topic-1";
        String topic2 = "partition-retrieval-topic-2";
        String topicNegative = "partition-retrieval-topic-negative";

        kafkaCluster.createTopic(topic1, 2, 1);
        kafkaCluster.createTopic(topic2, 5, 1);

        result = BCompileUtil.compile("producer/kafka_producer_partition_retrieval.bal");
        BValue[] inputBValues = {new BString(topic1)};
        BValue[] returnBValues = BRunUtil.invoke(result, "funcTestPartitionInfoRetrieval", inputBValues);
        Assert.assertEquals(returnBValues.length, 2);

        inputBValues = new BValue[]{new BString(topic2)};
        returnBValues = BRunUtil.invoke(result, "funcTestPartitionInfoRetrieval", inputBValues);
        Assert.assertEquals(returnBValues.length, 5);

        //negative test for the case where topic has not been created programmatically
        inputBValues = new BValue[]{new BString(topicNegative)};
        returnBValues = BRunUtil.invoke(result, "funcTestPartitionInfoRetrieval", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
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

    protected static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-producer-test");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2182, 9094);
        return kafkaCluster;
    }

}
