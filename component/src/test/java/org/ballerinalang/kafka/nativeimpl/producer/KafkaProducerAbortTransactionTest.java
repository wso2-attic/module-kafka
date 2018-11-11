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

package org.ballerinalang.kafka.nativeimpl.producer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.exceptions.BLangRuntimeException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaProducerAbortTransactionTest {
    private CompileResult result;
    private static File dataDir;
    protected static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        result = BCompileUtil.compile("producer/kafka_producer_abort_transaction.bal");
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(3).startup();
        kafkaCluster.createTopic("test-topic", 2, 3);
    }

    @Test(description = "Test abort transaction in producer", expectedExceptions = BLangRuntimeException.class)
    public void testKafkaProduce() {
        CountDownLatch completion = new CountDownLatch(1);
        kafkaCluster.useTo().produceStrings("test", 10, completion::countDown, () -> {
            return "test_string";
        });
        try {
            completion.await();
        } catch (Exception ex) {
            //Ignore
        }
        BValue[] inputBValues = {};
        BRunUtil.invoke(result, "funcKafkaAbortTransactionTest", inputBValues);
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
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-producer");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2182, 9094);
        return kafkaCluster;
    }
}
