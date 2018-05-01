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

package org.ballerinalang.kafka.nativeimpl.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.kafka.util.KafkaConstants;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.types.BStructType;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.util.codegen.PackageInfo;
import org.ballerinalang.util.codegen.ProgramFile;
import org.ballerinalang.util.codegen.StructInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Test cases for ballerina.net.kafka consumer ( with Pause ) native functions.
 */
public class KafkaConsumerPauseTest {
    private CompileResult result;
    private static File dataDir;
    protected static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        result = BCompileUtil.compile("consumer/kafka_consumer_pause.bal");
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
        kafkaCluster.createTopic("test", 1, 1);
    }

    @Test(description = "Test Basic consumer with seek")
    public void testKafkaConsumeWithPause() {
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
        BValue[] returnBValues = BRunUtil.invoke(result, "funcKafkaConnect", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BStruct);
        // getting kafka endpoint
        BValue consumerEndpoint = returnBValues[0];
        inputBValues = new BValue[]{consumerEndpoint};
        int msgCount = 0;
        while (true) {
            returnBValues = BRunUtil.invoke(result, "funcKafkaPoll", inputBValues);
            Assert.assertEquals(returnBValues.length, 1);
            Assert.assertTrue(returnBValues[0] instanceof BInteger);
            msgCount = msgCount + new Long(((BInteger) returnBValues[0]).intValue()).intValue();
            if (msgCount == 10) {
                break;
            }
        }
        Assert.assertEquals(msgCount, 10);
        ProgramFile programFile = result.getProgFile();
        BStruct part = createPartitionStruct(programFile);
        part.setStringField(0, "test");
        part.setIntField(0, 0);

        ArrayList<BStruct> structArray = new ArrayList<>();
        structArray.add(part);
        BRefValueArray partitionArray = new BRefValueArray(structArray.toArray(new BRefType[0]),
                createPartitionStruct(programFile).getType());

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPausedPartitions", inputBValues);
        Assert.assertEquals(returnBValues.length, 0);

        inputBValues = new BValue[]{consumerEndpoint, partitionArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaPause", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNull(returnBValues[0]);

        inputBValues = new BValue[]{consumerEndpoint};
        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPausedPartitions", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        BStruct tpReturned = (BStruct) returnBValues[0];
        Assert.assertEquals(tpReturned.getStringField(0), "test");
        Assert.assertEquals(tpReturned.getIntField(0), 0);

        inputBValues = new BValue[]{consumerEndpoint, partitionArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaResume", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNull(returnBValues[0]);

        inputBValues = new BValue[]{consumerEndpoint};
        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPausedPartitions", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);

        //negative tests for pausing and resuming non existing partitions
        part.setStringField(0, "test_not");
        part.setIntField(0, 100);

        inputBValues = new BValue[]{consumerEndpoint, partitionArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaPause", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BStruct);
        Assert.assertEquals(((BStruct) returnBValues[0]).getStringField(0),
                "No current assignment for partition test_not-100");

        inputBValues = new BValue[]{consumerEndpoint, partitionArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaResume", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BStruct);
        Assert.assertEquals(((BStruct) returnBValues[0]).getStringField(0),
                "No current assignment for partition test_not-100");

        inputBValues = new BValue[]{consumerEndpoint};
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

    protected static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-consumer");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2185, 9094);
        return kafkaCluster;
    }

    private BStruct createPartitionStruct(ProgramFile programFile) {
        PackageInfo kafkaPackageInfo = programFile.getPackageInfo(KafkaConstants.KAFKA_NATIVE_PACKAGE);
        StructInfo consumerRecordStructInfo = kafkaPackageInfo
                .getStructInfo(KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
        BStructType structType = consumerRecordStructInfo.getType();
        BStruct bStruct = new BStruct(structType);
        return bStruct;
    }

}
