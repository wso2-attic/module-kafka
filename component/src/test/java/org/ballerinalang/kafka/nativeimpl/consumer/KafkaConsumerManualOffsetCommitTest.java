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
import org.ballerinalang.connector.api.BLangConnectorSPIUtil;
import org.ballerinalang.kafka.util.KafkaConstants;
import org.ballerinalang.launcher.util.BCompileUtil;
import org.ballerinalang.launcher.util.BRunUtil;
import org.ballerinalang.launcher.util.CompileResult;
import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BError;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefType;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.model.values.BValueArray;
import org.ballerinalang.util.codegen.ProgramFile;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.ballerinalang.kafka.util.KafkaConstants.KAFKA_NATIVE_PACKAGE;

/**
 * Test cases for ballerina.net.kafka consumer ( with manual commit enabled )  manual offset commit
 * using  commitOffset() native function.
 */
public class KafkaConsumerManualOffsetCommitTest {
    private CompileResult result;
    private static File dataDir;
    private static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        result = BCompileUtil.compile("consumer/kafka_consumer_manual_offset_commit.bal");
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
        kafkaCluster.createTopic("test", 1, 1);
    }

    @Test(
            description = "Test Basic consumer polling with manual offset commit",
            sequential = true
    )
    public void testKafkaConsumeWithManualOffsetCommit() {
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
        Assert.assertTrue(returnBValues[0] instanceof BMap);
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
        BMap<String, BValue> part = createPartitionStruct(programFile);
        part.put("topic", new BString("test"));
        part.put("partition", new BInteger(0));

        inputBValues = new BValue[]{consumerEndpoint, part};

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetCommittedOffset", inputBValues);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BMap);
        Assert.assertEquals(((BInteger) ((BMap<String, BValue>) returnBValues[0])
                .get("offset")).value().intValue(), 0);

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 10);

        BMap<String, BValue> offset = createOffsetStruct(programFile);
        offset.put("partition", part);
        offset.put("offset", new BInteger(5));

        ArrayList<BMap<String, BValue>> structArray = new ArrayList<>();
        structArray.add(offset);
        BValueArray offsetArray = new BValueArray(structArray.toArray(new BRefType[0]),
                createOffsetStruct(programFile).getType());

        inputBValues = new BValue[]{consumerEndpoint, offsetArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaCommitOffsets", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BBoolean);
        Assert.assertTrue(((BBoolean) returnBValues[0]).booleanValue());

        // Committed up to 5 th index of topic partition test-0
        inputBValues = new BValue[]{consumerEndpoint, part};

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetCommittedOffset", inputBValues);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BMap);
        Assert.assertEquals(((BInteger) ((BMap<String, BValue>) returnBValues[0])
                .get("offset")).value().intValue(), 5);

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 10);

        offset = createOffsetStruct(programFile);
        offset.put("partition", part);
        offset.put("offset", new BInteger(10));

        structArray = new ArrayList<>();
        structArray.add(offset);
        offsetArray = new BValueArray(structArray.toArray(new BRefType[0]),
                createOffsetStruct(programFile).getType());

        inputBValues = new BValue[]{consumerEndpoint, offsetArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaCommitOffsets", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BBoolean);
        Assert.assertTrue(((BBoolean) returnBValues[0]).booleanValue());

        // Committed up to 10 th index of topic partition test-0
        inputBValues = new BValue[]{consumerEndpoint, part};

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetCommittedOffset", inputBValues);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BMap);
        Assert.assertEquals(((BInteger) ((BMap<String, BValue>) returnBValues[0])
                .get("offset")).value().intValue(), 10);

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 10);

        part.put("topic", new BString("test_not"));
        part.put("partition", new BInteger(100));

        inputBValues = new BValue[]{consumerEndpoint, part};

        // Test partition which is non existent
        returnBValues = BRunUtil.invoke(result, "funcKafkaGetCommittedOffset", inputBValues);
        Assert.assertNotNull(returnBValues[0]);

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertTrue(returnBValues[0] instanceof BError);
        Assert.assertEquals(((BError) returnBValues[0]).getReason(),
                "Failed to get position offset: " +
                        "You can only check the position for partitions assigned to this consumer.");

        part.put("topic", new BString("test"));
        part.put("partition", new BInteger(0));
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

    private static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            throw new IllegalStateException();
        }
        dataDir = Testing.Files.createTestingDirectory("cluster-kafka-consumer-manual-offset-commit-test");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir).withPorts(2181, 9094);
        return kafkaCluster;
    }

    private BMap<String, BValue> createPartitionStruct(ProgramFile programFile) {
        return BLangConnectorSPIUtil.createBStruct(programFile,
                KAFKA_NATIVE_PACKAGE,
                KafkaConstants.TOPIC_PARTITION_STRUCT_NAME);
    }

    private BMap<String, BValue> createOffsetStruct(ProgramFile programFile) {
        return BLangConnectorSPIUtil.createBStruct(programFile,
                KAFKA_NATIVE_PACKAGE,
                KafkaConstants.OFFSET_STRUCT_NAME);
    }


}
