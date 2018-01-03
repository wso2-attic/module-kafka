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

package org.ballerinalang.net.kafka.nativeimpl.consumer;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.ballerinalang.bre.Context;
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
import org.ballerinalang.net.kafka.Constants;
import org.ballerinalang.util.codegen.PackageInfo;
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
 * Test cases for ballerina.net.kafka consumer ( with seek ) native functions.
 */
public class NetKafkaConsumerSeekTest {
    private CompileResult result;
    private static File dataDir;
    protected static KafkaCluster kafkaCluster;

    @BeforeClass
    public void setup() throws IOException {
        result = BCompileUtil.compile("consumer/kafka-consumer-seek.bal");
        Properties prop = new Properties();
        kafkaCluster = kafkaCluster().deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true).withKafkaConfiguration(prop).addBrokers(1).startup();
        kafkaCluster.createTopic("test", 1, 1);
    }

    @Test(description = "Test Basic consumer with seek")
    public void testKafkaConsumeWithSeek() {
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
        BStruct consumerStruct = (BStruct) returnBValues[0];
        int msgCount = 0;
        inputBValues = new BValue[]{consumerStruct};
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
        Context ctx = new Context(result.getProgFile());
        BStruct part = createPartitionStruct(ctx);
        part.setStringField(0, "test");
        part.setIntField(0, 0);
        inputBValues = new BValue[]{consumerStruct, part};


        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 2);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertNull(returnBValues[1]);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 10);


        BStruct offset = createOffsetStruct(ctx);
        offset.setRefField(0, part);
        offset.setIntField(0, 5);

        inputBValues = new BValue[]{consumerStruct, offset};
        BRunUtil.invoke(result, "funcKafkaSeekOffset", inputBValues);

        inputBValues = new BValue[]{consumerStruct, part};

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 2);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertNull(returnBValues[1]);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 5);

        ArrayList<BStruct> structArray = new ArrayList<>();
        structArray.add(part);
        BRefValueArray partitionArray = new BRefValueArray(structArray.toArray(new BRefType[0]),
                createPartitionStruct(ctx).getType());

        inputBValues = new BValue[]{consumerStruct, partitionArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaBeginOffsets", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BRefValueArray);
        BStruct off = (BStruct) ((BRefValueArray) returnBValues[0]).get(0);
        Assert.assertEquals(off.getIntField(0), 0);

        inputBValues = new BValue[]{consumerStruct, partitionArray};
        returnBValues = BRunUtil.invoke(result, "funcKafkaEndOffsets", inputBValues);
        Assert.assertEquals(returnBValues.length, 1);
        Assert.assertTrue(returnBValues[0] instanceof BRefValueArray);
        off = (BStruct) ((BRefValueArray) returnBValues[0]).get(0);
        Assert.assertEquals(off.getIntField(0), 10);

        inputBValues = new BValue[]{consumerStruct, partitionArray};
        BRunUtil.invoke(result, "funcKafkaSeekToBegin", inputBValues);

        inputBValues = new BValue[]{consumerStruct, part};

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 2);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertNull(returnBValues[1]);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 0);

        inputBValues = new BValue[]{consumerStruct, partitionArray};
        BRunUtil.invoke(result, "funcKafkaSeekToEnd", inputBValues);

        inputBValues = new BValue[]{consumerStruct, part};

        returnBValues = BRunUtil.invoke(result, "funcKafkaGetPositionOffset", inputBValues);
        Assert.assertEquals(returnBValues.length, 2);
        Assert.assertNotNull(returnBValues[0]);
        Assert.assertNull(returnBValues[1]);
        Assert.assertTrue(returnBValues[0] instanceof BInteger);
        Assert.assertEquals(((BInteger) returnBValues[0]).intValue(), 10);

        inputBValues = new BValue[]{consumerStruct};
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

    private BStruct createPartitionStruct(Context context) {
        PackageInfo kafkaPackageInfo = context.getProgramFile()
                .getPackageInfo(Constants.KAFKA_NATIVE_PACKAGE);
        StructInfo consumerRecordStructInfo = kafkaPackageInfo
                .getStructInfo(Constants.TOPIC_PARTITION_STRUCT_NAME);
        BStructType structType = consumerRecordStructInfo.getType();
        BStruct bStruct = new BStruct(structType);
        return bStruct;
    }

    private BStruct createOffsetStruct(Context context) {
        PackageInfo kafkaPackageInfo = context.getProgramFile()
                .getPackageInfo(Constants.KAFKA_NATIVE_PACKAGE);
        StructInfo consumerRecordStructInfo = kafkaPackageInfo
                .getStructInfo(Constants.OFFSET_STRUCT_NAME);
        BStructType structType = consumerRecordStructInfo.getType();
        BStruct bStruct = new BStruct(structType);
        return bStruct;
    }

}
