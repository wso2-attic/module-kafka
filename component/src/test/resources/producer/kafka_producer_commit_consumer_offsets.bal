// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import wso2/kafka;

kafka:ProducerConfig producerConfigs = {
    bootstrapServers: "localhost:9294, localhost:9295, localhost:9296",
    clientID: "basic-producer",
    acks: "all",
    transactionalID: "test-transaction-id",
    noRetries: 3
};

kafka:SimpleProducer kafkaProducer = new (producerConfigs);

kafka:ConsumerConfig consumerConfigs1 = {
    bootstrapServers: "localhost:9294, localhost:9295, localhost:9296",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    autoCommit: false
};

kafka:SimpleConsumer kafkaConsumer1 = new (consumerConfigs1);

kafka:ConsumerConfig consumerConfigs2 = {
    bootstrapServers: "localhost:9294, localhost:9295, localhost:9296",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    autoCommit: false
};

kafka:SimpleConsumer kafkaConsumer2 = new (consumerConfigs2);

function funcTestKafkaProduce() {
    string msg = "test-msg";
    byte[] byteMsg = msg.toByteArray("UTF-8");
    kafkaProduce(byteMsg, "test");
}

function kafkaProduce(byte[] value, string topic) {
    transaction {
        kafkaProducer->send(value, topic);
    }
}

function funcTestKafkaCommitOffsets() returns boolean {
    var results = funcGetPartitionOffset(kafkaConsumer1);
    match results {
        error e => {
            return false;
        }
        kafka:PartitionOffset[] offsets => {
            if (offsets.length() == 0) {
                return false;
            } else {
                kafkaProducer->commitConsumerOffsets(offsets, "test-group");
                return true;
            }
        }
    }
}

function funcTestPollAgain() returns boolean {
    var results = funcGetPartitionOffset(kafkaConsumer2);
    match results {
        error e => {
            return false;
        }
        kafka:PartitionOffset[] offsets => {
            if (offsets.length() == 0) {
                return true;
            } else {
                return false; // This should not recieve any records as they are already committed.
            }
        }
    }
}

function funcGetPartitionOffset(kafka:SimpleConsumer consumer) returns kafka:PartitionOffset[]|error {
    error|kafka:ConsumerRecord[] result = consumer->poll(2000);
    if (result is error) {
        return result;
    } else {
        kafka:PartitionOffset[] offsets = [];
        int i = 0;
        foreach kafkaRecord in result {
            kafka:TopicPartition partition = { topic: kafkaRecord.topic, partition: kafkaRecord.partition };
            kafka:PartitionOffset offset = { partition: partition, offset: kafkaRecord.offset };
            offsets[i] = offset;
            i += 1;
        }
        return offsets;
    }
}

