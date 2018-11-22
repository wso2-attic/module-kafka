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

endpoint kafka:SimpleProducer kafkaProducer {
    bootstrapServers: "localhost:9094",
    clientID: "basic-producer",
    acks: "all",
    transactionalID: "test-transaction-id",
    noRetries: 3
};

endpoint kafka:SimpleConsumer kafkaConsumer1 {
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    autoCommit: false
};

endpoint kafka:SimpleConsumer kafkaConsumer2 {
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    autoCommit: false
};

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
            if (lengthof offsets == 0) {
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
            if (lengthof offsets == 0) {
                return true;
            } else {
                return false; // This should not recieve any records as they are already committed.
            }
        }
    }
}

function funcGetPartitionOffset(kafka:SimpleConsumer consumer) returns kafka:PartitionOffset[]|error {
    endpoint kafka:SimpleConsumer consumerEP {};
    consumerEP = consumer;
    error|kafka:ConsumerRecord[] result = consumerEP->poll(2000);
    match result {
        error e => {
            return e;
        }
        kafka:ConsumerRecord[] records => {
            kafka:PartitionOffset[] offsets = [];
            int i = 0;
            foreach kafkaRecord in records {
                kafka:TopicPartition partition = { topic: kafkaRecord.topic, partition: kafkaRecord.partition };
                kafka:PartitionOffset offset = { partition: partition, offset: kafkaRecord.offset };
                offsets[i] = offset;
                i += 1;
            }
            return offsets;
        }
    }
}

