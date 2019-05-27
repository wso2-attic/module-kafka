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

kafka:ConsumerConfig consumerConfigs = {
    bootstrapServers: "localhost:9094",
    groupId: "abcd",
    clientId: "manual-commit-consumer",
    offsetReset: "earliest",
    autoCommit: false,
    topics: ["test"]
};

function funcKafkaConnect() returns kafka:Consumer {
    kafka:Consumer kafkaConsumer = new(consumerConfigs);
    return kafkaConsumer;
}

function funcKafkaClose(kafka:Consumer consumer) returns boolean {
    var result = consumer->close();
    return !(result is error);
}

function funcKafkaPoll(kafka:Consumer consumer) returns int|error {
    var records = consumer->poll(1000);
    if (records is error) {
        return records;
    } else {
        return records.length();
    }
}

function funcKafkaGetCommittedOffset(kafka:Consumer consumer, kafka:TopicPartition partition)
             returns kafka:PartitionOffset|error {
    return consumer->getCommittedOffset(partition);
}

function funcKafkaGetPositionOffset(kafka:Consumer consumer, kafka:TopicPartition part) returns int|error {
    var result = consumer->getPositionOffset(part);
    return result;
}

function funcKafkaCommit(kafka:Consumer consumer) returns boolean {
    var result = consumer->commit();
    return !(result is error);
}
