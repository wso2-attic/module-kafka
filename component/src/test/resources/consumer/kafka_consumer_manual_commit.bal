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
    offsetReset: "earliest",
    autoCommit: false,
    topics: ["test"]
};

function funcKafkaConnect() returns kafka:SimpleConsumer {
    kafka:SimpleConsumer simpleKafkaConsumer = new (consumerConfigs);
    return simpleKafkaConsumer;
}

function funcKafkaClose(kafka:SimpleConsumer consumer) returns boolean {
    var conErr = consumer->close();
    if (conErr is error) {
        return false;
    }
    return true;
}

function funcKafkaPoll(kafka:SimpleConsumer consumer) returns int|error {
    var records = consumer->poll(1000);
    if (records is error) {
        return records;
    } else {
        return records.length();
    }
}

function funcKafkaGetCommittedOffset(kafka:SimpleConsumer consumer, kafka:TopicPartition part)
             returns kafka:PartitionOffset|error {
    kafka:PartitionOffset offset;
    offset = check consumer->getCommittedOffset(part);
    return offset;
}

function funcKafkaGetPositionOffset(kafka:SimpleConsumer consumer, kafka:TopicPartition part) returns int|error {
    int offset;
    var result = consumer->getPositionOffset(part);
    if (result is int) {
        return result;
    }
    else {
        return result;
    }
}

function funcKafkaCommit(kafka:SimpleConsumer consumer) {
    consumer->commit();
}
