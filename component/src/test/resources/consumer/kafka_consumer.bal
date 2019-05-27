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

kafka:ConsumerConfig consumerConfig = {
    bootstrapServers: "localhost:9094",
    groupId: "test-group",
    clientId: "basic-consumer",
    offsetReset: "earliest",
    topics: ["test"]
};

kafka:ConsumerConfig consumerConfigNegative = {};
kafka:Consumer negativeConsumer = new (consumerConfigNegative);

function funcKafkaConnect() returns kafka:Consumer {
    return new(consumerConfig);
}

function funcKafkaConnectNegative() returns error? {
    return negativeConsumer->connect();
}

function funcKafkaClose(kafka:Consumer consumer) returns boolean {
    var result = consumer->close();
    return !(result is error);
}

function funcKafkaGetSubscription(kafka:Consumer consumer) returns string[]|error {
    return consumer->getSubscription();
}

function funcKafkaGetAssignment(kafka:Consumer consumer) returns kafka:TopicPartition[]|error {
    return consumer->getAssignment();
}

function funcKafkaPoll(kafka:Consumer consumer) returns int|error {
    var results = consumer->poll(1000);
    if (results is error) {
        return results;
    } else {
        return results.length();
    }
}
