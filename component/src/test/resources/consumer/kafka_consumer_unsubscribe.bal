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

string topic1 = "consumer-unsubscribe-test-1";
string topic2 = "consumer-unsubscribe-test-2";

function funcKafkaTestUnsubscribe() returns boolean {
    kafka:Listener kafkaConsumer = new({
            bootstrapServers: "localhost:9094",
            groupId: "test-group",
            clientId: "unsubscribe-consumer",
            offsetReset: "earliest",
            topics: [topic1, topic2]
        });
    var subscribedTopics = kafkaConsumer->getSubscription();
    if (subscribedTopics is error) {
        return false;
    }
    else {
        if (subscribedTopics.length() != 2) {
            return false;
        }
    }
    var result = kafkaConsumer->unsubscribe();
    subscribedTopics = kafkaConsumer->getSubscription();
    if (subscribedTopics is error) {
        return false;
    } else {
        if (subscribedTopics.length() != 0) {
            return false;
        }
        return true;
    }
}
