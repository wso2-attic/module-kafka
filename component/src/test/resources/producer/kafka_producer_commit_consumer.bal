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
import ballerina/io;

endpoint kafka:SimpleProducer kafkaProducer {
    bootstrapServers: "localhost:9194, localhost:9195, localhost:9196",
    clientID: "basic-producer",
    acks: "all",
    transactionalID: "test-transaction-id",
    noRetries: 3
};

endpoint kafka:SimpleConsumer kafkaConsumer1 {
    bootstrapServers: "localhost:9194, localhost:9195, localhost:9196",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    autoCommit: false
};

endpoint kafka:SimpleConsumer kafkaConsumer2 {
    bootstrapServers: "localhost:9194, localhost:9195, localhost:9196",
    groupId: "test-group",
    offsetReset: "earliest",
    topics: ["test"],
    autoCommit: false
};

//No of chars in test string
int noOfChars = 8;
string msg = "test-msg";

function funcTestKafkaProduce() {
    byte[] byteMsg = msg.toByteArray("UTF-8");
    kafkaProduce(byteMsg, "test");
}

function kafkaProduce(byte[] value, string topic) {
    transaction {
        kafkaProducer->send(value, topic);
    }
}

function funcTestKafkaCommit() returns boolean {
    string results = kafkaConsume(kafkaConsumer1);
    if (results != msg) {
        return false;
    }
    kafkaProducer->commitConsumer(kafkaConsumer1.consumerActions);
    return true;
}

function funcTestKafkaPollAgain() returns boolean {
    string results = kafkaConsume(kafkaConsumer2);
    if (results == "No Records") {
        return true; // This should not return test-msg, as it's already committed.
    }
    return false;
}

function kafkaConsume(kafka:SimpleConsumer consumer) returns string {
    endpoint kafka:SimpleConsumer consumerEP {};
    consumerEP = consumer;
    error|kafka:ConsumerRecord[] result = consumerEP->poll(2000);
    match result {
        error e => {
            return "failed";
        }
        kafka:ConsumerRecord[] records => {
            string resultText = "";
            foreach kafkaRecord in records {
                byte[] serializedMsg = kafkaRecord.value;
                io:ReadableByteChannel byteChannel = io:createReadableChannel(serializedMsg);
                io:ReadableCharacterChannel characterChannel = new io:ReadableCharacterChannel(byteChannel, "UTF-8");
                var characters = characterChannel.read(noOfChars);
                match characters {
                    string text => {
                        resultText += text;
                    }
                    error e => {
                        resultText = "Failed";
                    }
                }
            }
            if (resultText == "") {
                return "No Records";
            }
            else {
                return resultText;
            }
        }
    }
}
