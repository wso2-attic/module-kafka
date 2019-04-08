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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import wso2/kafka;

string topic = "service-validate-custom-error-type-test";

kafka:ConsumerConfig consumerConfigs = {
    bootstrapServers: "localhost:9094",
    groupId: "service-test-validate-custom-error-group",
    clientId: "service-validate-rcustom-error-consumer",
    offsetReset: "earliest",
    topics: [topic]
};

listener kafka:SimpleConsumer kafkaConsumer = new(consumerConfigs);

type CustomError error<string, CustomErrorData>;

type CustomErrorData record {|
    string message = "";
    error? cause = ();
    string data = "";
|};

service kafkaTestService on kafkaConsumer {
    resource function onMessage(kafka:SimpleConsumer consumer, kafka:ConsumerRecord[] records) returns error? {
        foreach kafka:ConsumerRecord kafkaRecord in records {
            CustomError e = error("Custom Error", {data: "sample"});
        }
        return;
    }
}


