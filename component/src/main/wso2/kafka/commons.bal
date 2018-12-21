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

# This type represents topic partition position in which consumed record is stored.
#
# + partition - TopicPartition which record is related.
# + offset - Offset in which record is stored in partition.
public type PartitionOffset record {
    TopicPartition partition;
    int offset;
    !...
};

# This type represents a topic partition.
#
# + topic - Topic which partition is related.
# + partition - Index for the partition.
public type TopicPartition record {
    string topic;
    int partition;
    !...
};
