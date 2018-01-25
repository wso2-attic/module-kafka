package ballerina.net.kafka;

@Description { value:"Annotation which represents Kafka Consumer Service configuration."}
public annotation configuration attach service<> {
    string bootstrapServers;
    string groupId;
    string[] topics;
    int pollingTimeout;
    int pollingInterval;
    int concurrentConsumers;
    boolean autoCommit;
    boolean decoupleProcessing;
    string offsetReset;
    int sessionTimeout;
    int heartBeatInterval;
    string partitionAssignmentStrategy;
    int metadataMaxAge;
    int autoCommitInterval;
    string clientId;
    int maxPartitionFetchBytes;
    int sendBuffer;
    int receiveBuffer;
    int fetchMinBytes;
    int fetchMaxBytes;
    int fetchMaxWait;
    int reconnectBackoffMax;
    int retryBackoff;
    boolean checkCRCS;
    int metricsSampleWindow;
    int metricsNumSamples;
    string metricsRecordingLevel;
    string metricsReporterClasses;
    int requestTimeout;
    int connectionMaxIdle;
    string interceptorClasses;
    int maxPollRecords;
    int maxPollInterval;
    boolean excludeInternalTopics;
    string isolationLevel;
    string[] properties;
}
