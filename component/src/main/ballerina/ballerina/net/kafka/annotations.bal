package ballerina.net.kafka;

@Description { value:"Annotation which represents Kafka Consumer Service configuration."}
@Field { value:"bootstrapServers: List of remote server endpoints." }
@Field { value:"groupId: Unique string that identifies the consumer." }
@Field { value:"topics: Topics to be subscribed." }
@Field { value:"pollingTimeout: Polling timeout to wait on poll cycle." }
@Field { value:"pollingInterval: Polling interval between two polling cycles." }
@Field { value:"offsetReset: Offset reset strategy if no initial offset." }
@Field { value:"concurrentConsumers: Number of concurrent consumers for service." }
@Field { value:"decoupleProcessing: Decouple message retrival and processing." }
@Field { value:"partitionAssignmentStrategy: Strategy class for handle partition assignment among consumers." }
@Field { value:"metricsRecordingLevel: Metrics recording level." }
@Field { value:"metricReporterClasses: Metrics reporter classes." }
@Field { value:"clientID: Id to be used for server side logging." }
@Field { value:"interceptorClasses: Interceptor classes to be used before sending records." }
@Field { value:"isolationLevel: How the transactional messages are read." }
@Field { value:"sessionTimeout: Timeout used to detect consumer failures when heartbeat threshold is reached." }
@Field { value:"heartBeatInterval: Expected time between heartbeats." }
@Field { value:"metadataMaxAge: Max time to force a refresh of metadata." }
@Field { value:"autoCommitInterval: Offset committing interval." }
@Field { value:"maxPartitionFetchBytes: The max amount of data per-partition the server return." }
@Field { value:"sendBuffer: Size of the TCP send buffer (SO_SNDBUF)." }
@Field { value:"receiveBuffer: Size of the TCP receive buffer (SO_RCVBUF)." }
@Field { value:"fetchMinBytes: Minimum amount of data the server should return for a fetch request." }
@Field { value:"fetchMaxBytes: Maximum amount of data the server should return for a fetch request." }
@Field { value:"fetchMaxWait: Maximum amount of time the server will block before answering the fetch request." }
@Field { value:"reconnectBackoffMax: Maximum amount of time in milliseconds to wait when reconnecting." }
@Field { value:"retryBackoff: Time to wait before attempting to retry a failed request." }
@Field { value:"metricsSampleWindow: Window of time a metrics sample is computed over." }
@Field { value:"metricsNumSamples: Number of samples maintained to compute metrics." }
@Field { value:"requestTimeout: Wait time for response of a request." }
@Field { value:"connectionsMaxIdle: Close idle connections after the number of milliseconds." }
@Field { value:"maxPollRecords: Maximum number of records returned in a single call to poll." }
@Field { value:"maxPollInterval: Maximum delay between invocations of poll." }
@Field { value:"reconnectBackoff: Time to wait before attempting to reconnect." }
@Field { value:"autoCommit: Enables auto commit offsets." }
@Field { value:"checkCRCS: Check the CRC32 of the records consumed." }
@Field { value:"excludeInternalTopics: Whether records from internal topics should be exposed to the consumer." }
@Field { value:"properties: Additional properties array." }
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
    int reconnectBackoff;
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
