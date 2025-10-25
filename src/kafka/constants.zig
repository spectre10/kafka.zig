
/// Error codes returned by Kafka
pub const ErrorCode = enum(i16) {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    _,

    pub fn fromInt(value: i16) ErrorCode {
        return @enumFromInt(value);
    }

    pub fn toInt(self: ErrorCode) i16 {
        return @intFromEnum(self);
    }

    pub fn isError(self: ErrorCode) bool {
        return self != .None;
    }

    pub fn toString(self: ErrorCode) []const u8 {
        return switch (self) {
            .None => "No error",
            .OffsetOutOfRange => "Offset out of range",
            .CorruptMessage => "Corrupt message",
            .UnknownTopicOrPartition => "Unknown topic or partition",
            .InvalidFetchSize => "Invalid fetch size",
            .LeaderNotAvailable => "Leader not available",
            .NotLeaderOrFollower => "Not leader or follower",
            .RequestTimedOut => "Request timed out",
            .BrokerNotAvailable => "Broker not available",
            .ReplicaNotAvailable => "Replica not available",
            .MessageTooLarge => "Message too large",
            .StaleControllerEpoch => "Stale controller epoch",
            .OffsetMetadataTooLarge => "Offset metadata too large",
            .NetworkException => "Network exception",
            .CoordinatorLoadInProgress => "Coordinator load in progress",
            .CoordinatorNotAvailable => "Coordinator not available",
            .NotCoordinator => "Not coordinator",
            .InvalidTopicException => "Invalid topic exception",
            .RecordListTooLarge => "Record list too large",
            .NotEnoughReplicas => "Not enough replicas",
            .NotEnoughReplicasAfterAppend => "Not enough replicas after append",
            .InvalidRequiredAcks => "Invalid required acks",
            .IllegalGeneration => "Illegal generation",
            .InconsistentGroupProtocol => "Inconsistent group protocol",
            .InvalidGroupId => "Invalid group id",
            .UnknownMemberId => "Unknown member id",
            .InvalidSessionTimeout => "Invalid session timeout",
            .RebalanceInProgress => "Rebalance in progress",
            .InvalidCommitOffsetSize => "Invalid commit offset size",
            .TopicAuthorizationFailed => "Topic authorization failed",
            .GroupAuthorizationFailed => "Group authorization failed",
            .ClusterAuthorizationFailed => "Cluster authorization failed",
            .InvalidTimestamp => "Invalid timestamp",
            .UnsupportedSaslMechanism => "Unsupported SASL mechanism",
            .IllegalSaslState => "Illegal SASL state",
            .UnsupportedVersion => "Unsupported version",
            _ => "Unknown error",
        };
    }
};

/// API keys for Kafka requests
pub const ApiKey = enum(i16) {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,

    pub fn toInt(self: ApiKey) i16 {
        return @intFromEnum(self);
    }
};
