//! Kafka protocol primitives and serialization
//!
//! This module implements the basic types and serialization functions
//! for the Kafka binary protocol as defined in the Kafka Protocol Guide.
//!
//! Reference: https://kafka.apache.org/protocol.html

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

/// Kafka protocol uses big-endian byte order for all integers
pub const endian = .big;

/// Helper to write all bytes, handling partial writes
pub fn writeAll(writer: anytype, bytes: []const u8) !void {
    var index: usize = 0;
    while (index < bytes.len) {
        const written = try writer.write(bytes[index..]);
        if (written == 0) return error.EndOfStream;
        index += written;
    }
}

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

/// Request header for Kafka protocol
pub const RequestHeader = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: []const u8,

    /// Write the request header to a writer
    pub fn write(self: RequestHeader, writer: anytype) !void {
        try writeInt16(writer, self.api_key);
        try writeInt16(writer, self.api_version);
        try writeInt32(writer, self.correlation_id);
        try writeString(writer, self.client_id);
    }

    /// Calculate the size of the header when serialized
    pub fn size(self: RequestHeader) usize {
        return 2 + // api_key
            2 + // api_version
            4 + // correlation_id
            2 + self.client_id.len; // client_id (length prefix + data)
    }
};

/// Response header for Kafka protocol
pub const ResponseHeader = struct {
    correlation_id: i32,

    /// Read the response header from a reader
    pub fn read(reader: anytype) !ResponseHeader {
        return ResponseHeader{
            .correlation_id = try readInt32(reader),
        };
    }
};

/// Write an int8 in big-endian format
pub fn writeInt8(writer: anytype, value: i8) !void {
    try writer.writeByte(@bitCast(value));
}

/// Write an int16 in big-endian format
pub fn writeInt16(writer: anytype, value: i16) !void {
    var buf: [2]u8 = undefined;
    mem.writeInt(i16, &buf, value, endian);
    try writeAll(writer, &buf);
}

/// Write an int32 in big-endian format
pub fn writeInt32(writer: anytype, value: i32) !void {
    var buf: [4]u8 = undefined;
    mem.writeInt(i32, &buf, value, endian);
    try writeAll(writer, &buf);
}

/// Write an int64 in big-endian format
pub fn writeInt64(writer: anytype, value: i64) !void {
    var buf: [8]u8 = undefined;
    mem.writeInt(i64, &buf, value, endian);
    try writeAll(writer, &buf);
}

/// Write a string in Kafka format (int16 length prefix + UTF-8 data)
pub fn writeString(writer: anytype, value: []const u8) !void {
    if (value.len > std.math.maxInt(i16)) {
        return error.StringTooLong;
    }
    try writeInt16(writer, @intCast(value.len));
    try writeAll(writer, value);
}

/// Write a nullable string in Kafka format (-1 for null, or length + data)
pub fn writeNullableString(writer: anytype, value: ?[]const u8) !void {
    if (value) |str| {
        try writeString(writer, str);
    } else {
        try writeInt16(writer, -1);
    }
}

/// Write a byte array in Kafka format (int32 length prefix + data)
pub fn writeBytes(writer: anytype, value: []const u8) !void {
    if (value.len > std.math.maxInt(i32)) {
        return error.BytesTooLong;
    }
    try writeInt32(writer, @intCast(value.len));
    try writeAll(writer, value);
}

/// Write a nullable byte array in Kafka format (-1 for null, or length + data)
pub fn writeNullableBytes(writer: anytype, value: ?[]const u8) !void {
    if (value) |bytes| {
        try writeBytes(writer, bytes);
    } else {
        try writeInt32(writer, -1);
    }
}

/// Write an array length (int32)
pub fn writeArrayLen(writer: anytype, len: usize) !void {
    if (len > std.math.maxInt(i32)) {
        return error.ArrayTooLong;
    }
    try writeInt32(writer, @intCast(len));
}

/// Read an int8
pub fn readInt8(reader: anytype) !i8 {
    const byte = try reader.readByte();
    return @bitCast(byte);
}

/// Read an int16 in big-endian format
pub fn readInt16(reader: anytype) !i16 {
    var buf: [2]u8 = undefined;
    try reader.readNoEof(&buf);
    return mem.readInt(i16, &buf, endian);
}

/// Read an int32 in big-endian format
pub fn readInt32(reader: anytype) !i32 {
    var buf: [4]u8 = undefined;
    try reader.readNoEof(&buf);
    return mem.readInt(i32, &buf, endian);
}

/// Read an int64 in big-endian format
pub fn readInt64(reader: anytype) !i64 {
    var buf: [8]u8 = undefined;
    try reader.readNoEof(&buf);
    return mem.readInt(i64, &buf, endian);
}

/// Read a string in Kafka format (int16 length prefix + UTF-8 data)
/// Caller owns the returned memory
pub fn readString(reader: anytype, allocator: Allocator) ![]u8 {
    const len = try readInt16(reader);
    if (len < 0) {
        return error.UnexpectedNullString;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    try reader.readNoEof(buf);
    return buf;
}

/// Read a nullable string in Kafka format
/// Returns null if length is -1, otherwise allocates and returns the string
/// Caller owns the returned memory
pub fn readNullableString(reader: anytype, allocator: Allocator) !?[]u8 {
    const len = try readInt16(reader);
    if (len < 0) {
        return null;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    try reader.readNoEof(buf);
    return buf;
}

/// Read a byte array in Kafka format (int32 length prefix + data)
/// Caller owns the returned memory
pub fn readBytes(reader: anytype, allocator: Allocator) ![]u8 {
    const len = try readInt32(reader);
    if (len < 0) {
        return error.UnexpectedNullBytes;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    try reader.readNoEof(buf);
    return buf;
}

/// Read a nullable byte array in Kafka format
/// Returns null if length is -1, otherwise allocates and returns the bytes
/// Caller owns the returned memory
pub fn readNullableBytes(reader: anytype, allocator: Allocator) !?[]u8 {
    const len = try readInt32(reader);
    if (len < 0) {
        return null;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    try reader.readNoEof(buf);
    return buf;
}

/// Read an array length (int32)
pub fn readArrayLen(reader: anytype) !i32 {
    return try readInt32(reader);
}

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

test "write and read int16" {
    var buf: [2]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);

    try writeInt16(stream.writer(), 0x1234);
    stream.reset();
    const value = try readInt16(stream.reader());

    try std.testing.expectEqual(@as(i16, 0x1234), value);
}

test "write and read int32" {
    var buf: [4]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);

    try writeInt32(stream.writer(), 0x12345678);
    stream.reset();
    const value = try readInt32(stream.reader());

    try std.testing.expectEqual(@as(i32, 0x12345678), value);
}

test "write and read string" {
    var buf: [100]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);

    try writeString(stream.writer(), "hello");
    stream.reset();
    const value = try readString(stream.reader(), std.testing.allocator);
    defer std.testing.allocator.free(value);

    try std.testing.expectEqualStrings("hello", value);
}
