//! Kafka request and response types
//!
//! This module implements specific Kafka broker requests and their responses.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const broker = @import("broker.zig");
const constants = @import("constants.zig");
const Client = @import("client.zig").Client;

// =============================================================================
// ApiVersions Request/Response (API Key 18)
// =============================================================================

/// ApiVersions request - simplest request, requires no parameters
/// Used to query which API versions the broker supports
pub const ApiVersionsRequest = struct {
    pub fn write(_: ApiVersionsRequest, _: anytype) !void {
        // ApiVersions v0 has no request body
    }
};

/// Single API version information
pub const ApiVersion = struct {
    api_key: i16,
    min_version: i16,
    max_version: i16,

    pub fn deinit(_: *ApiVersion, _: Allocator) void {
        // No dynamic memory
    }
};

/// ApiVersions response
pub const ApiVersionsResponse = struct {
    error_code: constants.ErrorCode,
    api_versions: []ApiVersion,

    pub fn deinit(self: *ApiVersionsResponse, allocator: Allocator) void {
        for (self.api_versions) |*api_version| {
            api_version.deinit(allocator);
        }
        allocator.free(self.api_versions);
    }

    pub fn parse(reader: anytype, allocator: Allocator) !ApiVersionsResponse {
        const error_code = constants.ErrorCode.fromInt(try broker.readInt16(reader));

        const api_versions_len = try broker.readArrayLen(reader);
        if (api_versions_len < 0) {
            return error.InvalidArrayLength;
        }

        const api_versions = try allocator.alloc(ApiVersion, @intCast(api_versions_len));
        errdefer allocator.free(api_versions);

        for (api_versions) |*api_version| {
            api_version.* = ApiVersion{
                .api_key = try broker.readInt16(reader),
                .min_version = try broker.readInt16(reader),
                .max_version = try broker.readInt16(reader),
            };
        }

        return ApiVersionsResponse{
            .error_code = error_code,
            .api_versions = api_versions,
        };
    }
};

/// Send an ApiVersions request
pub fn apiVersions(client: *Client) !ApiVersionsResponse {
    const request = ApiVersionsRequest{};

    return try client.sendRequest(
        .ApiVersions,
        0, // version 0
        request,
        ApiVersionsResponse,
        ApiVersionsResponse.parse,
    );
}

// =============================================================================
// Metadata Request/Response (API Key 3)
// =============================================================================

/// Metadata request - query information about topics
pub const MetadataRequest = struct {
    topics: ?[]const []const u8, // null means all topics

    pub fn write(self: MetadataRequest, writer: anytype) !void {
        if (self.topics) |topics| {
            try broker.writeArrayLen(writer, topics.len);
            for (topics) |topic| {
                try broker.writeString(writer, topic);
            }
        } else {
            // -1 means all topics
            try broker.writeInt32(writer, -1);
        }
    }
};

/// Broker information
pub const Broker = struct {
    node_id: i32,
    host: []u8,
    port: i32,
    rack: ?[]u8,

    pub fn deinit(self: *Broker, allocator: Allocator) void {
        allocator.free(self.host);
    }
};

/// Partition metadata
pub const PartitionMetadata = struct {
    error_code: constants.ErrorCode,
    partition_id: i32,
    leader: i32,
    replicas: []i32,
    isr: []i32, // in-sync replicas

    pub fn deinit(self: *PartitionMetadata, allocator: Allocator) void {
        allocator.free(self.replicas);
        allocator.free(self.isr);
    }
};

/// Topic metadata
pub const TopicMetadata = struct {
    error_code: constants.ErrorCode,
    topic_name: []u8,
    partitions: []PartitionMetadata,

    pub fn deinit(self: *TopicMetadata, allocator: Allocator) void {
        allocator.free(self.topic_name);
        for (self.partitions) |*partition| {
            partition.deinit(allocator);
        }
        allocator.free(self.partitions);
    }
};

/// Metadata response
pub const MetadataResponse = struct {
    brokers: []Broker,
    topics: []TopicMetadata,

    pub fn deinit(self: *MetadataResponse, allocator: Allocator) void {
        for (self.brokers) |*b| {
            b.deinit(allocator);
        }
        allocator.free(self.brokers);

        for (self.topics) |*topic| {
            topic.deinit(allocator);
        }
        allocator.free(self.topics);
    }

    pub fn parse(reader: anytype, allocator: Allocator) !MetadataResponse {
        // Parse brokers
        const brokers_len = try broker.readArrayLen(reader);
        if (brokers_len < 0) {
            return error.InvalidArrayLength;
        }

        // const correlation_id = try broker.readArrayLen(reader);
        
        const brokers = try allocator.alloc(Broker, @intCast(brokers_len));
        errdefer allocator.free(brokers);

        for (brokers) |*b| {
            b.* = Broker{
                .node_id = try broker.readInt32(reader),
                .host = try broker.readString(reader, allocator),
                .port = try broker.readInt32(reader),
                .rack = try broker.readNullableString(reader, allocator),
            };
        }
        
        const controller_id = try broker.readInt32(reader);
        std.debug.print("{d}\n", .{controller_id});

        // Parse topics
        const topics_len = try broker.readArrayLen(reader);
        if (topics_len < 0) {
            for (brokers) |*b| b.deinit(allocator);
            return error.InvalidArrayLength;
        }

        const topics = try allocator.alloc(TopicMetadata, @intCast(topics_len));
        errdefer allocator.free(topics);

        for (topics) |*topic| {
            const error_code = constants.ErrorCode.fromInt(try broker.readInt16(reader));
            const topic_name = try broker.readString(reader, allocator);

            // Parse partitions
            const partitions_len = try broker.readArrayLen(reader);
            if (partitions_len < 0) {
                allocator.free(topic_name);
                return error.InvalidArrayLength;
            }

            const partitions = try allocator.alloc(PartitionMetadata, @intCast(partitions_len));
            errdefer allocator.free(partitions);

            for (partitions) |*partition| {
                const part_error_code = constants.ErrorCode.fromInt(try broker.readInt16(reader));
                const partition_id = try broker.readInt32(reader);
                const leader = try broker.readInt32(reader);

                // Parse replicas
                const replicas_len = try broker.readArrayLen(reader);
                if (replicas_len < 0) {
                    return error.InvalidArrayLength;
                }
                const replicas = try allocator.alloc(i32, @intCast(replicas_len));
                for (replicas) |*replica| {
                    replica.* = try broker.readInt32(reader);
                }

                // Parse ISR
                const isr_len = try broker.readArrayLen(reader);
                if (isr_len < 0) {
                    allocator.free(replicas);
                    return error.InvalidArrayLength;
                }
                const isr = try allocator.alloc(i32, @intCast(isr_len));
                for (isr) |*isr_id| {
                    isr_id.* = try broker.readInt32(reader);
                }

                partition.* = PartitionMetadata{
                    .error_code = part_error_code,
                    .partition_id = partition_id,
                    .leader = leader,
                    .replicas = replicas,
                    .isr = isr,
                };
            }

            topic.* = TopicMetadata{
                .error_code = error_code,
                .topic_name = topic_name,
                .partitions = partitions,
            };
        }

        return MetadataResponse{
            .brokers = brokers,
            .topics = topics,
        };
    }
};

/// Send a Metadata request
pub fn metadata(client: *Client, topics: ?[]const []const u8) !MetadataResponse {
    const request = MetadataRequest{ .topics = topics };

    const ret = try client.sendRequest(
        .Metadata,
        1, // version 0
        request,
        MetadataResponse,
        MetadataResponse.parse,
    );
    return ret;
}

// =============================================================================
// Produce Request/Response (API Key 0)
// =============================================================================

/// A single message to produce
pub const Message = struct {
    key: ?[]const u8,
    value: []const u8,
};

/// Produce request - send messages to a topic partition
pub const ProduceRequest = struct {
    required_acks: i16, // 0=no ack, 1=leader ack, -1=all replicas ack
    timeout: i32, // timeout in ms
    topic: []const u8,
    partition: i32,
    messages: []const Message,

    pub fn write(self: ProduceRequest, writer: anytype) !void {
        try broker.writeInt16(writer, self.required_acks);
        try broker.writeInt32(writer, self.timeout);

        // Write topic data array (length 1)
        try broker.writeArrayLen(writer, 1);

        // Write topic name
        try broker.writeString(writer, self.topic);

        // Write partition data array (length 1)
        try broker.writeArrayLen(writer, 1);

        // Write partition
        try broker.writeInt32(writer, self.partition);

        // Write message set size (we'll compute it)
        var message_set_buf: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
        defer message_set_buf.deinit();
        const message_set_writer = &message_set_buf.writer;

        // Write each message
        for (self.messages) |message| {
            // Offset (not used in produce, set to 0)
            try broker.writeInt64(message_set_writer, 0);

            // Compute message size
            const key_size: i32 = if (message.key) |k| @intCast(k.len) else -1;
            const value_size: i32 = @intCast(message.value.len);
            const message_size: i32 = 4 + // CRC
                1 + // magic byte
                1 + // attributes
                4 + // key length
                (if (key_size >= 0) key_size else 0) + // key
                4 + // value length
                value_size; // value

            // Write message size
            try broker.writeInt32(message_set_writer, message_size);

            // Calculate CRC placeholder (should calculate actual CRC32)
            const crc: i32 = 0;
            try broker.writeInt32(message_set_writer, crc);

            // Magic byte (0 for v0)
            try broker.writeInt8(message_set_writer, 0);

            // Attributes (0 for no compression)
            try broker.writeInt8(message_set_writer, 0);

            // Key
            if (message.key) |key| {
                try broker.writeInt32(message_set_writer, @intCast(key.len));
                try broker.writeAll(message_set_writer, key);
            } else {
                try broker.writeInt32(message_set_writer, -1);
            }

            // Value
            try broker.writeInt32(message_set_writer, @intCast(message.value.len));
            try broker.writeAll(message_set_writer, message.value);
        }

        // Write message set size and data
        try broker.writeInt32(writer, @intCast(message_set_buf.written().len));
        try broker.writeAll(writer, message_set_buf.written());
    }
};

/// Partition produce response
pub const PartitionProduceResponse = struct {
    partition: i32,
    error_code: constants.ErrorCode,
    offset: i64,
};

/// Topic produce response
pub const TopicProduceResponse = struct {
    topic_name: []u8,
    partitions: []PartitionProduceResponse,

    pub fn deinit(self: *TopicProduceResponse, allocator: Allocator) void {
        allocator.free(self.topic_name);
        allocator.free(self.partitions);
    }
};

/// Produce response
pub const ProduceResponse = struct {
    topics: []TopicProduceResponse,

    pub fn deinit(self: *ProduceResponse, allocator: Allocator) void {
        for (self.topics) |*topic| {
            topic.deinit(allocator);
        }
        allocator.free(self.topics);
    }

    pub fn parse(reader: anytype, allocator: Allocator) !ProduceResponse {
        const topics_len = try broker.readArrayLen(reader);
        if (topics_len < 0) {
            return error.InvalidArrayLength;
        }

        const topics = try allocator.alloc(TopicProduceResponse, @intCast(topics_len));
        errdefer allocator.free(topics);

        for (topics) |*topic| {
            const topic_name = try broker.readString(reader, allocator);

            const partitions_len = try broker.readArrayLen(reader);
            if (partitions_len < 0) {
                allocator.free(topic_name);
                return error.InvalidArrayLength;
            }

            const partitions = try allocator.alloc(PartitionProduceResponse, @intCast(partitions_len));
            errdefer allocator.free(partitions);

            for (partitions) |*partition| {
                partition.* = PartitionProduceResponse{
                    .partition = try broker.readInt32(reader),
                    .error_code = constants.ErrorCode.fromInt(try broker.readInt16(reader)),
                    .offset = try broker.readInt64(reader),
                };
            }

            topic.* = TopicProduceResponse{
                .topic_name = topic_name,
                .partitions = partitions,
            };
        }

        return ProduceResponse{
            .topics = topics,
        };
    }
};

/// Send a Produce request
pub fn produce(
    client: *Client,
    topic: []const u8,
    partition: i32,
    messages: []const Message,
    required_acks: i16,
    timeout: i32,
) !ProduceResponse {
    const request = ProduceRequest{
        .required_acks = required_acks,
        .timeout = timeout,
        .topic = topic,
        .partition = partition,
        .messages = messages,
    };

    return try client.sendRequest(
        .Produce,
        0, // version 0
        request,
        ProduceResponse,
        ProduceResponse.parse,
    );
}
