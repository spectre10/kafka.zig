//! Kafka client library for Zig
//!
//! This library provides a simple Kafka client implementation that supports
//! basic operations like querying API versions, fetching metadata, and
//! producing messages.
//!
//! Example usage:
//! ```zig
//! var client = try kafka.Client.connect(allocator, "localhost", 9092, "my-client");
//! defer client.close();
//!
//! var response = try kafka.apiVersions(&client);
//! defer response.deinit(allocator);
//! ```

const std = @import("std");

// Re-export public modules
pub const protocol = @import("kafka/protocol.zig");
pub const client = @import("kafka/client.zig");
pub const requests = @import("kafka/requests.zig");

// Re-export commonly used types
pub const Client = client.Client;
pub const ApiKey = protocol.ApiKey;
pub const ErrorCode = protocol.ErrorCode;

// Re-export request/response types
pub const ApiVersionsResponse = requests.ApiVersionsResponse;
pub const ApiVersion = requests.ApiVersion;
pub const MetadataResponse = requests.MetadataResponse;
pub const Broker = requests.Broker;
pub const TopicMetadata = requests.TopicMetadata;
pub const PartitionMetadata = requests.PartitionMetadata;
pub const ProduceResponse = requests.ProduceResponse;
pub const Message = requests.Message;

// Re-export convenience functions
pub const apiVersions = requests.apiVersions;
pub const metadata = requests.metadata;
pub const produce = requests.produce;

test {
    // Run tests from all submodules
    std.testing.refAllDecls(@This());
    std.testing.refAllDecls(protocol);
}
