# Kafka Client for Zig

A simple Apache Kafka client implementation written in Zig, demonstrating the Kafka binary protocol.

## Overview

This project implements a basic Kafka client in Zig that supports:
- **ApiVersions**: Query broker API version support
- **Metadata**: Fetch topic and broker information
- **Produce**: Send messages to Kafka topics

The implementation uses the Kafka wire protocol directly over TCP without external dependencies.

## Project Structure

```
src/
├── kafka/
│   ├── protocol.zig     # Binary protocol primitives (int16, int32, strings, etc.)
│   ├── client.zig       # TCP connection and request/response handling
│   └── requests.zig     # Specific Kafka requests (ApiVersions, Metadata, Produce)
├── kafka.zig            # Public API
└── main.zig             # Demo application
```

## Key Features

### Protocol Implementation
- Big-endian integer serialization/deserialization
- Kafka string format (int16 length + UTF-8)
- Kafka byte array format (int32 length + data)
- Nullable types support
- Error code handling

### Client Features
- TCP connection to Kafka brokers
- Request/response correlation
- Automatic message framing (size prefix)
- Generic request/response handling

### Supported Requests

**ApiVersions** (API Key 18)
- Query broker for supported API versions
- No authentication required
- Useful for broker compatibility checks

**Metadata** (API Key 3)
- List all topics or query specific topics
- Returns broker list with host:port
- Returns partition information (leader, replicas, ISR)

**Produce** (API Key 0)
- Send messages to topic partitions
- Support for keyed and unkeyed messages
- Configurable acknowledgment levels (0, 1, -1)

## Usage

### Running the Demo

```bash
# Build the project
zig build

# Run with a local Kafka broker
./zig-out/bin/z
```

### Starting Kafka (Docker)

```bash
# Start Kafka with KRaft (no Zookeeper)
docker run -p 9092:9092 apache/kafka:latest

# Create a test topic
docker exec -it <container> /opt/kafka/bin/kafka-topics.sh \
  --create --topic test --bootstrap-server localhost:9092
```

### Code Example

```zig
const kafka = @import("kafka.zig");

// Connect to broker
var client = try kafka.Client.connect(allocator, "localhost", 9092, "my-client");
defer client.close();

// Query API versions
var versions = try kafka.apiVersions(&client);
defer versions.deinit(allocator);

// Fetch metadata
var metadata = try kafka.metadata(&client, null);
defer metadata.deinit(allocator);

// Produce a message
const messages = [_]kafka.Message{
    .{ .key = null, .value = "Hello from Zig!" },
};
var response = try kafka.produce(
    &client,
    "test",          // topic
    0,               // partition
    &messages,
    1,               // wait for leader ack
    5000,            // timeout ms
);
defer response.deinit(allocator);
```

## Implementation Details

### Kafka Protocol

The implementation follows the [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol.html):

1. **Message Framing**: Each request/response is prefixed with a 4-byte big-endian length
2. **Request Header**: API key, version, correlation ID, client ID
3. **Response Header**: Correlation ID (must match request)
4. **Request Body**: API-specific fields
5. **Response Body**: Error code + API-specific fields

### API Version Compatibility

This implementation uses Kafka protocol version 0 for all requests, which is compatible with:
- Kafka 0.8.x and later
- Modern Kafka brokers (backward compatible)

For production use, implement API version negotiation using the ApiVersions request.

### Error Handling

The client includes comprehensive error handling:
- Network errors (connection refused, timeout, etc.)
- Protocol errors (invalid message size, correlation ID mismatch)
- Kafka errors (topic not found, authorization failed, etc.)

## Limitations

### Current Implementation

1. **Protocol Version**: Only implements version 0 of each API
2. **Message Format**: Uses legacy message format (v0), not the newer record batch format
3. **CRC Validation**: CRC checksums are not calculated (set to 0)
4. **Compression**: No compression support
5. **Consumer**: No consumer implementation (Fetch, Offset management)
6. **Authentication**: No SASL/SSL support

### Zig Version Compatibility

This code was developed for **Zig 0.15.1** and uses:
- `std.fs.File.stdout()` with buffered writer
- `std.ArrayList` with `.empty` initialization
- ArrayList `.writer(allocator)` method
- `net.Stream` with direct `.read()` and `.write()` methods (manual loops for full reads/writes)
- Custom `writeAll` helper function for handling partial writes in protocol primitives

**Status**: ✅ Successfully compiles with Zig 0.15.1

## Documentation Sources

This implementation was built using the Context7 MCP server to retrieve documentation:

- **Zig Documentation**: `/jedisct1/zig-for-mcp` - Zig standard library APIs
- **Kafka Protocol**: `/apache/kafka` - Wire protocol specification
- **librdkafka**: `/confluentinc/librdkafka` - Reference C implementation

## Future Enhancements

- [x] Complete Zig 0.15.1 API compatibility
- [ ] Implement Consumer (Fetch, ListOffsets, OffsetCommit, OffsetFetch)
- [ ] Support newer protocol versions
- [ ] Implement record batch format (Kafka 0.11+)
- [ ] Add CRC32 checksum calculation
- [ ] Compression support (Snappy, LZ4, Zstd)
- [ ] SASL authentication
- [ ] SSL/TLS support
- [ ] Connection pooling
- [ ] Automatic metadata refresh
- [ ] Partition assignment strategies
- [ ] Exactly-once semantics (transactions)

## License

This is a demonstration project created for educational purposes.

## Contributing

This project was created using Claude Code with extensive use of the Context7 MCP server for retrieving API documentation. Contributions are welcome!
