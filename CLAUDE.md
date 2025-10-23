# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Kafka client library written in Zig that implements the Apache Kafka binary protocol over TCP. This is a demonstration/educational project showing how to implement protocol handlers from scratch without external dependencies.

**Minimum Zig version**: 0.15.1

## Build and Run Commands

```bash
# Build the project (creates zig-out/bin/z)
zig build

# Run the demo application
zig build run

# Run all tests (module and executable tests in parallel)
zig build test
```

The build system creates two artifacts:
- A module named "kafka.zig" (in build.zig) that can be imported by other projects
- An executable "z" that demonstrates the library functionality

## Architecture

### Module Structure

The codebase follows a layered architecture with clear separation of concerns:

```
src/kafka/protocol.zig  →  Low-level protocol primitives
src/kafka/client.zig    →  TCP connection and request/response lifecycle
src/kafka/requests.zig  →  High-level API implementations
src/root.zig            →  Public API (re-exports, module root)
src/main.zig            →  Demo executable
```

**Protocol Layer** (`protocol.zig`):
- Big-endian integer serialization/deserialization (writeInt16/32/64, readInt16/32/64)
- Kafka string format: int16 length prefix + UTF-8 data
- Kafka bytes format: int32 length prefix + data
- Nullable types: -1 length for null values
- Custom `writeAll` helper to handle partial writes
- Error code enum with human-readable descriptions

**Client Layer** (`client.zig`):
- TCP connection management via `net.tcpConnectToHost`
- Generic `sendRequest` method that handles:
  - Request serialization to ArrayList buffer
  - 4-byte big-endian size prefix framing
  - Manual write loops for partial writes
  - Response size validation (4 bytes to 100MB)
  - Manual read loops for partial reads
  - Response deserialization with correlation ID verification
- Currently uses hardcoded correlation ID (999) - noted in code comments

**Requests Layer** (`requests.zig`):
- Implements specific Kafka API requests (ApiVersions, Metadata, Produce)
- Each request has a struct with `write` method for serialization
- Each response has a struct with `parse` method and `deinit` for cleanup
- Convenience functions (apiVersions, metadata, produce) that wrap client.sendRequest

### Kafka Protocol Implementation Details

**Wire Protocol**:
1. Each message prefixed with 4-byte big-endian length
2. Request header: API key (i16), version (i16), correlation ID (i32), client ID (string)
3. Response header: correlation ID (i32) - must match request
4. Request/response bodies are API-specific

**Current Implementation Limitations**:
- Only protocol version 0 for all APIs (compatible with Kafka 0.8.x+)
- Legacy message format (v0), not record batch format
- No CRC checksum calculation (set to 0)
- No compression support
- No consumer APIs (Fetch, Offset management)
- No authentication (SASL/SSL)

**Supported APIs**:
- **ApiVersions** (Key 18, v0): Query broker capabilities, no auth required
- **Metadata** (Key 3, v0): List topics/brokers, get partition information
- **Produce** (Key 0, v0): Send messages to topics, configurable acks (0/1/-1)

### Zig 0.15.1 Specific Patterns

The code uses these Zig 0.15.1 APIs:
- `std.ArrayList` with `.empty` initialization
- `.writer(allocator)` method on ArrayList
- `std.fs.File.stdout()` with buffered writer
- `net.Stream` with manual read/write loops (no `readAll`/`writeAll` on Stream)
- Custom `protocol.writeAll` helper for handling partial writes
- `reader.readNoEof` for reading exact byte counts

## Documentation Sources

This project was built using the Context7 MCP server with these libraries:
- `/jedisct1/zig-for-mcp` - Zig 0.15.1 standard library documentation
- `/apache/kafka` - Kafka wire protocol specification
- `/confluentinc/librdkafka` - Reference C implementation

## Development Notes

**When adding new Kafka API support**:
1. Add API key to `ApiKey` enum in protocol.zig
2. Create request struct with `write(self, writer)` method in requests.zig
3. Create response struct with `parse(reader, allocator)` and `deinit(self, allocator)` methods
4. Add convenience function that calls `client.sendRequest` with appropriate types
5. Handle all allocated memory in response.deinit (strings, arrays, nested structs)

**Memory Management**:
- All response parsing allocates memory - callers must call `.deinit(allocator)`
- String/bytes fields are owned by response structs
- Arrays of structs need per-element and array cleanup in deinit

**Testing Against Kafka**:
```bash
# Start Kafka with KRaft (no Zookeeper)
docker run -p 9092:9092 apache/kafka:latest

# Create test topic
docker exec -it <container> /opt/kafka/bin/kafka-topics.sh \
  --create --topic test --bootstrap-server localhost:9092
```

## Future Enhancements Roadmap

See README.md for the full list. Key missing features:
- Consumer APIs (Fetch, ListOffsets, OffsetCommit, OffsetFetch)
- Newer protocol versions and API version negotiation
- Record batch format (Kafka 0.11+)
- Compression (Snappy, LZ4, Zstd)
- SASL/SSL authentication
- Connection pooling and automatic metadata refresh
