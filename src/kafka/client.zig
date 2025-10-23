//! Kafka client implementation
//!
//! This module provides the core client functionality for connecting to
//! Kafka brokers and sending/receiving messages.

const std = @import("std");
const net = std.net;
const mem = std.mem;
const Allocator = mem.Allocator;
const protocol = @import("protocol.zig");

/// Kafka client for managing connections to brokers
pub const Client = struct {
    allocator: Allocator,
    stream: net.Stream,
    host: []const u8,
    port: u16,
    client_id: []const u8,
    next_correlation_id: i32,

    /// Connect to a Kafka broker
    pub fn connect(allocator: Allocator, host: []const u8, port: u16, client_id: []const u8) !Client {
        const stream = try net.tcpConnectToHost(allocator, host, port);
        errdefer stream.close();

        return Client{
            .allocator = allocator,
            .stream = stream,
            .host = host,
            .port = port,
            .client_id = client_id,
            .next_correlation_id = 0,
        };
    }

    /// Close the connection to the broker
    pub fn close(self: *Client) void {
        self.stream.close();
    }

    /// Get the next correlation ID for a request
    fn getCorrelationId(self: *Client) i32 {
        const id = self.next_correlation_id;
        self.next_correlation_id +%= 1;
        return id;
    }

    /// Send a request and receive a response
    /// The request_body should write its contents to the provided writer
    /// The response_parser should read from the provided reader and return the response
    pub fn sendRequest(
        self: *Client,
        api_key: protocol.ApiKey,
        api_version: i16,
        request_body: anytype,
        comptime ResponseType: type,
        response_parser_fn: *const fn (anytype, Allocator) anyerror!ResponseType,
    ) !ResponseType {
        // Generate correlation ID
        // const correlation_id = self.getCorrelationId();

        // Create request header
        const header = protocol.RequestHeader{
            .api_key = api_key.toInt(),
            .api_version = api_version,
            .correlation_id = 999,
            .client_id = self.client_id,
        };

        // Serialize request to a buffer
        var request_buf: std.ArrayList(u8) = .empty;
        defer request_buf.deinit(self.allocator);

        const request_writer = request_buf.writer(self.allocator);

        // Write header
        try header.write(request_writer);
        

        // Write body
        try request_body.write(request_writer);

        // Send size prefix + request
        const size: i32 = @intCast(request_buf.items.len);

        // Write size prefix (4 bytes, big-endian)
        var size_buf: [4]u8 = undefined;
        mem.writeInt(i32, &size_buf, size, protocol.endian);
        {
            var written: usize = 0;
            while (written < size_buf.len) {
                written += try self.stream.write(size_buf[written..]);
            }
        }

        
        // Write request body
        {
            var written: usize = 0;
            while (written < request_buf.items.len) {
                written += try self.stream.write(request_buf.items[written..]);
            }
        }

        /////////////////////////////////////
        for (size_buf) |byte| {
            std.debug.print("0x{x:0>2} ", .{byte});
        }
        std.debug.print("\n", .{});
        /////////////////////////////////////
        // Read response size (4 bytes, big-endian)
        var response_size_buf: [4]u8 = undefined;
        {
            var readn: usize = 0;
            while (readn < 4) {
                const n = try self.stream.read(response_size_buf[readn..]);
                if (n == 0) {
                    return error.UnexpectedEndOfStream;
                }
                readn += n;
            }
        }
        const response_size = mem.readInt(i32, &response_size_buf, protocol.endian);

        if (response_size < 4 or response_size > 100 * 1024 * 1024) {
            return error.InvalidResponseSize;
        }

        // Read response data
        const response_data = try self.allocator.alloc(u8, @intCast(response_size));
        defer self.allocator.free(response_data);
        {
            var readn: usize = 0;
            while (readn < response_data.len) {
                const n = try self.stream.read(response_data[readn..]);
                if (n == 0) {
                    return error.UnexpectedEndOfStream;
                }
                readn += n;
            }
        }

        // Parse response
        var response_stream = std.io.fixedBufferStream(response_data);
        const response_reader = response_stream.reader();

        // Read and verify response header
        const response_header = try protocol.ResponseHeader.read(response_reader);
        if (response_header.correlation_id != 999) {
            return error.CorrelationIdMismatch;
        }

        // Parse response body
        return try response_parser_fn(response_reader, self.allocator);
    }
};
