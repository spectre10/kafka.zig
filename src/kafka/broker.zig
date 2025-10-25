const std = @import("std");
const Io = std.Io;
const net = std.net;
const mem = std.mem;
const Allocator = mem.Allocator;

/// Kafka protocol uses big-endian byte order for all integers
pub const endian = .big;

/// Request header for Kafka protocol
pub const RequestHeader = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: []const u8,

    /// Write the request header to a writer
    pub fn write(self: RequestHeader, writer: *Io.Writer) !void {
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
    pub fn read(reader: *Io.Reader) !ResponseHeader {
        return ResponseHeader{
            .correlation_id = try readInt32(reader),
        };
    }
};

const broker = struct {
    stream: net.Stream,
    allocator: Allocator,
    host: []const u8,
    port: u16,
};

/// Write an int8 in big-endian format
pub fn writeInt8(writer: *Io.Writer, value: i8) !void {
    try writer.writeByte(@bitCast(value));
}

/// Write an int16 in big-endian format
pub fn writeInt16(writer: *Io.Writer, value: i16) !void {
    var buf: [2]u8 = undefined;
    mem.writeInt(i16, &buf, value, endian);
    try writeAll(writer, &buf);
}

/// Write an int32 in big-endian format
pub fn writeInt32(writer: *Io.Writer, value: i32) !void {
    var buf: [4]u8 = undefined;
    mem.writeInt(i32, &buf, value, endian);
    try writeAll(writer, &buf);
}

/// Write an int64 in big-endian format
pub fn writeInt64(writer: *Io.Writer, value: i64) !void {
    var buf: [8]u8 = undefined;
    mem.writeInt(i64, &buf, value, endian);
    try writeAll(writer, &buf);
}

/// Write a string in Kafka format (int16 length prefix + UTF-8 data)
pub fn writeString(writer: *Io.Writer, value: []const u8) !void {
    if (value.len > std.math.maxInt(i16)) {
        return error.StringTooLong;
    }
    try writeInt16(writer, @intCast(value.len));
    try writeAll(writer, value);
}

/// Write a nullable string in Kafka format (-1 for null, or length + data)
pub fn writeNullableString(writer: *Io.Writer, value: ?[]const u8) !void {
    if (value) |str| {
        try writeString(writer, str);
    } else {
        try writeInt16(writer, -1);
    }
}

/// Write a byte array in Kafka format (int32 length prefix + data)
pub fn writeBytes(writer: *Io.Writer, value: []const u8) !void {
    if (value.len > std.math.maxInt(i32)) {
        return error.BytesTooLong;
    }
    try writeInt32(writer, @intCast(value.len));
    try writeAll(writer, value);
}

/// Write a nullable byte array in Kafka format (-1 for null, or length + data)
pub fn writeNullableBytes(writer: *Io.Writer, value: ?[]const u8) !void {
    if (value) |bytes| {
        try writeBytes(writer, bytes);
    } else {
        try writeInt32(writer, -1);
    }
}

/// Write an array length (int32)
pub fn writeArrayLen(writer: *Io.Writer, len: usize) !void {
    if (len > std.math.maxInt(i32)) {
        return error.ArrayTooLong;
    }
    try writeInt32(writer, @intCast(len));
}
//
// fn readTillEof(reader: *Io.Reader, buf: []u8) !void {
//     while (true) {
//         const n = reader.readSliceShort(buf);
//     }
// }

/// Read an int8
pub fn readInt8(reader: *Io.Reader) !i8 {
    const byte = try reader.readByte();
    return @bitCast(byte);
}

/// Read an int16 in big-endian format
pub fn readInt16(reader: *Io.Reader) !i16 {
    var buf: [2]u8 = undefined;
    _ = try reader.readSliceShort(&buf);
    return mem.readInt(i16, &buf, endian);
}

/// Read an int32 in big-endian format
pub fn readInt32(reader: *Io.Reader) !i32 {
    var buf: [4]u8 = undefined;
    _ = try reader.readSliceShort(&buf);
    return mem.readInt(i32, &buf, endian);
}

/// Read an int64 in big-endian format
pub fn readInt64(reader: *Io.Reader) !i64 {
    var buf: [8]u8 = undefined;
    _ = try reader.readSliceShort(&buf);
    return mem.readInt(i64, &buf, endian);
}

/// Read a string in Kafka format (int16 length prefix + UTF-8 data)
/// Caller owns the returned memory
pub fn readString(reader: *Io.Reader, allocator: Allocator) ![]u8 {
    const len = try readInt16(reader);
    if (len < 0) {
        return error.UnexpectedNullString;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    _ = try reader.readSliceShort(buf);
    return buf;
}

/// Read a nullable string in Kafka format
/// Returns null if length is -1, otherwise allocates and returns the string
/// Caller owns the returned memory
pub fn readNullableString(reader: *Io.Reader, allocator: Allocator) !?[]u8 {
    const len = try readInt16(reader);
    if (len < 0) {
        return null;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    _ = try reader.readSliceShort(buf);
    return buf;
}

/// Read a byte array in Kafka format (int32 length prefix + data)
/// Caller owns the returned memory
pub fn readBytes(reader: *Io.Reader, allocator: Allocator) ![]u8 {
    const len = try readInt32(reader);
    if (len < 0) {
        return error.UnexpectedNullBytes;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    _ = try reader.readSliceShort(buf);
    return buf;
}

/// Read a nullable byte array in Kafka format
/// Returns null if length is -1, otherwise allocates and returns the bytes
/// Caller owns the returned memory
pub fn readNullableBytes(reader: *Io.Reader, allocator: Allocator) !?[]u8 {
    const len = try readInt32(reader);
    if (len < 0) {
        return null;
    }
    const buf = try allocator.alloc(u8, @intCast(len));
    errdefer allocator.free(buf);
    _ = try reader.readSliceShort(buf);
    return buf;
}

/// Read an array length (int32)
pub fn readArrayLen(reader: *Io.Reader) !i32 {
    return try readInt32(reader);
}

/// Helper to write all bytes, handling partial writes
pub fn writeAll(writer: *Io.Writer, bytes: []const u8) !void {
    var index: usize = 0;
    while (index < bytes.len) {
        const written = try writer.write(bytes[index..]);
        if (written == 0) return error.EndOfStream;
        index += written;
    }
}

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
