const std = @import("std");

const KafkaVersion = struct {
    version: [4]u32,

    fn newKafkaVersion(major: u32, minor: u32, veryMinor: u32, patch: u32) KafkaVersion {
        return KafkaVersion{
            .version = [4]u32{ major, minor, veryMinor, patch },
        };
    }
};

const ApiVersionRange = struct {
    maxVersion: u16,
    minVersion: u16
};

const ApiVersionMap: std.AutoHashMap(u16, ApiVersionRange) = undefined;
