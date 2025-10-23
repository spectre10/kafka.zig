const std = @import("std");
const kafka = @import("kafka.zig");

pub fn main() !void {
    // Setup allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("=== Kafka Client Demo ===\n\n", .{});

    // Configuration
    const kafka_host = "localhost";
    const kafka_port = 9092;
    const client_id = "my-client";

    std.debug.print("Connecting to Kafka at {s}:{d}...\n", .{ kafka_host, kafka_port });

    // Connect to Kafka broker
    var client = kafka.Client.connect(allocator, kafka_host, kafka_port, client_id) catch |err| {
        std.debug.print("Failed to connect to Kafka: {}\n", .{err});
        std.debug.print("\nMake sure Kafka is running at {s}:{d}\n", .{ kafka_host, kafka_port });
        std.debug.print("You can start Kafka with Docker:\n", .{});
        std.debug.print("  docker run -p 9092:9092 apache/kafka:latest\n\n", .{});
        return err;
    };
    defer client.close();

    std.debug.print("Connected successfully!\n\n", .{});

    // 1. Query API Versions
    // std.debug.print("--- API Versions Request ---\n", .{});
    // var api_versions_response = try kafka.apiVersions(&client);
    // defer api_versions_response.deinit(allocator);
    //
    // if (api_versions_response.error_code.isError()) {
    //     std.debug.print("Error: {s}\n", .{api_versions_response.error_code.toString()});
    // } else {
    //     std.debug.print("Broker supports {} API versions:\n", .{api_versions_response.api_versions.len});
    //     for (api_versions_response.api_versions[0..@min(10, api_versions_response.api_versions.len)]) |api_version| {
    //         std.debug.print("  API Key {d}: versions {d}-{d}\n", .{
    //             api_version.api_key,
    //             api_version.min_version,
    //             api_version.max_version,
    //         });
    //     }
    //     if (api_versions_response.api_versions.len > 10) {
    //         std.debug.print("  ... and {} more\n", .{api_versions_response.api_versions.len - 10});
    //     }
    // }
    // std.debug.print("\n", .{});

    // 2. Query Metadata
    std.debug.print("--- Metadata Request ---\n", .{});
    var metadata_response = kafka.metadata(&client, null) catch |err| {
        std.debug.print("Metadata request failed: {}\n", .{err});
        std.debug.print("This might happen if the broker is still initializing.\n\n", .{});
        return;
    };
    defer metadata_response.deinit(allocator);

    std.debug.print("Cluster has {} brokers:\n", .{metadata_response.brokers.len});
    for (metadata_response.brokers) |broker| {
        std.debug.print("  Broker {d}: {s}:{d}\n", .{ broker.node_id, broker.host, broker.port });
    }
    std.debug.print("\n", .{});

    std.debug.print("Cluster has {} topics:\n", .{metadata_response.topics.len});
    for (metadata_response.topics[0..@min(5, metadata_response.topics.len)]) |topic| {
        if (topic.error_code.isError()) {
            std.debug.print("  {s}: Error - {s}\n", .{ topic.topic_name, topic.error_code.toString() });
        } else {
            std.debug.print("  {s}: {} partitions\n", .{ topic.topic_name, topic.partitions.len });
        }
    }
    if (metadata_response.topics.len > 5) {
        std.debug.print("  ... and {} more\n", .{metadata_response.topics.len - 5});
    }
    std.debug.print("\n", .{});

    // 3. Produce a message (if a test topic exists)
    const test_topic = "test";
    var topic_exists = false;
    for (metadata_response.topics) |topic| {
        if (std.mem.eql(u8, topic.topic_name, test_topic) and !topic.error_code.isError()) {
            topic_exists = true;
            break;
        }
    }

    if (topic_exists) {
        std.debug.print("--- Produce Request ---\n", .{});
        std.debug.print("Sending message to topic '{s}'...\n", .{test_topic});

        const messages = [_]kafka.Message{
            .{
                .key = null,
                .value = "Hello from Zig Kafka Client!",
            },
        };

        var produce_response = kafka.produce(
            &client,
            test_topic,
            0, // partition 0
            &messages,
            1, // wait for leader ack
            5000, // 5 second timeout
        ) catch |err| {
            std.debug.print("Produce request failed: {}\n", .{err});
            std.debug.print("Note: The message format is simplified and may not work with modern Kafka.\n\n", .{});
            return;
        };
        defer produce_response.deinit(allocator);

        for (produce_response.topics) |topic_response| {
            std.debug.print("Topic: {s}\n", .{topic_response.topic_name});
            for (topic_response.partitions) |partition_response| {
                if (partition_response.error_code.isError()) {
                    std.debug.print("  Partition {d}: Error - {s}\n", .{
                        partition_response.partition,
                        partition_response.error_code.toString(),
                    });
                } else {
                    std.debug.print("  Partition {d}: Success! Offset: {d}\n", .{
                        partition_response.partition,
                        partition_response.offset,
                    });
                }
            }
        }
        std.debug.print("\n", .{});
    } else {
        std.debug.print("--- Produce Request (Skipped) ---\n", .{});
        std.debug.print("Topic '{s}' does not exist. Create it with:\n", .{test_topic});
        std.debug.print("  docker exec -it <container> /opt/kafka/bin/kafka-topics.sh \\\n", .{});
        std.debug.print("    --create --topic test --bootstrap-server localhost:9092\n\n", .{});
    }

    std.debug.print("Demo completed successfully!\n", .{});
}
