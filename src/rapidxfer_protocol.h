// rapidxfer_protocol.h
//
// RapidXfer v2.0 Unified Protocol Definition
// All messages are data packets, distinguished by flags
//
// Copyright (C) 2026 RapidXfer Team

#ifndef RAPIDXFER_PROTOCOL_H_
#define RAPIDXFER_PROTOCOL_H_

#include <cstdint>
#include <vector>
#include "rapid_transfer.h"

namespace rapid {
namespace rapidxfer {

// Message flags (16-bit)
enum MessageFlags : uint16_t {
    DATA_PACKET       = 0x0000,  // Normal data packet
    READ_REQUEST      = 0x0001,  // Read request (pull-based read)
    SACK              = 0x0002,  // Selective ACK
    CHUNK_ACK         = 0x0004,  // Chunk completion ACK
    NOTIFICATION      = 0x0008,  // Application notification (optional)
};

// Unified packet header (24 bytes, fixed size)
struct __attribute__((packed)) RapidXferHeader {
    uint32_t session_id;    // Session ID
    uint32_t chunk_id;      // Chunk ID (4MB aligned)
    uint16_t seq_num;       // Sequence number within chunk
    uint16_t flags;         // MessageFlags
    uint64_t timestamp;     // Timestamp (microseconds)

    // Helper methods
    bool isDataPacket() const { return flags == DATA_PACKET; }
    bool isReadRequest() const { return flags & READ_REQUEST; }
    bool isSACK() const { return flags & SACK; }
    bool isChunkAck() const { return flags & CHUNK_ACK; }
    bool isNotification() const { return flags & NOTIFICATION; }
};

// Read Request Payload
struct ReadRequestPayload {
    uint32_t num_local_targets;
    std::vector<Buffer> local_targets;   // Sender's receive addresses

    uint32_t num_remote_sources;
    std::vector<Buffer> remote_sources;  // Receiver's data sources
};

// SACK Payload
struct __attribute__((packed)) SACKPayload {
    uint64_t recv_bitmap;      // 64-bit bitmap of received packets
    uint16_t bitmap_start;     // Starting sequence number
};

// Chunk-ACK Payload
struct __attribute__((packed)) ChunkAckPayload {
    uint32_t total_pkts;       // Total packets in chunk
    uint64_t recv_bitmap;      // Complete bitmap (all 1s = complete)
};

// Notification Payload
struct NotificationPayload {
    int task_id;
    char message[256];         // Variable length, max 256 bytes
    uint16_t message_length;
};

// Constants
constexpr size_t kChunkSize = 4 * 1024 * 1024;  // 4MB per chunk
constexpr size_t kMaxPacketsPerChunk = 64;        // Max packets per chunk (bitmap size)
constexpr uint64_t kChunkCompleteMask = 0xFFFFFFFFFFFFFFFFULL;

} // namespace rapidxfer
} // namespace rapid

#endif  // RAPIDXFER_PROTOCOL_H_
