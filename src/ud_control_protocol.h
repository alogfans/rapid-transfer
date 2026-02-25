// ud_control_protocol.h
//
// Control message protocol for RDMA UD-based control plane
// Replaces TCP RPC with UD-based control messaging
//
// Copyright (C) 2024 Feng Ren

#ifndef UD_CONTROL_PROTOCOL_H_
#define UD_CONTROL_PROTOCOL_H_

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rapid_transfer.h"

namespace rapid {

// Buffer info structure (moved from session_manager.h)
// This is used for e2e testing and buffer info exchange
struct BufferInfo {
    uint64_t addr;   // Buffer address as uint64_t for safe serialization
    uint64_t length; // Buffer size
    uint32_t rkey;   // Remote key
};

namespace ud {

// NOTE: PKT_CMD_DATA and PKT_CMD_ACK are defined in packet_manager.h as macros
// We define PKT_CMD_CONTROL here as a new command type for control messages
// The actual values are:
// PKT_CMD_DATA = 0 (from packet_manager.h)
// PKT_CMD_ACK = 1 (from packet_manager.h)
constexpr uint8_t PKT_CMD_CONTROL = 2;  // All control messages use this

// Control message types (in payload)
enum class ControlMessageType : uint16_t {
    // Connection establishment
    CONNECTION_REQUEST = 1,    // Initial connection request
    CONNECTION_RESPONSE = 2,   // Connection response with UD attributes

    // Data transfer operations
    READ_REQUEST = 3,          // Request remote peer to send data
    READ_RESPONSE = 4,         // Response to read request

    // Notifications
    NOTIFICATION = 5,          // Notification message

    // Buffer info exchange
    BUFFER_INFO_REQUEST = 6,   // Request buffer information
    BUFFER_INFO_RESPONSE = 7,  // Response with buffer info

    // Error handling
    ERROR_RESPONSE = 8,        // Error response
};

// Control message header (in payload, after PktHdrImm/PktHdr)
struct ControlMessageHeader {
    ControlMessageType type;
    uint16_t flags;
    uint32_t request_id;      // For request/response correlation
    uint32_t payload_length;  // Length of payload following header
};

// Control message payloads

// Connection request
struct ConnectionRequest {
    std::string peer_address;  // Client's address
};

// Connection response
struct ConnectionResponse {
    std::string session_name;  // Server-assigned session name
    std::string peer_address;  // Confirmed peer address
    Attributes ud_attributes;  // LID, GID, QP numbers for UD data plane
    int status;                // 0 = success, < 0 = error
};

// Read request (replaces handleReadRequest RPC)
struct ReadRequest {
    std::string peer_name;
    std::string session_name;

    // Serialized JSON containing buffer arrays
    // Format: {"local": [...], "remote": [...]}
    std::string buffers_json;
};

// Read response
struct ReadResponse {
    int status;    // 0 = success, < 0 = error
    TaskID task_id;  // Task ID for tracking the transfer
};

// Notification message (replaces handleNotification RPC)
struct NotificationMessage {
    std::string peer_name;
    TaskID task_id;
    std::string message;
};

// Buffer info response (replaces getBufferInfo RPC)
struct BufferInfoRequest {
    // Empty request - just triggers response
};

struct BufferInfoResponse {
    BufferInfo info;  // addr, length, rkey
};

// Error response
struct ErrorResponse {
    int error_code;
    std::string error_message;
};

// Message serializer/deserializer
class ControlMessageSerializer {
public:
    // Serialize control message to buffer
    static std::vector<uint8_t> serialize(
        ControlMessageType type,
        uint32_t request_id,
        const void* payload_struct,
        size_t payload_size);

    // Deserialize control message from buffer
    static bool deserialize(
        const uint8_t* buffer,
        size_t buffer_size,
        ControlMessageHeader& header,
        std::vector<uint8_t>& payload);

    // Helper functions for specific message types

    // Connection request/response
    static std::vector<uint8_t> serializeConnectionRequest(
        uint32_t request_id,
        const ConnectionRequest& req);

    static bool deserializeConnectionRequest(
        const std::vector<uint8_t>& payload,
        ConnectionRequest& req);

    static std::vector<uint8_t> serializeConnectionResponse(
        uint32_t request_id,
        const ConnectionResponse& resp);

    static bool deserializeConnectionResponse(
        const std::vector<uint8_t>& payload,
        ConnectionResponse& resp);

    // Read request/response
    static std::vector<uint8_t> serializeReadRequest(
        uint32_t request_id,
        const ReadRequest& req);

    static bool deserializeReadRequest(
        const std::vector<uint8_t>& payload,
        ReadRequest& req);

    static std::vector<uint8_t> serializeReadResponse(
        uint32_t request_id,
        const ReadResponse& resp);

    static bool deserializeReadResponse(
        const std::vector<uint8_t>& payload,
        ReadResponse& resp);

    // Notification
    static std::vector<uint8_t> serializeNotification(
        uint32_t request_id,
        const NotificationMessage& msg);

    static bool deserializeNotification(
        const std::vector<uint8_t>& payload,
        NotificationMessage& msg);

    // Buffer info
    static std::vector<uint8_t> serializeBufferInfoRequest(
        uint32_t request_id);

    static std::vector<uint8_t> serializeBufferInfoResponse(
        uint32_t request_id,
        const BufferInfoResponse& resp);

    static bool deserializeBufferInfoRequest(
        const std::vector<uint8_t>& payload);

    static bool deserializeBufferInfoResponse(
        const std::vector<uint8_t>& payload,
        BufferInfoResponse& resp);

    // Error response
    static std::vector<uint8_t> serializeErrorResponse(
        uint32_t request_id,
        const ErrorResponse& err);

    static bool deserializeErrorResponse(
        const std::vector<uint8_t>& payload,
        ErrorResponse& err);

private:
    // Helper to serialize string
    static void serializeString(const std::string& str, std::vector<uint8_t>& buffer);

    // Helper to deserialize string
    static bool deserializeString(const uint8_t*& buffer, size_t& remaining, std::string& str);

    // Helper to serialize Attributes (from session_manager)
    static void serializeAttributes(const Attributes& attr, std::vector<uint8_t>& buffer);

    // Helper to deserialize Attributes
    static bool deserializeAttributes(const uint8_t*& buffer, size_t& remaining,
                                      Attributes& attr);
};

} // namespace ud
} // namespace rapid

#endif  // UD_CONTROL_PROTOCOL_H_
