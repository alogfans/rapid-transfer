// ud_control_protocol.cpp
//
// Control message protocol implementation
//
// Copyright (C) 2024 Feng Ren

#include "ud_control_protocol.h"

#include <cstring>
#include <glog/logging.h>

namespace rapid {
namespace ud {

std::vector<uint8_t> ControlMessageSerializer::serialize(
    ControlMessageType type,
    uint32_t request_id,
    const void* payload_struct,
    size_t payload_size) {

    // Calculate total size: header + payload
    size_t total_size = sizeof(ControlMessageHeader) + payload_size;
    std::vector<uint8_t> buffer(total_size);

    // Write header
    ControlMessageHeader* header = reinterpret_cast<ControlMessageHeader*>(buffer.data());
    header->type = type;
    header->flags = 0;
    header->request_id = request_id;
    header->payload_length = static_cast<uint32_t>(payload_size);

    // Copy payload
    if (payload_size > 0 && payload_struct != nullptr) {
        std::memcpy(buffer.data() + sizeof(ControlMessageHeader),
                   payload_struct, payload_size);
    }

    return buffer;
}

bool ControlMessageSerializer::deserialize(
    const uint8_t* buffer,
    size_t buffer_size,
    ControlMessageHeader& header,
    std::vector<uint8_t>& payload) {

    if (buffer_size < sizeof(ControlMessageHeader)) {
        LOG(ERROR) << "[ControlProtocol] Buffer too small for header";
        return false;
    }

    // Read header
    const ControlMessageHeader* hdr =
        reinterpret_cast<const ControlMessageHeader*>(buffer);
    header.type = hdr->type;
    header.flags = hdr->flags;
    header.request_id = hdr->request_id;
    header.payload_length = hdr->payload_length;

    // Validate payload length
    if (sizeof(ControlMessageHeader) + header.payload_length > buffer_size) {
        LOG(ERROR) << "[ControlProtocol] Invalid payload length: "
                   << header.payload_length << ", buffer size: " << buffer_size;
        return false;
    }

    // Extract payload
    if (header.payload_length > 0) {
        payload.assign(buffer + sizeof(ControlMessageHeader),
                      buffer + sizeof(ControlMessageHeader) + header.payload_length);
    } else {
        payload.clear();
    }

    return true;
}

void ControlMessageSerializer::serializeString(const std::string& str,
                                               std::vector<uint8_t>& buffer) {
    uint32_t len = static_cast<uint32_t>(str.size());
    const uint8_t* len_bytes = reinterpret_cast<const uint8_t*>(&len);
    buffer.insert(buffer.end(), len_bytes, len_bytes + sizeof(len));
    buffer.insert(buffer.end(), str.begin(), str.end());
}

bool ControlMessageSerializer::deserializeString(const uint8_t*& buffer,
                                                 size_t& remaining,
                                                 std::string& str) {
    if (remaining < sizeof(uint32_t)) {
        return false;
    }

    uint32_t len;
    std::memcpy(&len, buffer, sizeof(len));
    buffer += sizeof(len);
    remaining -= sizeof(len);

    if (remaining < len) {
        return false;
    }

    str.assign(reinterpret_cast<const char*>(buffer), len);
    buffer += len;
    remaining -= len;

    return true;
}

void ControlMessageSerializer::serializeAttributes(const Attributes& attr,
                                                   std::vector<uint8_t>& buffer) {
    // Serialize unordered_map as key-value pairs
    uint32_t size = static_cast<uint32_t>(attr.size());
    const uint8_t* size_bytes = reinterpret_cast<const uint8_t*>(&size);
    buffer.insert(buffer.end(), size_bytes, size_bytes + sizeof(size));

    for (const auto& kv : attr) {
        serializeString(kv.first, buffer);
        serializeString(kv.second, buffer);
    }
}

bool ControlMessageSerializer::deserializeAttributes(const uint8_t*& buffer,
                                                     size_t& remaining,
                                                     Attributes& attr) {
    if (remaining < sizeof(uint32_t)) {
        return false;
    }

    uint32_t size;
    std::memcpy(&size, buffer, sizeof(size));
    buffer += sizeof(size);
    remaining -= sizeof(size);

    for (uint32_t i = 0; i < size; ++i) {
        std::string key, value;
        if (!deserializeString(buffer, remaining, key)) return false;
        if (!deserializeString(buffer, remaining, value)) return false;
        attr[key] = value;
    }

    return true;
}

// Connection request/response
std::vector<uint8_t> ControlMessageSerializer::serializeConnectionRequest(
    uint32_t request_id,
    const ConnectionRequest& req) {
    std::vector<uint8_t> payload;
    serializeString(req.peer_address, payload);

    return serialize(ControlMessageType::CONNECTION_REQUEST, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeConnectionRequest(
    const std::vector<uint8_t>& payload,
    ConnectionRequest& req) {
    const uint8_t* buffer = payload.data();
    size_t remaining = payload.size();
    return deserializeString(buffer, remaining, req.peer_address);
}

std::vector<uint8_t> ControlMessageSerializer::serializeConnectionResponse(
    uint32_t request_id,
    const ConnectionResponse& resp) {
    std::vector<uint8_t> payload;
    serializeString(resp.session_name, payload);
    serializeString(resp.peer_address, payload);

    // Serialize status
    const uint8_t* status_bytes = reinterpret_cast<const uint8_t*>(&resp.status);
    payload.insert(payload.end(), status_bytes, status_bytes + sizeof(resp.status));

    // Serialize attributes
    serializeAttributes(resp.ud_attributes, payload);

    return serialize(ControlMessageType::CONNECTION_RESPONSE, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeConnectionResponse(
    const std::vector<uint8_t>& payload,
    ConnectionResponse& resp) {
    const uint8_t* buffer = payload.data();
    size_t remaining = payload.size();

    if (!deserializeString(buffer, remaining, resp.session_name)) return false;
    if (!deserializeString(buffer, remaining, resp.peer_address)) return false;

    if (remaining < sizeof(resp.status)) return false;
    std::memcpy(&resp.status, buffer, sizeof(resp.status));
    buffer += sizeof(resp.status);
    remaining -= sizeof(resp.status);

    if (!deserializeAttributes(buffer, remaining, resp.ud_attributes)) return false;

    return true;
}

// Read request/response
std::vector<uint8_t> ControlMessageSerializer::serializeReadRequest(
    uint32_t request_id,
    const ReadRequest& req) {
    std::vector<uint8_t> payload;
    serializeString(req.peer_name, payload);
    serializeString(req.session_name, payload);
    serializeString(req.buffers_json, payload);

    return serialize(ControlMessageType::READ_REQUEST, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeReadRequest(
    const std::vector<uint8_t>& payload,
    ReadRequest& req) {
    const uint8_t* buffer = payload.data();
    size_t remaining = payload.size();

    if (!deserializeString(buffer, remaining, req.peer_name)) return false;
    if (!deserializeString(buffer, remaining, req.session_name)) return false;
    if (!deserializeString(buffer, remaining, req.buffers_json)) return false;

    return true;
}

std::vector<uint8_t> ControlMessageSerializer::serializeReadResponse(
    uint32_t request_id,
    const ReadResponse& resp) {
    std::vector<uint8_t> payload;
    const uint8_t* status_bytes = reinterpret_cast<const uint8_t*>(&resp.status);
    payload.insert(payload.end(), status_bytes, status_bytes + sizeof(resp.status));

    const uint8_t* task_id_bytes = reinterpret_cast<const uint8_t*>(&resp.task_id);
    payload.insert(payload.end(), task_id_bytes, task_id_bytes + sizeof(resp.task_id));

    return serialize(ControlMessageType::READ_RESPONSE, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeReadResponse(
    const std::vector<uint8_t>& payload,
    ReadResponse& resp) {
    const uint8_t* buffer = payload.data();
    size_t remaining = payload.size();

    if (remaining < sizeof(resp.status) + sizeof(resp.task_id)) {
        return false;
    }

    std::memcpy(&resp.status, buffer, sizeof(resp.status));
    buffer += sizeof(resp.status);
    remaining -= sizeof(resp.status);

    std::memcpy(&resp.task_id, buffer, sizeof(resp.task_id));
    buffer += sizeof(resp.task_id);
    remaining -= sizeof(resp.task_id);

    return true;
}

// Notification
std::vector<uint8_t> ControlMessageSerializer::serializeNotification(
    uint32_t request_id,
    const NotificationMessage& msg) {
    std::vector<uint8_t> payload;
    serializeString(msg.peer_name, payload);

    const uint8_t* task_id_bytes = reinterpret_cast<const uint8_t*>(&msg.task_id);
    payload.insert(payload.end(), task_id_bytes, task_id_bytes + sizeof(msg.task_id));

    serializeString(msg.message, payload);

    return serialize(ControlMessageType::NOTIFICATION, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeNotification(
    const std::vector<uint8_t>& payload,
    NotificationMessage& msg) {
    const uint8_t* buffer = payload.data();
    size_t remaining = payload.size();

    if (!deserializeString(buffer, remaining, msg.peer_name)) return false;

    if (remaining < sizeof(msg.task_id)) return false;
    std::memcpy(&msg.task_id, buffer, sizeof(msg.task_id));
    buffer += sizeof(msg.task_id);
    remaining -= sizeof(msg.task_id);

    if (!deserializeString(buffer, remaining, msg.message)) return false;

    return true;
}

// Buffer info
std::vector<uint8_t> ControlMessageSerializer::serializeBufferInfoRequest(
    uint32_t request_id) {
    return serialize(ControlMessageType::BUFFER_INFO_REQUEST, request_id,
                    nullptr, 0);
}

bool ControlMessageSerializer::deserializeBufferInfoRequest(
    const std::vector<uint8_t>& payload) {
    // Empty payload
    return payload.empty();
}

std::vector<uint8_t> ControlMessageSerializer::serializeBufferInfoResponse(
    uint32_t request_id,
    const BufferInfoResponse& resp) {
    std::vector<uint8_t> payload;
    const uint8_t* info_bytes = reinterpret_cast<const uint8_t*>(&resp.info);
    payload.insert(payload.end(), info_bytes, info_bytes + sizeof(resp.info));

    return serialize(ControlMessageType::BUFFER_INFO_RESPONSE, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeBufferInfoResponse(
    const std::vector<uint8_t>& payload,
    BufferInfoResponse& resp) {
    if (payload.size() != sizeof(BufferInfo)) {
        return false;
    }

    std::memcpy(&resp.info, payload.data(), sizeof(resp.info));
    return true;
}

// Error response
std::vector<uint8_t> ControlMessageSerializer::serializeErrorResponse(
    uint32_t request_id,
    const ErrorResponse& err) {
    std::vector<uint8_t> payload;
    const uint8_t* code_bytes = reinterpret_cast<const uint8_t*>(&err.error_code);
    payload.insert(payload.end(), code_bytes, code_bytes + sizeof(err.error_code));

    serializeString(err.error_message, payload);

    return serialize(ControlMessageType::ERROR_RESPONSE, request_id,
                    payload.data(), payload.size());
}

bool ControlMessageSerializer::deserializeErrorResponse(
    const std::vector<uint8_t>& payload,
    ErrorResponse& err) {
    const uint8_t* buffer = payload.data();
    size_t remaining = payload.size();

    if (remaining < sizeof(err.error_code)) return false;
    std::memcpy(&err.error_code, buffer, sizeof(err.error_code));
    buffer += sizeof(err.error_code);
    remaining -= sizeof(err.error_code);

    if (!deserializeString(buffer, remaining, err.error_message)) return false;

    return true;
}

} // namespace ud
} // namespace rapid
