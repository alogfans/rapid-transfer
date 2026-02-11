// session_manager.cpp
// Copyright (C) 2024 Feng Ren

#include "session_manager.h"

#include <arpa/inet.h>
#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <json/json.h>
#include <poll.h>
#include <sys/socket.h>

#include <set>

namespace rapid {
using namespace coro_rpc;
using namespace async_simple::coro;

static inline bool isValidPort(int port) { return port >= 0 && port <= 65535; }

static inline int parseHostPort(const std::string& address,
                                std::string& hostname, uint16_t& port) {
    size_t pos = address.find(':');
    if (pos == std::string::npos) return -1;
    hostname = address.substr(0, pos);
    port = (uint16_t)std::stoi(address.substr(pos + 1));
    return 0;
}

SessionManager::~SessionManager() {
    shutdownListener();
    sessions_.clear();
}

std::string SessionManager::exchangeMetadata(std::string request_json) {
    std::cout << "[SessionManager] exchangeMetadata called, on_accept_="
              << (on_accept_ ? "set" : "null") << std::endl;
    std::cout.flush();

    Attributes request, response;

    // Extract peer_address from request if available
    // If client included its address in request, use it; otherwise generate
    // unique name
    std::string peer_address =
        request.count("peer_address") ? request["peer_address"] : "";

    std::string session_name;
    if (!peer_address.empty()) {
        // Use the peer's address as session name for consistency
        session_name = peer_address;
    } else {
        // Fallback: generate unique name (for backward compatibility)
        session_name = "server/" + std::to_string(uid_.fetch_add(1));
    }

    std::cout << "[SessionManager] Using session name: " << session_name
              << std::endl;
    std::cout.flush();

    if (readAttributes(request_json, request)) return "<error>";
    if (on_accept_(session_name, request, response)) return "<error>";

    // Include session_name in response so client can confirm the mapping
    response["session_name"] = session_name;

    std::string response_json;
    if (writeAttributes(response_json, response)) return "<error>";
    return response_json;
}

int SessionManager::startListener(const std::string& address,
                                  const OnAcceptCallback& on_accept,
                                  const OnErrorCallback& on_close) {
    on_accept_ = on_accept;
    std::string hostname;
    uint16_t port;
    if (parseHostPort(address, hostname, port)) {
        PLOG(ERROR) << "Illegal address format";
        return -1;
    }
    on_accept_ = on_accept;
    server_ = new coro_rpc::coro_rpc_server(1, port);
    server_->register_handler<&SessionManager::exchangeMetadata>(this);
    server_->register_handler<&SessionManager::handleWriteRequest>(this);
    server_->register_handler<&SessionManager::handleReadRequest>(this);
    server_->register_handler<&SessionManager::handleNotification>(this);
    server_->register_handler<&SessionManager::getBufferInfo>(this);
    server_->async_start();
    return 0;
}

int SessionManager::shutdownListener() {
    if (server_) {
        server_->stop();
        delete server_;
        server_ = nullptr;
    }
    return 0;
}

int SessionManager::connect(const std::string& address,
                            const Attributes& request, Attributes& response) {
    auto client_ptr = std::make_unique<coro_rpc_client>();
    auto conn_result =
        async_simple::coro::syncAwait(client_ptr->connect(address));
    if (conn_result.val() != 0) {
        LOG(ERROR) << "Failed to connect to master: " << conn_result.message();
        return -1;
    }

    // Create a mutable copy of request and add peer_address
    Attributes request_with_addr = request;
    request_with_addr["peer_address"] = address;

    std::string request_json;
    if (writeAttributes(request_json, request_with_addr)) return -1;
    auto request_result =
        client_ptr->send_request<&SessionManager::exchangeMetadata>(
            request_json);
    std::optional<std::string> result = async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<std::optional<std::string>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to handshake: " << result.error().msg;
                co_return "";
            }
            co_return result->result();
        }());

    if (!result) return -1;
    if (readAttributes(result.value(), response)) return -1;

    // Extract session_name from response
    std::string session_name;
    if (response.count("session_name")) {
        session_name = response["session_name"];
        std::cout << "[SessionManager::connect] Received session_name: "
                  << session_name << " for " << address << std::endl;
        std::cout.flush();
    } else {
        LOG(ERROR) << "Response does not contain session_name";
        return -1;
    }

    RWSpinlock::WriteGuard guard(sessions_lock_);
    sessions_.insert(address);
    // Save the mapping and client for reuse
    peer_to_session_map_[address] = session_name;
    peer_client_map_[address] = std::move(client_ptr);
    std::cout << "[SessionManager::connect] Saved mapping: " << address
              << " -> " << session_name << std::endl;
    std::cout.flush();

    return 0;
}

bool SessionManager::isMulticastAddress(const std::string& address) {
    if (address.find(":") != address.npos) return false;
    std::istringstream iss(address);
    std::string token;
    std::vector<int> bytes;
    while (std::getline(iss, token, '.')) {
        bytes.push_back(std::stoi(token));
    }
    if (bytes.size() != 4) {
        return false;
    }
    return bytes[0] >= 224 && bytes[0] <= 239;
}

int SessionManager::disconnect(const std::string& address) {
    RWSpinlock::WriteGuard guard(sessions_lock_);
    sessions_.erase(address);
    // Also remove mappings and close client
    peer_to_session_map_.erase(address);
    peer_client_map_.erase(address);
    return 0;
}

std::string SessionManager::getSessionName(const std::string& peer_address) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    auto it = peer_to_session_map_.find(peer_address);
    if (it != peer_to_session_map_.end()) {
        return it->second;
    }
    // If no mapping found, return the original address (might be for server
    // side)
    return peer_address;
}

coro_rpc::coro_rpc_client* SessionManager::getRPCClient(
    const std::string& peer_address) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    auto it = peer_client_map_.find(peer_address);
    if (it != peer_client_map_.end()) {
        return it->second.get();
    }
    return nullptr;
}

bool SessionManager::hasConnection(const std::string& address) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    return sessions_.count(address);
}

int SessionManager::readAttributes(const std::string& json_string,
                                   Attributes& attr) {
    Json::CharReaderBuilder reader;
    Json::Value json_object;
    std::string errs;
    if (json_string.empty()) return -2;  // Representing EOF
    std::istringstream iss(json_string);
    if (!Json::parseFromStream(reader, iss, &json_object, &errs)) {
        LOG(ERROR) << "Failed to parse: " << errs;
        return -1;
    }
    for (const auto& key : json_object.getMemberNames())
        attr[key] = json_object[key].asString();
    return 0;
}

int SessionManager::writeAttributes(std::string& json_string,
                                    const Attributes& attr) {
    Json::Value json_object;
    for (const auto& pair : attr) json_object[pair.first] = pair.second;
    Json::StreamWriterBuilder writer;
    json_string = Json::writeString(writer, json_object);
    return 0;
}

void SessionManager::setWriteReadCallbacks(
    const OnWriteRequestCallback& on_write,
    const OnReadRequestCallback& on_read) {
    on_write_request_ = on_write;
    on_read_request_ = on_read;
}

void SessionManager::setNotificationCallback(OnNotificationCallback callback) {
    on_notification_ = callback;
}

int SessionManager::handleNotification(const std::string& peer_name,
                                       int task_id,
                                       const std::string& message) {
    if (on_notification_) {
        on_notification_(peer_name, task_id, message);
        return 0;
    } else {
        LOG(WARNING) << "No notification callback registered, ignoring message";
        return 0;
    }
}

int SessionManager::handleWriteRequest(const std::string& peer_name,
                                       const std::string& session_name,
                                       const std::string& buffers_json) {
    if (!on_write_request_) {
        LOG(ERROR) << "No write request callback registered";
        return -1;
    }

    // Parse JSON array of Buffer objects
    Json::CharReaderBuilder reader;
    Json::Value json_array;
    std::string errs;
    std::istringstream iss(buffers_json);
    if (!Json::parseFromStream(reader, iss, &json_array, &errs)) {
        LOG(ERROR) << "Failed to parse buffers JSON: " << errs;
        return -1;
    }

    if (!json_array.isArray()) {
        LOG(ERROR) << "Expected JSON array for buffers";
        return -1;
    }

    // Convert JSON array to vector<Buffer>
    std::vector<Buffer> buffers;
    for (const auto& item : json_array) {
        Buffer buf;
        buf.addr = reinterpret_cast<void*>(std::stoull(item["addr"].asString()));
        buf.length = item["length"].asUInt64();
        buffers.push_back(buf);
    }
    int result = on_write_request_(session_name, buffers);
    return result;
}

int SessionManager::handleReadRequest(const std::string& peer_name,
                                      const std::string& session_name,
                                      const std::string& buffers_json) {
    if (!on_read_request_) {
        LOG(ERROR) << "No read request callback registered";
        return -1;
    }

    // Parse JSON array of Buffer objects
    Json::CharReaderBuilder reader;
    Json::Value json_array;
    std::string errs;
    std::istringstream iss(buffers_json);
    if (!Json::parseFromStream(reader, iss, &json_array, &errs)) {
        LOG(ERROR) << "Failed to parse buffers JSON: " << errs;
        return -1;
    }

    if (!json_array.isArray()) {
        LOG(ERROR) << "Expected JSON array for buffers";
        return -1;
    }

    // Convert JSON array to vector<Buffer>
    std::vector<Buffer> buffers;
    for (const auto& item : json_array) {
        Buffer buf;
        buf.addr = reinterpret_cast<void*>(std::stoull(item["addr"].asString()));
        buf.length = item["length"].asUInt64();
        buffers.push_back(buf);
    }

    // Pass session_name to callback instead of peer_name
    return on_read_request_(session_name, buffers);
}

// Buffer info management for e2e testing
void SessionManager::setBufferInfo(const BufferInfo& info) {
    local_buffer_info_ = info;
    buffer_info_available_ = true;
    std::cout << "[SessionManager] Buffer info set: addr=0x" << std::hex
              << info.addr << std::dec << ", length=" << info.length
              << ", rkey=" << info.rkey << std::endl;
}

BufferInfo SessionManager::getBufferInfo() {
    if (!buffer_info_available_) {
        LOG(WARNING) << "[SessionManager] Buffer info not available, returning zeros";
        return BufferInfo{0, 0, 0};
    }
    std::cout << "[SessionManager] Returning buffer info: addr=0x" << std::hex
              << local_buffer_info_.addr << std::dec
              << ", length=" << local_buffer_info_.length
              << ", rkey=" << local_buffer_info_.rkey << std::endl;
    return local_buffer_info_;
}
}  // namespace rapid
