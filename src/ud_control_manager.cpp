// ud_control_manager.cpp
//
// UD-based control plane manager implementation
//
// Copyright (C) 2024 Feng Ren

#include "ud_control_manager.h"

#include <arpa/inet.h>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <glog/logging.h>
#include <json/json.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

namespace rapid {

UDControlManager::UDControlManager(Context& ud_context)
    : ud_context_(ud_context) {
    // Set control packet handler in Context
    ud_context_.setControlPacketHandler(
        [this](const std::string& peer_name, const uint8_t* data, size_t length) {
            return handleControlPacket(peer_name, data, length);
        });
}

UDControlManager::~UDControlManager() {
    shutdownListener();
}

// ============================================================================
// Deduplication Helpers
// ============================================================================

namespace {
    std::string computeRequestHash(const rapid::ud::ReadRequest& req) {
        // Hash based on peer_name, session_name, and buffers_json
        std::string combined = req.peer_name + ":" + req.session_name + ":" + req.buffers_json;
        return std::to_string(std::hash<std::string>{}(combined));
    }

    std::string computeRequestHash(const rapid::ud::NotificationMessage& msg) {
        // Hash based on peer_name, task_id, and message
        std::string combined = msg.peer_name + ":" + std::to_string(msg.task_id) + ":" + msg.message;
        return std::to_string(std::hash<std::string>{}(combined));
    }
}

void UDControlManager::cleanupExpiredDedupEntries() {
    auto now = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(dedup_mutex_);
    for (auto it = dedup_cache_.begin(); it != dedup_cache_.end();) {
        if (it->second.expiry_time < now) {
            it = dedup_cache_.erase(it);
        } else {
            ++it;
        }
    }
}

// ============================================================================
// TCP Bootstrap Listener
// ============================================================================

int UDControlManager::startListener(const std::string& tcp_address,
                                    const OnAcceptCallback& on_accept,
                                    const OnErrorCallback& on_error) {
    on_accept_ = on_accept;
    on_error_ = on_error;

    return startBootstrapListener(tcp_address);
}

int UDControlManager::shutdownListener() {
    return stopBootstrapListener();
}

int UDControlManager::startBootstrapListener(const std::string& tcp_address) {
    // Parse address (format: "host:port")
    size_t colon_pos = tcp_address.find(':');
    if (colon_pos == std::string::npos) {
        LOG(ERROR) << "[UDControlManager] Invalid address format: " << tcp_address;
        return -1;
    }

    std::string host = tcp_address.substr(0, colon_pos);
    std::string port_str = tcp_address.substr(colon_pos + 1);

    // Resolve host address
    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;  // IPv4
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    int ret = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
    if (ret != 0) {
        LOG(ERROR) << "[UDControlManager] getaddrinfo failed: " << gai_strerror(ret);
        return -1;
    }

    // Create socket
    tcp_listen_fd_ = socket(result->ai_family, result->ai_socktype, IPPROTO_TCP);
    if (tcp_listen_fd_ < 0) {
        PLOG(ERROR) << "[UDControlManager] socket creation failed";
        freeaddrinfo(result);
        return -1;
    }

    // Set socket options
    int opt = 1;
    if (setsockopt(tcp_listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        PLOG(ERROR) << "[UDControlManager] setsockopt(SO_REUSEADDR) failed";
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
        freeaddrinfo(result);
        return -1;
    }

    // Bind
    if (bind(tcp_listen_fd_, result->ai_addr, result->ai_addrlen) < 0) {
        PLOG(ERROR) << "[UDControlManager] bind failed for " << tcp_address;
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
        freeaddrinfo(result);
        return -1;
    }

    freeaddrinfo(result);

    // Listen
    if (listen(tcp_listen_fd_, 10) < 0) {
        PLOG(ERROR) << "[UDControlManager] listen failed";
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
        return -1;
    }

    tcp_listen_address_ = tcp_address;
    tcp_listener_running_.store(true);

    // Start accept thread
    tcp_accept_thread_ = std::thread(&UDControlManager::bootstrapAcceptThread, this);

    LOG(INFO) << "[UDControlManager] TCP bootstrap listener started on " << tcp_address;
    return 0;
}

int UDControlManager::stopBootstrapListener() {
    tcp_listener_running_.store(false);

    if (tcp_listen_fd_ >= 0) {
        shutdown(tcp_listen_fd_, SHUT_RDWR);
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
    }

    if (tcp_accept_thread_.joinable()) {
        tcp_accept_thread_.join();
    }

    LOG(INFO) << "[UDControlManager] TCP bootstrap listener stopped";
    return 0;
}

void UDControlManager::bootstrapAcceptThread() {
    while (tcp_listener_running_.load()) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(tcp_listen_fd_,
                              (struct sockaddr*)&client_addr,
                              &client_len);
        if (client_fd < 0) {
            if (tcp_listener_running_.load()) {
                PLOG(WARNING) << "[UDControlManager] accept failed";
            }
            continue;
        }

        // Handle connection in thread pool or directly
        handleBootstrapConnection(client_fd);
    }
}

void UDControlManager::handleBootstrapConnection(int client_fd) {
    LOG(INFO) << "[UDControlManager] New bootstrap connection";

    // Receive peer's UD connection info (JSON format)
    char buffer[4096];
    ssize_t n = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
    if (n <= 0) {
        LOG(ERROR) << "[UDControlManager] Failed to receive bootstrap data";
        close(client_fd);
        return;
    }
    buffer[n] = '\0';

    // Parse received JSON
    Json::Value peer_root;
    Json::Reader reader;
    if (!reader.parse(buffer, peer_root)) {
        LOG(ERROR) << "[UDControlManager] Failed to parse peer attributes JSON: " << buffer;
        close(client_fd);
        return;
    }

    std::string peer_key = peer_root["peer_key"].asString();

    Attributes peer_attrs;
    peer_attrs["lid"] = peer_root["lid"].asString();
    peer_attrs["gid"] = peer_root["gid"].asString();
    peer_attrs["qp"] = peer_root.get("qp", "").asString();
    peer_attrs["ext_qp"] = peer_root.get("ext_qp", "").asString();

    LOG(INFO) << "[UDControlManager] Bootstrap from: " << peer_key
              << ", LID=" << peer_attrs["lid"]
              << ", GID=" << peer_attrs["gid"]
              << ", QP=" << peer_attrs["qp"];

    // Prepare our local attributes
    std::string session_name = "server/" + std::to_string(uid_.fetch_add(1));

    Attributes local_attrs;
    int ret = ud_context_.prepareConnection(peer_key, local_attrs);
    if (ret) {
        LOG(ERROR) << "[UDControlManager] Failed to prepare local attributes";
        close(client_fd);
        return;
    }

    // Setup bidirectional connection
    ret = ud_context_.setupConnection(peer_key, peer_attrs);
    if (ret) {
        LOG(ERROR) << "[UDControlManager] Failed to setup connection with peer";
        close(client_fd);
        return;
    }

    // Store mapping
    {
        RWSpinlock::WriteGuard guard(sessions_lock_);
        sessions_.insert(session_name);
        peer_to_session_map_[peer_key] = session_name;
        session_to_peer_map_[session_name] = peer_key;

        // Store GID:QP to peer address mapping for reverse lookup of incoming UD packets
        // Trim leading/trailing whitespace from QP value
        std::string qp_value = peer_attrs["qp"];
        size_t start = qp_value.find_first_not_of(" \t");
        size_t end = qp_value.find_last_not_of(" \t");
        if (start != std::string::npos) {
            qp_value = qp_value.substr(start, end - start + 1);
        }
        std::string gid_qp_key = peer_attrs["gid"] + ":" + qp_value;
        gid_qp_to_peer_map_[gid_qp_key] = peer_key;
        LOG(INFO) << "[UDControlManager] Stored GID:QP mapping: " << gid_qp_key << " -> " << peer_key;
    }

    // Debug logging: show local_attrs before sending
    LOG(INFO) << "[UDControlManager] Local attrs for response: lid=" << local_attrs["lid"]
              << ", gid=" << local_attrs["gid"]
              << ", qp=" << local_attrs["qp"]
              << ", ext_qp=" << local_attrs["ext_qp"];

    // Send response with our attributes and session name
    Json::Value response_root;
    response_root["lid"] = local_attrs["lid"];
    response_root["gid"] = local_attrs["gid"];
    response_root["qp"] = local_attrs["qp"];
    response_root["ext_qp"] = local_attrs["ext_qp"];
    response_root["session_name"] = session_name;

    Json::StreamWriterBuilder writer;
    std::string response_str = Json::writeString(writer, response_root);

    if (send(client_fd, response_str.c_str(), response_str.size(), 0) < 0) {
        PLOG(ERROR) << "[UDControlManager] Failed to send bootstrap response";
    }

    close(client_fd);

    LOG(INFO) << "[UDControlManager] Bootstrap connection established for " << peer_key;
}

// ============================================================================
// Connection Management
// ============================================================================

int UDControlManager::connect(const std::string& address,
                              const Attributes& request,
                              Attributes& response) {
    // Parse address
    size_t colon_pos = address.find(':');
    if (colon_pos == std::string::npos) {
        LOG(ERROR) << "[UDControlManager] Invalid address format: " << address;
        return -1;
    }

    std::string host = address.substr(0, colon_pos);
    std::string port_str = address.substr(colon_pos + 1);

    // Resolve and connect via TCP bootstrap
    struct addrinfo hints, *result;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int ret = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
    if (ret != 0) {
        LOG(ERROR) << "[UDControlManager] getaddrinfo failed: " << gai_strerror(ret);
        return -1;
    }

    int sock_fd = socket(result->ai_family, result->ai_socktype, IPPROTO_TCP);
    if (sock_fd < 0) {
        PLOG(ERROR) << "[UDControlManager] socket creation failed";
        freeaddrinfo(result);
        return -1;
    }

    if (::connect(sock_fd, result->ai_addr, result->ai_addrlen) < 0) {
        PLOG(ERROR) << "[UDControlManager] connect failed to " << address;
        close(sock_fd);
        freeaddrinfo(result);
        return -1;
    }

    freeaddrinfo(result);

    // Prepare local attributes first
    Attributes local_attrs;
    ret = ud_context_.prepareConnection(address, local_attrs);
    if (ret) {
        LOG(ERROR) << "[UDControlManager] Failed to prepare local attributes";
        close(sock_fd);
        return -1;
    }

    // Debug logging: show local_attrs after prepareConnection
    LOG(INFO) << "[UDControlManager] Client local attrs: lid=" << local_attrs["lid"]
              << ", gid=" << local_attrs["gid"]
              << ", qp=" << local_attrs["qp"]
              << ", ext_qp=" << local_attrs["ext_qp"];

    // Send our UD connection info as JSON
    Json::Value request_root;
    request_root["peer_key"] = address;
    request_root["lid"] = local_attrs["lid"];
    request_root["gid"] = local_attrs["gid"];
    request_root["qp"] = local_attrs["qp"];
    request_root["ext_qp"] = local_attrs["ext_qp"];

    Json::StreamWriterBuilder writer;
    std::string request_str = Json::writeString(writer, request_root);

    if (send(sock_fd, request_str.c_str(), request_str.size(), 0) < 0) {
        PLOG(ERROR) << "[UDControlManager] Failed to send bootstrap request";
        close(sock_fd);
        return -1;
    }

    // Receive peer's UD connection info (JSON response)
    char buffer[4096];
    ssize_t n = recv(sock_fd, buffer, sizeof(buffer) - 1, 0);
    if (n <= 0) {
        LOG(ERROR) << "[UDControlManager] Failed to receive bootstrap response";
        close(sock_fd);
        return -1;
    }
    buffer[n] = '\0';

    close(sock_fd);

    // Parse JSON response
    Json::Value response_root;
    Json::Reader reader;
    if (!reader.parse(buffer, response_root)) {
        LOG(ERROR) << "[UDControlManager] Failed to parse response JSON: " << buffer;
        return -1;
    }

    // Extract server attributes
    Attributes peer_attrs;
    peer_attrs["lid"] = response_root["lid"].asString();
    peer_attrs["gid"] = response_root["gid"].asString();
    peer_attrs["qp"] = response_root.get("qp", "").asString();
    peer_attrs["ext_qp"] = response_root.get("ext_qp", "").asString();

    std::string session_name = response_root["session_name"].asString();

    LOG(INFO) << "[UDControlManager] Received: LID=" << peer_attrs["lid"]
              << ", GID=" << peer_attrs["gid"]
              << ", QP=" << peer_attrs["qp"]
              << ", session=" << session_name;

    // Setup bidirectional connection
    ret = ud_context_.setupConnection(address, peer_attrs);
    if (ret) {
        LOG(ERROR) << "[UDControlManager] Failed to setup connection";
        return -1;
    }

    // Copy peer attributes to output
    response = peer_attrs;

    // Store mapping
    {
        RWSpinlock::WriteGuard guard(sessions_lock_);
        sessions_.insert(session_name);
        peer_to_session_map_[address] = session_name;
        session_to_peer_map_[session_name] = address;

        // Store GID:QP to peer address mapping for reverse lookup of incoming UD packets
        // Trim leading/trailing whitespace from QP value
        std::string qp_value = peer_attrs["qp"];
        size_t start = qp_value.find_first_not_of(" \t");
        size_t end = qp_value.find_last_not_of(" \t");
        if (start != std::string::npos) {
            qp_value = qp_value.substr(start, end - start + 1);
        }
        std::string gid_qp_key = peer_attrs["gid"] + ":" + qp_value;
        gid_qp_to_peer_map_[gid_qp_key] = address;
        LOG(INFO) << "[UDControlManager] Stored GID:QP mapping: " << gid_qp_key << " -> " << address;
    }

    LOG(INFO) << "[UDControlManager] Connected to " << address
              << ", session: " << session_name;

    return 0;
}

int UDControlManager::disconnect(const std::string& address) {
    RWSpinlock::WriteGuard guard(sessions_lock_);

    auto it = peer_to_session_map_.find(address);
    if (it != peer_to_session_map_.end()) {
        std::string session_name = it->second;
        sessions_.erase(session_name);
        session_to_peer_map_.erase(session_name);
        peer_to_session_map_.erase(it);
        LOG(INFO) << "[UDControlManager] Disconnected from " << address;
        return 0;
    }

    return -1;
}

bool UDControlManager::hasConnection(const std::string& address) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    return peer_to_session_map_.find(address) != peer_to_session_map_.end();
}

std::string UDControlManager::getSessionName(const std::string& peer_address) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    auto it = peer_to_session_map_.find(peer_address);
    if (it != peer_to_session_map_.end()) {
        return it->second;
    }
    return "";
}

// ============================================================================
// Callback Registration
// ============================================================================

void UDControlManager::setReadCallback(const OnReadRequestCallback& on_read) {
    on_read_request_ = on_read;
}

void UDControlManager::setNotificationCallback(const OnNotificationCallback& on_notification) {
    on_notification_ = on_notification;
}

// ============================================================================
// Buffer Info Management
// ============================================================================

void UDControlManager::setBufferInfo(const BufferInfo& info) {
    local_buffer_info_ = info;
    buffer_info_available_.store(true);
    LOG(INFO) << "[UDControlManager] Buffer info set: addr=0x" << std::hex
              << info.addr << std::dec << ", length=" << info.length;
}

// ============================================================================
// Request/Response Correlation
// ============================================================================

uint32_t UDControlManager::allocateRequestId() {
    return next_request_id_.fetch_add(1);
}

int UDControlManager::registerPendingRequest(
    uint32_t request_id,
    std::chrono::milliseconds timeout,
    const std::function<void(const uint8_t*, size_t)>& callback) {

    std::lock_guard<std::mutex> lock(pending_requests_mutex_);

    auto pending = std::make_unique<PendingRequest>();
    pending->deadline = std::chrono::steady_clock::now() + timeout;
    pending->callback = callback;
    pending->cv = new std::condition_variable();
    pending->completed = false;

    pending_requests_[request_id] = std::move(pending);

    return 0;
}

int UDControlManager::waitForResult(uint32_t request_id, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(pending_requests_mutex_);

    auto it = pending_requests_.find(request_id);
    if (it == pending_requests_.end()) {
        return -1;  // Request not found
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;

    if (it->second->cv->wait_until(lock, deadline) == std::cv_status::timeout) {
        delete it->second->cv;
        pending_requests_.erase(it);
        return -1;  // Timeout
    }

    bool completed = it->second->completed;
    delete it->second->cv;
    pending_requests_.erase(it);

    return completed ? 0 : -1;
}

int UDControlManager::completeRequest(uint32_t request_id, const uint8_t* data, size_t length) {
    std::lock_guard<std::mutex> lock(pending_requests_mutex_);

    auto it = pending_requests_.find(request_id);
    if (it == pending_requests_.end()) {
        return -1;  // Request not found
    }

    it->second->callback(data, length);
    it->second->completed = true;
    it->second->cv->notify_one();

    return 0;
}

// ============================================================================
// Control Message Sending
// ============================================================================

int UDControlManager::sendControlMessage(const std::string& peer_name,
                                        const std::vector<uint8_t>& message) {
    // Send via UD Context
    int ret = ud_context_.sendControl(peer_name, message);
    if (ret < 0) {
        LOG(ERROR) << "[UDControlManager] Failed to send control message to " << peer_name;
        return -1;
    }

    LOG(INFO) << "[UDControlManager] Sent control message to " << peer_name
              << ", size: " << message.size();

    return 0;
}

// ============================================================================
// Control Message Handlers
// ============================================================================

int UDControlManager::handleControlPacket(const std::string& peer_name,
                                         const uint8_t* data,
                                         size_t length) {
    using namespace ud;

    // Deserialize header
    ControlMessageHeader header;
    std::vector<uint8_t> payload;

    if (!ControlMessageSerializer::deserialize(data, length, header, payload)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize control message from " << peer_name;
        return -1;
    }

    LOG(INFO) << "[UDControlManager] Received control message from " << peer_name
              << ", type: " << static_cast<int>(header.type)
              << ", request_id: " << header.request_id;

    // Translate GID:QP peer name to original peer address for response routing
    std::string original_peer_name = peer_name;
    {
        RWSpinlock::ReadGuard guard(sessions_lock_);
        auto it = gid_qp_to_peer_map_.find(peer_name);
        if (it != gid_qp_to_peer_map_.end()) {
            original_peer_name = it->second;
            LOG(INFO) << "[UDControlManager] Mapped peer " << peer_name << " to " << original_peer_name;
        } else {
            LOG(WARNING) << "[UDControlManager] No mapping found for peer " << peer_name;
        }
    }

    // Route to appropriate handler
    switch (header.type) {
        case ControlMessageType::CONNECTION_REQUEST:
            return handleConnectionRequest(original_peer_name, header.request_id, payload);

        case ControlMessageType::CONNECTION_RESPONSE:
            return handleConnectionResponse(original_peer_name, header.request_id, payload);

        case ControlMessageType::READ_REQUEST:
            return handleReadRequestMessage(original_peer_name, header.request_id, payload);

        case ControlMessageType::READ_RESPONSE:
            return handleReadResponseMessage(original_peer_name, header.request_id, payload);

        case ControlMessageType::NOTIFICATION:
            return handleNotificationMessage(original_peer_name, header.request_id, payload);

        case ControlMessageType::BUFFER_INFO_REQUEST:
            return handleBufferInfoRequest(original_peer_name, header.request_id);

        case ControlMessageType::BUFFER_INFO_RESPONSE:
            return handleBufferInfoResponse(original_peer_name, header.request_id, payload);

        case ControlMessageType::ERROR_RESPONSE:
            return handleErrorResponse(original_peer_name, header.request_id, payload);

        default:
            LOG(WARNING) << "[UDControlManager] Unknown control message type: "
                        << static_cast<int>(header.type);
            return -1;
    }
}

int UDControlManager::handleConnectionRequest(const std::string& peer_name,
                                              uint32_t request_id,
                                              const std::vector<uint8_t>& payload) {
    using namespace ud;

    ConnectionRequest req;
    if (!ControlMessageSerializer::deserializeConnectionRequest(payload, req)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize connection request";
        return -1;
    }

    LOG(INFO) << "[UDControlManager] Connection request from " << peer_name
              << ", address: " << req.peer_address;

    // Generate session name
    std::string session_name = "server/" + std::to_string(uid_.fetch_add(1));

    {
        RWSpinlock::WriteGuard guard(sessions_lock_);
        sessions_.insert(session_name);
        peer_to_session_map_[peer_name] = session_name;
        session_to_peer_map_[session_name] = peer_name;
    }

    // TODO: Call on_accept_ callback to get UD attributes
    // For now, send response with empty attributes

    ConnectionResponse resp;
    resp.session_name = session_name;
    resp.peer_address = req.peer_address;
    resp.status = 0;
    // resp.ud_attributes = ...;  // From on_accept_ callback

    auto response_msg = ControlMessageSerializer::serializeConnectionResponse(
        request_id, resp);

    return sendControlMessage(peer_name, response_msg);
}

int UDControlManager::handleConnectionResponse(const std::string& peer_name,
                                               uint32_t request_id,
                                               const std::vector<uint8_t>& payload) {
    using namespace ud;

    ConnectionResponse resp;
    if (!ControlMessageSerializer::deserializeConnectionResponse(payload, resp)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize connection response";
        return -1;
    }

    LOG(INFO) << "[UDControlManager] Connection response from " << peer_name
              << ", session: " << resp.session_name
              << ", status: " << resp.status;

    // Complete the pending connect request
    completeRequest(request_id, payload.data(), payload.size());

    return 0;
}

int UDControlManager::handleReadRequestMessage(const std::string& peer_name,
                                               uint32_t request_id,
                                               const std::vector<uint8_t>& payload) {
    using namespace ud;

    ReadRequest req;
    if (!ControlMessageSerializer::deserializeReadRequest(payload, req)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize read request";
        return -1;
    }

    // Compute hash for deduplication
    std::string request_hash = computeRequestHash(req);
    DedupKey dedup_key{peer_name, static_cast<uint16_t>(ud::ControlMessageType::READ_REQUEST), request_hash};

    // Check if this is a duplicate request
    {
        std::lock_guard<std::mutex> lock(dedup_mutex_);

        auto it = dedup_cache_.find(dedup_key);
        if (it != dedup_cache_.end()) {
            // Duplicate request! Return the same task_id
            LOG(INFO) << "[UDControlManager] Duplicate read request from " << peer_name
                      << ", returning existing task_id=" << it->second.task_id;

            // Update expiry time
            it->second.expiry_time = std::chrono::steady_clock::now() + kDedupTTL;

            // Send response with existing task_id
            ReadResponse resp;
            resp.status = 0;  // Success
            resp.task_id = it->second.task_id;

            auto response_msg = ControlMessageSerializer::serializeReadResponse(
                request_id, resp);

            return sendControlMessage(peer_name, response_msg);
        }

        // New request - will insert into cache after processing
    }

    LOG(INFO) << "[UDControlManager] Read request from " << peer_name
              << ", session: " << req.session_name;

    // Parse buffers JSON
    Json::Value json_root;
    Json::Reader reader;

    if (!reader.parse(req.buffers_json, json_root)) {
        LOG(ERROR) << "[UDControlManager] Failed to parse buffers JSON";
        return -1;
    }

    std::vector<Buffer> local_targets;
    std::vector<Buffer> data_sources;

    // Parse local arrays
    const Json::Value& local_array = json_root["local"];
    for (const auto& item : local_array) {
        Buffer buf;
        buf.addr = reinterpret_cast<void*>(std::stoull(item["addr"].asString()));
        buf.length = item["length"].asUInt64();
        local_targets.push_back(buf);
    }

    // Parse remote arrays
    const Json::Value& remote_array = json_root["remote"];
    for (const auto& item : remote_array) {
        Buffer buf;
        buf.addr = reinterpret_cast<void*>(std::stoull(item["addr"].asString()));
        buf.length = item["length"].asUInt64();
        data_sources.push_back(buf);
    }

    // Call registered callback
    if (on_read_request_) {
        TaskID task_id = on_read_request_(peer_name, local_targets, data_sources);

        // Insert into dedup cache
        {
            std::lock_guard<std::mutex> lock(dedup_mutex_);
            DedupEntry entry;
            entry.task_id = task_id;
            entry.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            entry.expiry_time = std::chrono::steady_clock::now() + kDedupTTL;

            dedup_cache_[dedup_key] = entry;
        }

        LOG(INFO) << "[UDControlManager] Created task_id=" << task_id
                  << " for read request from " << peer_name;

        // Send response
        ReadResponse resp;
        resp.status = (task_id >= 0) ? 0 : -1;
        resp.task_id = task_id;

        auto response_msg = ControlMessageSerializer::serializeReadResponse(
            request_id, resp);

        return sendControlMessage(peer_name, response_msg);
    }

    return -1;
}

int UDControlManager::handleReadResponseMessage(const std::string& peer_name,
                                                uint32_t request_id,
                                                const std::vector<uint8_t>& payload) {
    using namespace ud;

    ReadResponse resp;
    if (!ControlMessageSerializer::deserializeReadResponse(payload, resp)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize read response";
        return -1;
    }

    LOG(INFO) << "[UDControlManager] Read response from " << peer_name
              << ", status: " << resp.status
              << ", task_id: " << resp.task_id;

    // Complete pending request
    completeRequest(request_id, payload.data(), payload.size());

    return 0;
}

int UDControlManager::handleNotificationMessage(const std::string& peer_name,
                                                uint32_t request_id,
                                                const std::vector<uint8_t>& payload) {
    using namespace ud;

    NotificationMessage msg;
    if (!ControlMessageSerializer::deserializeNotification(payload, msg)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize notification";
        return -1;
    }

    // Compute hash for deduplication
    std::string request_hash = computeRequestHash(msg);
    DedupKey dedup_key{peer_name, static_cast<uint16_t>(ud::ControlMessageType::NOTIFICATION), request_hash};

    // Check if this is a duplicate notification (at-most-once semantics)
    {
        std::lock_guard<std::mutex> lock(dedup_mutex_);

        auto it = dedup_cache_.find(dedup_key);
        if (it != dedup_cache_.end()) {
            // Duplicate notification! Ignore it.
            LOG(INFO) << "[UDControlManager] Duplicate notification from " << peer_name
                      << ", task_id=" << msg.task_id << " (ignoring)";

            // Update expiry time
            it->second.expiry_time = std::chrono::steady_clock::now() + kDedupTTL;
            return 0;  // Silently ignore duplicate
        }

        // New notification - insert into cache
        DedupEntry entry;
        entry.task_id = -1;  // Not applicable for notifications
        entry.timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        entry.expiry_time = std::chrono::steady_clock::now() + kDedupTTL;

        dedup_cache_[dedup_key] = entry;
    }

    LOG(INFO) << "[UDControlManager] Notification from " << peer_name
              << ", task_id: " << msg.task_id
              << ", message: " << msg.message;

    // Call registered callback
    if (on_notification_) {
        on_notification_(msg.peer_name, msg.task_id, msg.message);
    }

    return 0;
}

int UDControlManager::handleBufferInfoRequest(const std::string& peer_name,
                                              uint32_t request_id) {
    LOG(INFO) << "[UDControlManager] Buffer info request from " << peer_name;

    if (!buffer_info_available_.load()) {
        // Send error response
        ud::ErrorResponse err;
        err.error_code = -1;
        err.error_message = "Buffer info not available";

        auto response_msg = ud::ControlMessageSerializer::serializeErrorResponse(
            request_id, err);

        return sendControlMessage(peer_name, response_msg);
    }

    // Send buffer info
    ud::BufferInfoResponse resp;
    resp.info = local_buffer_info_;

    auto response_msg = ud::ControlMessageSerializer::serializeBufferInfoResponse(
        request_id, resp);

    return sendControlMessage(peer_name, response_msg);
}

int UDControlManager::handleBufferInfoResponse(const std::string& peer_name,
                                               uint32_t request_id,
                                               const std::vector<uint8_t>& payload) {
    using namespace ud;

    BufferInfoResponse resp;
    if (!ControlMessageSerializer::deserializeBufferInfoResponse(payload, resp)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize buffer info response";
        return -1;
    }

    LOG(INFO) << "[UDControlManager] Buffer info from " << peer_name
              << ", addr=0x" << std::hex << resp.info.addr << std::dec
              << ", length=" << resp.info.length;

    // Complete pending request
    completeRequest(request_id, payload.data(), payload.size());

    return 0;
}

int UDControlManager::handleErrorResponse(const std::string& peer_name,
                                         uint32_t request_id,
                                         const std::vector<uint8_t>& payload) {
    using namespace ud;

    ErrorResponse err;
    if (!ControlMessageSerializer::deserializeErrorResponse(payload, err)) {
        LOG(ERROR) << "[UDControlManager] Failed to deserialize error response";
        return -1;
    }

    LOG(ERROR) << "[UDControlManager] Error response from " << peer_name
              << ", code: " << err.error_code
              << ", message: " << err.error_message;

    // Complete pending request with error
    completeRequest(request_id, payload.data(), payload.size());

    return 0;
}

// ============================================================================
// Public API Methods
// ============================================================================

int UDControlManager::sendReadRequest(const std::string& peer_name,
                                     const std::string& session_name,
                                     const std::string& buffers_json,
                                     int& task_id) {
    using namespace ud;

    // Retry loop for idempotent RPC
    // Note: Client should ensure read requests are idempotent or handle duplicate task_ids
    for (int attempt = 0; attempt < kMaxRetries; attempt++) {
        uint32_t request_id = allocateRequestId();

        ReadRequest req;
        req.peer_name = peer_name;
        req.session_name = session_name;
        req.buffers_json = buffers_json;

        auto message = ControlMessageSerializer::serializeReadRequest(request_id, req);

        // Register pending request with shorter timeout for retry
        std::promise<int> result_promise;
        auto result_future = result_promise.get_future();

        registerPendingRequest(request_id, kRetryTimeout,
            [&](const uint8_t* data, size_t length) {
                ReadResponse resp;
                if (ControlMessageSerializer::deserializeReadResponse(
                        std::vector<uint8_t>(data, data + length), resp)) {
                    task_id = resp.task_id;
                    result_promise.set_value(resp.status);
                } else {
                    result_promise.set_value(-1);
                }
            });

        // Send message
        int ret = sendControlMessage(peer_name, message);
        if (ret < 0) {
            if (attempt < kMaxRetries - 1) {
                LOG(WARNING) << "[UDControlManager] Read request send failed, attempt "
                          << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
                continue;
            }
            return -1;
        }

        // Wait for response
        if (result_future.wait_for(kRetryTimeout) != std::future_status::timeout) {
            // Success
            int status = result_future.get();
            if (status >= 0) {
                LOG(INFO) << "[UDControlManager] Read request succeeded on attempt "
                          << (attempt + 1) << "/" << kMaxRetries << ", task_id=" << task_id;
                return status;
            }
            // Error response from peer
            if (attempt < kMaxRetries - 1) {
                LOG(WARNING) << "[UDControlManager] Read request returned error, attempt "
                          << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
                continue;
            }
            return -1;
        }

        // Timeout
        if (attempt < kMaxRetries - 1) {
            LOG(WARNING) << "[UDControlManager] Read request timed out, attempt "
                      << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
        } else {
            LOG(ERROR) << "[UDControlManager] Read request failed after " << kMaxRetries << " attempts";
        }
    }

    return -1;
}

int UDControlManager::sendNotification(const std::string& peer_name, int task_id,
                                       const std::string& message) {
    using namespace ud;

    uint32_t request_id = allocateRequestId();

    NotificationMessage msg;
    msg.peer_name = peer_name;
    msg.task_id = task_id;
    msg.message = message;

    auto msg_buffer = ControlMessageSerializer::serializeNotification(request_id, msg);

    // Retry sending notification (fire-and-forget, but try multiple times)
    for (int attempt = 0; attempt < kMaxRetries; attempt++) {
        int ret = sendControlMessage(peer_name, msg_buffer);
        if (ret >= 0) {
            if (attempt > 0) {
                LOG(INFO) << "[UDControlManager] Notification sent on attempt "
                          << (attempt + 1) << "/" << kMaxRetries;
            }
            return 0;
        }

        if (attempt < kMaxRetries - 1) {
            LOG(WARNING) << "[UDControlManager] Failed to send notification, attempt "
                      << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
        }
    }

    LOG(ERROR) << "[UDControlManager] Failed to send notification after " << kMaxRetries << " attempts";
    return -1;
}

std::optional<BufferInfo> UDControlManager::getBufferInfo(const std::string& peer_address) {
    using namespace ud;

    // Retry loop for idempotent RPC
    for (int attempt = 0; attempt < kMaxRetries; attempt++) {
        uint32_t request_id = allocateRequestId();
        auto message = ControlMessageSerializer::serializeBufferInfoRequest(request_id);

        // Register pending request with shorter timeout for retry
        std::promise<BufferInfo> result_promise;
        auto result_future = result_promise.get_future();

        registerPendingRequest(request_id, kRetryTimeout,
            [&](const uint8_t* data, size_t length) {
                BufferInfoResponse resp;
                if (ControlMessageSerializer::deserializeBufferInfoResponse(
                        std::vector<uint8_t>(data, data + length), resp)) {
                    result_promise.set_value(resp.info);
                } else {
                    // Set error value
                    result_promise.set_value(BufferInfo{0, 0, 0});
                }
            });

        // Send message
        int ret = sendControlMessage(peer_address, message);
        if (ret < 0) {
            if (attempt < kMaxRetries - 1) {
                LOG(WARNING) << "[UDControlManager] Buffer info request send failed, attempt "
                          << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
                continue;
            }
            return std::nullopt;
        }

        // Wait for response
        if (result_future.wait_for(kRetryTimeout) != std::future_status::timeout) {
            // Success
            BufferInfo info = result_future.get();
            if (info.length > 0) {
                LOG(INFO) << "[UDControlManager] Buffer info request succeeded on attempt "
                          << (attempt + 1) << "/" << kMaxRetries;
                return info;
            }
            // Invalid response
            if (attempt < kMaxRetries - 1) {
                LOG(WARNING) << "[UDControlManager] Buffer info invalid, attempt "
                          << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
                continue;
            }
            return std::nullopt;
        }

        // Timeout
        if (attempt < kMaxRetries - 1) {
            LOG(WARNING) << "[UDControlManager] Buffer info request timed out, attempt "
                      << (attempt + 1) << "/" << kMaxRetries << ", retrying...";
        } else {
            LOG(ERROR) << "[UDControlManager] Buffer info request failed after "
                      << kMaxRetries << " attempts";
        }
    }

    return std::nullopt;
}

// ============================================================================
// Progress Engine
// ============================================================================

int UDControlManager::runStep() {
    // Check for timed out requests
    auto now = std::chrono::steady_clock::now();

    std::lock_guard<std::mutex> lock(pending_requests_mutex_);
    for (auto it = pending_requests_.begin(); it != pending_requests_.end();) {
        if (now > it->second->deadline) {
            LOG(WARNING) << "[UDControlManager] Request " << it->first
                        << " timed out";

            // Complete with empty data (callback should handle timeout)
            it->second->callback(nullptr, 0);
            it->second->completed = true;
            it->second->cv->notify_one();

            delete it->second->cv;
            it = pending_requests_.erase(it);
        } else {
            ++it;
        }
    }

    // Clean up expired dedup cache entries (periodic cleanup)
    cleanupExpiredDedupEntries();

    return 0;
}

} // namespace rapid
