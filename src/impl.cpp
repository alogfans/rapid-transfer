// impl.cpp
//
// RapidTransfer v2 Implementation - Minimal Framework
//
// Copyright (C) 2026 RapidXfer Team

#include "impl.h"

#include <glog/logging.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

namespace rapid {
namespace v1 {

// ============================================================================
// Initialization
// ============================================================================

int RapidTransfer::Impl::initialize(const RailConfig& rail_config,
                                    const std::string& listen_address) {
    rail_config_ = rail_config;
    listen_address_ = listen_address;
    start_time_ = std::chrono::steady_clock::now();

    // Construct UD Context
    int ret = ud_context_.construct(
        rail_config.device_name, rail_config.rdma_port, rail_config.gid_index);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to construct UD context";
        rail_state_ = RailState::FAILED;
        return ret;
    }

    rail_state_ = RailState::ACTIVE;

    LOG(INFO) << "[RapidTransfer] Initialized with device: " << rail_config.device_name;
    return 0;
}

int RapidTransfer::Impl::shutdown() {
    int ret = ud_context_.deconstruct();
    rail_state_ = RailState::DISABLED;
    running_ = false;

    if (progress_thread_.joinable()) {
        progress_thread_.join();
    }

    LOG(INFO) << "[RapidTransfer] Shutdown complete";
    return ret;
}

int RapidTransfer::Impl::runStep() {
    return ud_context_.runStep();
}

// ============================================================================
// Connection Management
// ============================================================================

int RapidTransfer::Impl::prepareConnection(const std::string& peer_name,
                                          Attributes& local_attrs) {
    return ud_context_.prepareConnection(peer_name, local_attrs);
}

int RapidTransfer::Impl::setupConnection(const std::string& peer_name,
                                        const Attributes& peer_attrs) {
    return ud_context_.setupConnection(peer_name, peer_attrs);
}

int RapidTransfer::Impl::ensureConnection(const std::string& peer_name, bool for_write) {
    // Check if already connected
    {
        std::lock_guard<std::mutex> lock(connection_mutex_);
        if (connected_peers_.find(peer_name) != connected_peers_.end()) {
            return 0;  // Already connected
        }
    }

    // Parse peer address (format: "host:port")
    size_t colon_pos = peer_name.find_last_of(':');
    if (colon_pos == std::string::npos) {
        LOG(ERROR) << "[RapidTransfer] Invalid peer address format: " << peer_name;
        return -1;
    }

    std::string peer_host = peer_name.substr(0, colon_pos);
    std::string peer_port_str = peer_name.substr(colon_pos + 1);
    int peer_port = std::stoi(peer_port_str);

    // Get local UD info
    Attributes local_attrs;
    int ret = prepareConnection(peer_name, local_attrs);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to prepare local UD attributes";
        return ret;
    }

    // Create TCP socket for bootstrap
    int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_sock < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to create TCP socket";
        return -1;
    }

    // Set socket timeout
    struct timeval tv;
    tv.tv_sec = 5;  // 5 second timeout
    tv.tv_usec = 0;
    setsockopt(tcp_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(tcp_sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // Connect to peer
    struct sockaddr_in peer_addr;
    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(peer_port);

    if (inet_pton(AF_INET, peer_host.c_str(), &peer_addr.sin_addr) <= 0) {
        LOG(ERROR) << "[RapidTransfer] Invalid peer address: " << peer_host;
        close(tcp_sock);
        return -1;
    }

    if (connect(tcp_sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
        LOG(ERROR) << "[RapidTransfer] TCP connection failed to " << peer_name;
        close(tcp_sock);
        return -1;
    }

    LOG(INFO) << "[RapidTransfer] TCP bootstrap connected to " << peer_name;

    // Send local UD info (simplified - just send a marker for now)
    // In production, you would exchange LID, GID, QP numbers, etc.
    uint32_t local_marker = 0x12345678;
    send(tcp_sock, &local_marker, sizeof(local_marker), 0);

    // Receive peer's UD info
    uint32_t peer_marker;
    recv(tcp_sock, &peer_marker, sizeof(peer_marker), MSG_WAITALL);

    close(tcp_sock);

    // Setup UD connection using exchanged info
    Attributes peer_attrs;
    // For now, use placeholder attributes (in production, exchange real UD info)
    peer_attrs["lid"] = std::to_string(peer_marker & 0xFFFF);
    peer_attrs["qp_num"] = std::to_string((peer_marker >> 16) & 0xFFFF);
    peer_attrs["gid"] = "fe80::0000:0000:0000:0000";  // Placeholder GID

    ret = setupConnection(peer_name, peer_attrs);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to setup UD connection";
        return ret;
    }

    // Mark as connected
    {
        std::lock_guard<std::mutex> lock(connection_mutex_);
        connected_peers_[peer_name] = true;
    }

    LOG(INFO) << "[RapidTransfer] Connection established to " << peer_name;
    return 0;
}

// ============================================================================
// Write/Read Operations
// ============================================================================

TaskID RapidTransfer::Impl::write(const std::string& peer_name,
                                     const std::vector<Buffer>& local_buffers,
                                     const std::vector<Buffer>& remote_buffers,
                                     const std::string& notify_message) {

    LOG(INFO) << "[RapidTransfer] write to " << peer_name
              << ", local_bufs=" << local_buffers.size()
              << ", remote_bufs=" << remote_buffers.size();

    int ret = ensureConnection(peer_name);
    if (ret != 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to establish connection";
        return -1;
    }

    // Start write operation via Context
    TaskID task_id = ud_context_.startWrite(peer_name, local_buffers, remote_buffers);

    if (task_id < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to start write";
        return -1;
    }

    // Initialize task result
    {
        std::lock_guard<std::mutex> lock(task_results_mutex_);

        // Calculate total bytes
        size_t total_bytes = 0;
        for (const auto& buf : local_buffers) {
            total_bytes += buf.length;
        }

        TransferResult result;
        result.task_id = std::to_string(task_id);
        result.status = Status::PENDING;
        result.total_bytes = total_bytes;
        result.device_name = rail_config_.device_name;
        task_results_[task_id] = result;
    }

    // Send notification if requested
    if (!notify_message.empty()) {
        notify(peer_name, task_id, notify_message);
    }

    return task_id;
}

TaskID RapidTransfer::Impl::read(const std::string& peer_name,
                                    const std::vector<Buffer>& local_buffers,
                                    const std::vector<Buffer>& remote_buffers,
                                    const std::string& notify_message) {

    LOG(INFO) << "[RapidTransfer] read from " << peer_name
              << ", local_bufs=" << local_buffers.size()
              << ", remote_bufs=" << remote_buffers.size();

    int ret = ensureConnection(peer_name);
    if (ret != 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to establish connection";
        return -1;
    }

    // Start read operation via Context (sends Read Request packet)
    TaskID task_id = ud_context_.startRead(peer_name, local_buffers, remote_buffers);

    if (task_id < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to start read";
        return -1;
    }

    // Initialize task result
    {
        std::lock_guard<std::mutex> lock(task_results_mutex_);

        // Calculate total bytes
        size_t total_bytes = 0;
        for (const auto& buf : local_buffers) {
            total_bytes += buf.length;
        }

        TransferResult result;
        result.task_id = std::to_string(task_id);
        result.status = Status::PENDING;
        result.total_bytes = total_bytes;
        result.device_name = rail_config_.device_name;
        task_results_[task_id] = result;
    }

    // Send notification if requested
    if (!notify_message.empty()) {
        notify(peer_name, task_id, notify_message);
    }

    return task_id;
}

// ============================================================================
// Status and Operations
// ============================================================================

Status RapidTransfer::Impl::getStatus(TaskID task_id, size_t* transferred_bytes) {
    std::lock_guard<std::mutex> lock(task_results_mutex_);

    auto it = task_results_.find(task_id);
    if (it == task_results_.end()) {
        return Status::UNKNOWN;
    }

    if (transferred_bytes) {
        *transferred_bytes = it->second.total_bytes;
    }

    return it->second.status;
}

Status RapidTransfer::Impl::wait(TaskID task_id, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(task_results_mutex_);

    // Check if task exists
    auto it = task_results_.find(task_id);
    if (it == task_results_.end()) {
        return Status::UNKNOWN;
    }

    // Wait for completion or timeout
    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (it->second.status == Status::PENDING) {
        if (task_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            // Timeout occurred
            if (it->second.status == Status::PENDING) {
                LOG(WARNING) << "[RapidTransfer] Task " << task_id << " timed out";
                return Status::PENDING;  // Still pending after timeout
            }
            break;
        }
    }

    return it->second.status;
}

std::optional<TransferResult> RapidTransfer::Impl::getTransferResult(TaskID task_id) {
    std::lock_guard<std::mutex> lock(task_results_mutex_);

    auto it = task_results_.find(task_id);
    if (it == task_results_.end()) {
        return std::nullopt;
    }

    return it->second;
}

int RapidTransfer::Impl::freeTask(TaskID task_id) {
    std::lock_guard<std::mutex> lock(task_results_mutex_);
    task_results_.erase(task_id);
    return 0;
}

// ============================================================================
// Notifications
// ============================================================================

void RapidTransfer::Impl::setNotificationCallback(NotificationCallback callback) {
    user_notification_callback_ = std::move(callback);
}

int RapidTransfer::Impl::notify(const std::string& peer_name, TaskID task_id,
                               const std::string& message) {

    LOG(INFO) << "[RapidTransfer] notify " << peer_name
              << ", task_id=" << task_id
              << ", message=" << message;

    // Store notification for tracking
    {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        PendingNotification pending;
        pending.peer_name = peer_name;
        pending.message = message;
        pending.sent = false;
        pending_notifications_[task_id] = pending;
    }

    // Send notification packet via Context
    int ret = ud_context_.sendNotification(peer_name, task_id, message);

    // Mark as sent if successful
    if (ret == 0) {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        auto it = pending_notifications_.find(task_id);
        if (it != pending_notifications_.end()) {
            it->second.sent = true;
        }
    }

    return ret;
}

// ============================================================================
// Rail Information
// ============================================================================

RailStats RapidTransfer::Impl::getRailStats() const {
    RailStats stats;
    stats.device_name = rail_config_.device_name;
    stats.state = rail_state_;
    stats.bytes_transferred = bytes_sent_.load();
    stats.transfer_count = send_count_.load();

    return stats;
}

// ============================================================================
// Memory Registration
// ============================================================================

int RapidTransfer::Impl::registerLocalMemory(void* addr, size_t length) {
    return ud_context_.registerLocalMemory(addr, length);
}

int RapidTransfer::Impl::unregisterLocalMemory(void* addr) {
    return ud_context_.unregisterLocalMemory(addr);
}

// ============================================================================
// Buffer Info Exchange
// ============================================================================

void RapidTransfer::Impl::setBufferInfo(const RapidTransfer::BufferInfo& info) {
    std::lock_guard<std::mutex> lock(buffer_info_mutex_);
    local_buffer_info_ = info;
    LOG(INFO) << "[RapidTransfer] Local buffer info set: addr=0x"
              << std::hex << info.addr << std::dec
              << ", size=" << info.length;
}

std::optional<RapidTransfer::BufferInfo> RapidTransfer::Impl::getRemoteBufferInfo(const std::string& peer_address) {
    // Request buffer info from remote peer via TCP
    // Parse peer address
    size_t colon_pos = peer_address.find_last_of(':');
    if (colon_pos == std::string::npos) {
        LOG(ERROR) << "[RapidTransfer] Invalid peer address format: " << peer_address;
        return std::nullopt;
    }

    std::string peer_host = peer_address.substr(0, colon_pos);
    std::string peer_port_str = peer_address.substr(colon_pos + 1);
    int peer_port = std::stoi(peer_port_str);

    // Create TCP socket
    int tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_sock < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to create TCP socket for buffer info";
        return std::nullopt;
    }

    // Set socket timeout
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(tcp_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(tcp_sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // Connect to peer
    struct sockaddr_in peer_addr;
    memset(&peer_addr, 0, sizeof(peer_addr));
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(peer_port);

    if (inet_pton(AF_INET, peer_host.c_str(), &peer_addr.sin_addr) <= 0) {
        LOG(ERROR) << "[RapidTransfer] Invalid peer address: " << peer_host;
        close(tcp_sock);
        return std::nullopt;
    }

    if (connect(tcp_sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
        LOG(ERROR) << "[RapidTransfer] TCP connection failed to " << peer_address;
        close(tcp_sock);
        return std::nullopt;
    }

    // Send buffer info request (simple marker)
    uint32_t request = 0xBEEFBEEF;
    send(tcp_sock, &request, sizeof(request), 0);

    // Receive buffer info
    RapidTransfer::BufferInfo info;
    ssize_t recv_len = recv(tcp_sock, &info, sizeof(info), MSG_WAITALL);
    close(tcp_sock);

    if (recv_len != sizeof(info)) {
        LOG(ERROR) << "[RapidTransfer] Failed to receive buffer info from " << peer_address;
        return std::nullopt;
    }

    LOG(INFO) << "[RapidTransfer] Received buffer info from " << peer_address
              << ": addr=0x" << std::hex << info.addr << std::dec
              << ", size=" << info.length;

    return info;
}

// ============================================================================
// Internal Implementation
// ============================================================================

int RapidTransfer::Impl::makeConnectionIfNeeded(const std::string& peer_name) {
    return ensureConnection(peer_name);
}

void RapidTransfer::Impl::startProgressThread() {
    running_ = true;
    progress_thread_ = std::thread([this]() {
        while (running_) {
            runStep();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
}

void RapidTransfer::Impl::stopProgressThread() {
    running_ = false;
    if (progress_thread_.joinable()) {
        progress_thread_.join();
    }
}

} // namespace v1
} // namespace rapid
