// impl.cpp
//
// RapidTransfer v2 Implementation - Minimal Framework
//
// Copyright (C) 2026 RapidXfer Team

#include "impl.h"

#include <glog/logging.h>

#include <cstring>
#include <sstream>

#include "tcp_bootstrap.h"

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

    // Set transfer complete callback before constructing context
    ud_context_.setTransferCompleteCallback(
        [this](TaskID task_id, const std::string& peer_name, Status status) {
            onTransferComplete(task_id, peer_name, status);
        });

    // Construct UD Context
    int ret = ud_context_.construct(
        rail_config.device_name, rail_config.rdma_port, rail_config.gid_index);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to construct UD context";
        rail_state_ = RailState::FAILED;
        return ret;
    }

    rail_state_ = RailState::ACTIVE;

    LOG(INFO) << "[RapidTransfer] Initialized with device: "
              << rail_config.device_name;
    return 0;
}

int RapidTransfer::Impl::shutdown() {
    // Stop TCP Bootstrap listener first
    stopBootstrapListener();

    int ret = ud_context_.deconstruct();
    rail_state_ = RailState::DISABLED;
    running_ = false;

    if (progress_thread_.joinable()) {
        progress_thread_.join();
    }

    LOG(INFO) << "[RapidTransfer] Shutdown complete";
    return ret;
}

int RapidTransfer::Impl::runStep() { return ud_context_.runStep(); }

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

int RapidTransfer::Impl::ensureConnection(const std::string& peer_name,
                                          bool for_write) {
    // Check if already connected
    {
        std::lock_guard<std::mutex> lock(connection_mutex_);
        if (connected_peers_.find(peer_name) != connected_peers_.end()) {
            return 0;  // Already connected
        }
    }

    // Get local UD info
    Attributes local_attrs;
    int ret = prepareConnection(peer_name, local_attrs);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to prepare local UD attributes";
        return ret;
    }

    // Prepare local UDInfo (using TcpBootstrap types)
    TcpBootstrap::UDInfo local_info;
    local_info.lid = std::stoi(local_attrs.at("lid"));

    // Convert GID string to bytes
    std::string gid_str = local_attrs.at("gid");
    memset(local_info.gid, 0, sizeof(local_info.gid));
    // Parse GID string format "xx:xx:..." to bytes
    std::istringstream gid_iss(gid_str);
    for (int i = 0; i < 16; ++i) {
        int value;
        gid_iss >> std::hex >> value;
        local_info.gid[i] = static_cast<uint8_t>(value);
        if (i < 15) {
            char colon;
            gid_iss >> colon;
        }
    }

    // Parse all QP numbers
    std::string qp_str = local_attrs.at("qp");
    std::istringstream qp_iss(qp_str);
    std::vector<uint32_t> qp_nums;
    uint32_t qp_num;
    while (qp_iss >> qp_num) {
        qp_nums.push_back(qp_num);
    }

    if (qp_nums.empty()) {
        LOG(ERROR) << "[RapidTransfer] No QP numbers available";
        return -1;
    }

    // Assign QP numbers directly to the vector
    local_info.qp_nums = std::move(qp_nums);

    LOG(INFO) << "[RapidTransfer] Prepared " << local_info.qp_nums.size()
              << " QP numbers";

    // Exchange UD info via TCP bootstrap
    TcpBootstrap::UDInfo peer_info;
    ret = TcpBootstrap::exchangeUDInfo(peer_name, local_info, peer_info);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] UD info exchange failed";
        return ret;
    }

    // Convert peer GID bytes to string
    char peer_gid_str[64];
    for (int i = 0; i < 16; ++i) {
        sprintf(&peer_gid_str[i * 2], "%02x", peer_info.gid[i]);
    }

    // Convert peer QP numbers to string format
    std::stringstream peer_qp_ss;
    for (size_t i = 0; i < peer_info.qp_nums.size(); ++i) {
        if (i > 0) peer_qp_ss << " ";
        peer_qp_ss << peer_info.qp_nums[i];
    }

    // Setup UD connection using exchanged info
    Attributes peer_attrs;
    peer_attrs["lid"] = std::to_string(peer_info.lid);
    peer_attrs["gid"] = std::string(peer_gid_str, 32);
    peer_attrs["qp"] = peer_qp_ss.str();

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
    TaskID task_id =
        ud_context_.startWrite(peer_name, local_buffers, remote_buffers);

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

    // Store pending notification to be sent when transfer completes
    if (!notify_message.empty()) {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        PendingNotification pending;
        pending.peer_name = peer_name;
        pending.message = notify_message;
        pending.sent = false;
        pending_notifications_[task_id] = pending;
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
    TaskID task_id =
        ud_context_.startRead(peer_name, local_buffers, remote_buffers);

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

    // Store pending notification to be sent when transfer completes
    if (!notify_message.empty()) {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        PendingNotification pending;
        pending.peer_name = peer_name;
        pending.message = notify_message;
        pending.sent = false;
        pending_notifications_[task_id] = pending;
    }

    return task_id;
}

// ============================================================================
// Status and Operations
// ============================================================================

Status RapidTransfer::Impl::getStatus(TaskID task_id,
                                      size_t* transferred_bytes) {
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

Status RapidTransfer::Impl::wait(TaskID task_id,
                                 std::chrono::milliseconds timeout) {
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
                LOG(WARNING)
                    << "[RapidTransfer] Task " << task_id << " timed out";
                return Status::PENDING;  // Still pending after timeout
            }
            break;
        }
    }

    return it->second.status;
}

std::optional<TransferResult> RapidTransfer::Impl::getTransferResult(
    TaskID task_id) {
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

void RapidTransfer::Impl::setNotificationCallback(
    NotificationCallback callback) {
    user_notification_callback_ = std::move(callback);
}

int RapidTransfer::Impl::notify(const std::string& peer_name, TaskID task_id,
                                const std::string& message) {
    LOG(INFO) << "[RapidTransfer] notify " << peer_name
              << ", task_id=" << task_id << ", message=" << message;

    // Send notification packet immediately (user-initiated, not delayed)
    return ud_context_.sendNotification(peer_name, task_id, message);
}

void RapidTransfer::Impl::onTransferComplete(TaskID task_id,
                                             const std::string& peer_name,
                                             Status status) {
    LOG(INFO) << "[RapidTransfer] Transfer complete: task_id=" << task_id
              << ", peer=" << peer_name
              << ", status=" << static_cast<int>(status);

    // Update task result status
    {
        std::lock_guard<std::mutex> lock(task_results_mutex_);
        auto it = task_results_.find(task_id);
        if (it != task_results_.end()) {
            it->second.status = status;
        }
        task_cv_.notify_all();
    }

    // Send pending notification if any
    {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        auto it = pending_notifications_.find(task_id);
        if (it != pending_notifications_.end() && !it->second.sent) {
            // Send notification now that transfer is complete
            const auto& pending = it->second;
            int ret = ud_context_.sendNotification(pending.peer_name, task_id,
                                                   pending.message);
            if (ret == 0) {
                it->second.sent = true;
                LOG(INFO)
                    << "[RapidTransfer] Sent delayed notification for task "
                    << task_id;
            } else {
                LOG(ERROR) << "[RapidTransfer] Failed to send delayed "
                              "notification for task "
                           << task_id;
            }
        }
    }
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
    // Store in impl layer for compatibility
    {
        std::lock_guard<std::mutex> lock(buffer_info_mutex_);
        local_buffer_info_ = info;
    }

    // Sync to Context layer for TCP bootstrap server
    rapid::Context::BufferInfo ctx_info;
    ctx_info.addr = info.addr;
    ctx_info.length = info.length;
    ctx_info.rkey = info.rkey;
    ud_context_.setBufferInfo(ctx_info);

    LOG(INFO) << "[RapidTransfer] Local buffer info set: addr=0x" << std::hex
              << info.addr << std::dec << ", size=" << info.length;
}

std::optional<RapidTransfer::BufferInfo>
RapidTransfer::Impl::getRemoteBufferInfo(const std::string& peer_address) {
    // Use TcpBootstrap to request buffer info
    auto tcp_info = TcpBootstrap::getBufferInfo(peer_address);
    if (!tcp_info) {
        return std::nullopt;
    }

    // Convert TcpBootstrap::BufferInfo to RapidTransfer::BufferInfo
    RapidTransfer::BufferInfo info;
    info.addr = tcp_info->addr;
    info.length = tcp_info->length;
    info.rkey = tcp_info->rkey;

    LOG(INFO) << "[RapidTransfer] Received buffer info from " << peer_address
              << ": addr=0x" << std::hex << info.addr << std::dec
              << ", size=" << info.length;

    return info;
}

// ============================================================================
// TCP Bootstrap Server
// ============================================================================

int RapidTransfer::Impl::startBootstrapListener(const std::string& tcp_address) {
    std::lock_guard<std::mutex> lock(tcp_bootstrap_mutex_);

    if (tcp_bootstrap_) {
        LOG(WARNING) << "[RapidTransfer] TCP Bootstrap listener already running";
        return -1;
    }

    tcp_bootstrap_ = std::make_unique<TcpBootstrap>();

    // Set UD connect callback
    tcp_bootstrap_->setUDConnectCallback([this](const std::string& peer_addr,
                                                       TcpBootstrap::UDInfo& local_info,
                                                       const TcpBootstrap::UDInfo& peer_info) {
        // Convert peer UD info to connection attributes
        char peer_gid_str[64];
        for (int i = 0; i < 16; ++i) {
            sprintf(&peer_gid_str[i * 2], "%02x", peer_info.gid[i]);
        }

        // Convert QP numbers to string format
        std::stringstream qp_ss;
        for (size_t i = 0; i < peer_info.qp_nums.size(); ++i) {
            if (i > 0) qp_ss << " ";
            qp_ss << peer_info.qp_nums[i];
        }

        Attributes peer_attrs;
        peer_attrs["lid"] = std::to_string(peer_info.lid);
        peer_attrs["gid"] = std::string(peer_gid_str, 32);
        peer_attrs["qp"] = qp_ss.str();

        // Create peer address string (use first QP)
        std::string peer_name = std::string(peer_gid_str) + ":" +
                                std::to_string(peer_info.qp_nums[0]);

        // Setup UD connection
        int ret = ud_context_.setupConnection(peer_name, peer_attrs);
        if (ret != 0) {
            LOG(ERROR) << "[RapidTransfer] Failed to setup UD connection for " << peer_name;
            return false;
        }

        // Fill local UD info using accessor methods
        uint16_t local_lid = ud_context_.getLid();
        std::string local_gid_str = ud_context_.getGid();

        local_info.lid = local_lid;
        memset(local_info.gid, 0, sizeof(local_info.gid));

        // Parse GID string format "xx:xx:..." to bytes
        std::istringstream gid_iss(local_gid_str);
        for (int i = 0; i < 16 && !gid_iss.eof(); ++i) {
            int value;
            gid_iss >> std::hex >> value;
            local_info.gid[i] = static_cast<uint8_t>(value);
            if (!gid_iss.eof()) {
                char colon;
                gid_iss >> colon;
            }
        }

        // Get local QP numbers from endpoint store using accessor method
        auto& endpoint_store = ud_context_.endpointStore();
        auto qp_nums = endpoint_store.qpNum();
        local_info.qp_nums = qp_nums;

        LOG(INFO) << "[RapidTransfer] Accepted UD connection from " << peer_addr;
        return true;
    });

    // Set buffer info callback
    tcp_bootstrap_->setBufferInfoCallback([this]() -> std::optional<TcpBootstrap::BufferInfo> {
        std::lock_guard<std::mutex> lock(buffer_info_mutex_);
        if (local_buffer_info_) {
            TcpBootstrap::BufferInfo info;
            info.addr = local_buffer_info_->addr;
            info.length = local_buffer_info_->length;
            info.rkey = local_buffer_info_->rkey;
            return info;
        }
        return std::nullopt;
    });

    // Start listener
    int ret = tcp_bootstrap_->startListener(tcp_address);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to start TCP Bootstrap listener";
        tcp_bootstrap_.reset();
        return ret;
    }

    LOG(INFO) << "[RapidTransfer] TCP Bootstrap listener started on " << tcp_address;
    return 0;
}

void RapidTransfer::Impl::stopBootstrapListener() {
    std::lock_guard<std::mutex> lock(tcp_bootstrap_mutex_);

    if (tcp_bootstrap_) {
        tcp_bootstrap_->stopListener();
        tcp_bootstrap_.reset();
        LOG(INFO) << "[RapidTransfer] TCP Bootstrap listener stopped";
    }
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

}  // namespace v1
}  // namespace rapid
