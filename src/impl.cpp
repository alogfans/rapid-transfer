// transfer_engine_impl.cpp
//
// RapidTransfer v1 Implementation
// Direct use of existing RDMA UD Context
//
// Copyright (C) 2024 Feng Ren

#include "impl.h"

#include <glog/logging.h>
#include <json/json.h>

#include "ud_control_manager.h"

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

    // Construct UD Context first (UDControlManager needs it)
    int ret = ud_context_.construct(
        rail_config.device_name, rail_config.rdma_port, rail_config.gid_index);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to construct UD context";
        rail_state_ = RailState::FAILED;
        return ret;
    }

    // Create UD Control Manager (replaces Session Manager)
    ud_control_manager_ = new ::rapid::UDControlManager(ud_context_);

    rail_state_ = RailState::ACTIVE;

    // Start listener if listen address provided
    if (!listen_address.empty()) {
        // Set up accept callback - this prepares and sets up the RDMA
        // connection
        ::rapid::UDControlManager::OnAcceptCallback on_accept =
            [this](const std::string& peer_name, const Attributes& request,
                   Attributes& response) -> int {
            // Prepare local connection attributes
            int ret = ud_context_.prepareConnection(peer_name, response);
            if (ret) {
                LOG(ERROR) << "[RapidTransfer] Unable to setup endpoint: "
                              "get local attributes";
                return -1;
            }

            // Setup connection with peer's attributes
            ret = ud_context_.setupConnection(peer_name, request);
            if (ret) {
                LOG(ERROR) << "[RapidTransfer] Unable to setup endpoint: "
                              "set peer attributes";
                return -1;
            }

            // Get the session name assigned by the control manager
            std::string session_name =
                ud_control_manager_->getSessionName(peer_name);

            // Also register the session name with UD Context
            Attributes session_request, session_response;
            ret = ud_context_.prepareConnection(session_name, session_request);
            if (ret) {
                LOG(WARNING) << "[RapidTransfer] Unable to prepare "
                                "connection for session name";
                // Not fatal
            }

            ret = ud_context_.setupConnection(session_name, request);
            if (ret) {
                LOG(WARNING) << "[RapidTransfer] Unable to setup "
                                "connection for session name";
                // Not fatal
            }

            // Store the mapping
            peer_to_session_map_[peer_name] = session_name;

            return 0;  // Accept
        };

        // Set up error callback
        ::rapid::UDControlManager::OnErrorCallback on_error =
            [](const std::string& peer_name) {
                LOG(ERROR) << "[RapidTransfer] Connection error with: "
                           << peer_name;
            };

        // Set up read request callback - handles incoming read requests from
        // peers
        ::rapid::UDControlManager::OnReadRequestCallback on_read_request =
            [this](const std::string& peer_name,
                   const std::vector<Buffer>& local_targets,
                   const std::vector<Buffer>& data_sources) -> int {
            // Get session name for this peer
            std::string session_name = peer_name;
            auto it = peer_to_session_map_.find(peer_name);
            if (it != peer_to_session_map_.end()) {
                session_name = it->second;
            }

            // Send data from data_sources to local_targets (using direct write)
            TaskID task_id =
                ud_context_.send(session_name, data_sources, local_targets);
            if (task_id < 0) {
                LOG(ERROR) << "[RapidTransfer] Failed to send data for read "
                              "request, ret="
                           << task_id;
                return -1;
            }

            // Return task_id so reader can track progress
            return task_id;
        };

        // Register the read callback
        ud_control_manager_->setReadCallback(on_read_request);

        // Set up notification callback - handles incoming notifications from
        // peers
        ::rapid::UDControlManager::OnNotificationCallback on_notification =
            [this](const std::string& peer_name, int task_id,
                   const std::string& message) {
                std::lock_guard<std::mutex> lock(notification_mutex_);
                if (user_notification_callback_) {
                    user_notification_callback_(peer_name, task_id, message);
                }
            };
        ud_control_manager_->setNotificationCallback(on_notification);

        ret = ud_control_manager_->startListener(listen_address, on_accept,
                                              on_error);
        if (ret) {
            LOG(ERROR) << "[RapidTransfer] Failed to start listener";
            delete ud_control_manager_;
            ud_control_manager_ = nullptr;
            ud_context_.deconstruct();
            rail_state_ = RailState::FAILED;
            return ret;
        }
    }

    // Start progress thread
    startProgressThread();
    return 0;
}

int RapidTransfer::Impl::shutdown() {
    stopProgressThread();

    // Stop listener if running
    if (ud_control_manager_) {
        ud_control_manager_->shutdownListener();
    }

    ud_context_.deconstruct();

    // Cleanup UDControlManager
    if (ud_control_manager_) {
        delete ud_control_manager_;
        ud_control_manager_ = nullptr;
    }

    rail_state_ = RailState::DISABLED;

    {
        std::lock_guard<std::mutex> lock(task_results_mutex_);
        task_results_.clear();
    }

    task_cv_.notify_all();

    return 0;
}

int RapidTransfer::Impl::runStep() { return ud_context_.runStep(); }

// ============================================================================
// Connection Management (using UD Context)
// ============================================================================

int RapidTransfer::Impl::prepareConnection(const std::string& peer_name,
                                           Attributes& local_attrs) {
    return ud_context_.prepareConnection(peer_name, local_attrs);
}

int RapidTransfer::Impl::setupConnection(const std::string& peer_name,
                                         const Attributes& peer_attrs) {
    return ud_context_.setupConnection(peer_name, peer_attrs);
}

int RapidTransfer::Impl::makeConnectionIfNeeded(const std::string& peer_name) {
    if (!ud_control_manager_) {
        LOG(ERROR) << "[RapidTransfer] No UDControlManager available";
        return -1;
    }

    if (ud_control_manager_->hasConnection(peer_name)) {
        return 0;
    }

    // Prepare local connection attributes
    Attributes request, response;
    int ret = ud_context_.prepareConnection(peer_name, request);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to prepare connection attributes";
        return ret;
    }

    // Use UDControlManager to establish connection (TCP bootstrap + UD)
    ret = ud_control_manager_->connect(peer_name, request, response);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to establish connection "
                      "via UDControlManager";
        return ret;
    }

    // Setup connection with peer's attributes
    ret = ud_context_.setupConnection(peer_name, response);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to setup connection";
        ud_control_manager_->disconnect(peer_name);
        return ret;
    }

    std::string session_name = ud_control_manager_->getSessionName(peer_name);
    Attributes session_request, session_response;
    ret = ud_context_.prepareConnection(session_name, session_request);
    if (ret) {
        LOG(WARNING) << "[RapidTransfer] Unable to prepare connection "
                        "for session name";
        // Not fatal, can continue with peer_name
        return 0;
    }

    ret = ud_context_.setupConnection(session_name, response);
    if (ret) {
        LOG(WARNING) << "[RapidTransfer] Unable to setup connection for "
                        "session name";
        // Not fatal
        return 0;
    }

    // Store the mapping for later use
    peer_to_session_map_[peer_name] = session_name;
    return 0;
}

int RapidTransfer::Impl::ensureConnection(const std::string& peer_name,
                                          bool for_write) {
    // For read (receiving), we don't need to establish connection first
    // The sender will establish the connection
    if (!for_write) {
        return 0;
    }

    // For write (sending), establish the connection
    return makeConnectionIfNeeded(peer_name);
}

// ============================================================================
// Write/Read Operations (using UD send/receive)
// ============================================================================

TaskID RapidTransfer::Impl::write(const std::string& peer_name,
                                  const std::vector<Buffer>& local_buffers,
                                  const std::vector<Buffer>& remote_buffers,
                                  const std::string& notify_message) {
    if (local_buffers.empty()) {
        LOG(ERROR) << "[RapidTransfer] No buffers provided for write";
        return -1;
    }

    // 1. Ensure RDMA connection is established
    int ret = makeConnectionIfNeeded(peer_name);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to establish connection";
        return ret;
    }

    // 2. Get the session name for this peer
    std::string session_name = ud_control_manager_->getSessionName(peer_name);

    // 3. Send with remote write targets embedded in packet headers
    ret = ud_context_.send(session_name, local_buffers, remote_buffers);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] Send failed";
        return ret;
    }

    // 4. Register notification if provided (will be sent in getStatus())
    if (!notify_message.empty()) {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        pending_notifications_[ret] = {peer_name, notify_message, false};
        LOG(INFO) << "[RapidTransfer] Registered notification for task_id="
                  << ret;
    }

    return ret;
}

TaskID RapidTransfer::Impl::read(const std::string& peer_name,
                                 const std::vector<Buffer>& local_buffers,
                                 const std::vector<Buffer>& remote_buffers,
                                 const std::string& notify_message) {
    if (local_buffers.empty()) {
        LOG(ERROR) << "[RapidTransfer] No buffers provided for read";
        return -1;
    }

    // 1. Ensure RDMA connection is established
    int ret = makeConnectionIfNeeded(peer_name);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to establish connection";
        return ret;
    }

    // 2. Get the session name for this peer
    std::string session_name = ud_control_manager_->getSessionName(peer_name);

    // 3. Serialize buffers to JSON
    // local_buffers = where to put received data (becomes remote_targets for
    // sender) remote_buffers = where to read data from (sender's local data)
    Json::Value json_root;
    Json::Value local_array(Json::arrayValue);
    for (const auto& buf : local_buffers) {
        Json::Value item;
        item["addr"] = std::to_string(reinterpret_cast<uintptr_t>(buf.addr));
        item["length"] = Json::Value::UInt64(buf.length);
        local_array.append(item);
    }
    Json::Value remote_array(Json::arrayValue);
    for (const auto& buf : remote_buffers) {
        Json::Value item;
        item["addr"] = std::to_string(reinterpret_cast<uintptr_t>(buf.addr));
        item["length"] = Json::Value::UInt64(buf.length);
        remote_array.append(item);
    }
    json_root["local"] = local_array;
    json_root["remote"] = remote_array;
    Json::StreamWriterBuilder writer;
    std::string buffers_json = Json::writeString(writer, json_root);

    // 4. UD control message to notify remote peer to send data to our buffers
    int task_id = 0;
    ret = ud_control_manager_->sendReadRequest(peer_name, session_name,
                                               buffers_json, task_id);
    if (ret < 0 || task_id < 0) {
        LOG(ERROR) << "[RapidTransfer] Remote peer failed to send data";
        return -1;
    }

    // 5. Register notification if provided (will be sent in getStatus())
    if (!notify_message.empty()) {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        pending_notifications_[task_id] = {peer_name, notify_message, false};
        LOG(INFO) << "[RapidTransfer] Registered notification for task_id="
                  << task_id;
    }

    return task_id;
}

// ============================================================================
// Status and Operations
// ============================================================================

Status RapidTransfer::Impl::getStatus(TaskID task_id,
                                      size_t* transferred_bytes) {
    Status status = ud_context_.getStatus(task_id, transferred_bytes);

    // Check if there's a pending notification for this task
    {
        std::lock_guard<std::mutex> lock(notifications_mutex_);
        auto it = pending_notifications_.find(task_id);
        if (it != pending_notifications_.end() && !it->second.sent) {
            // Check if status changed from PENDING to SUCCESS/FAILED
            if (status == Status::SUCCESS || status == Status::FAILED) {
                // Send notification
                notify(it->second.peer_name, task_id, it->second.message);
                it->second.sent = true;
                LOG(INFO) << "[RapidTransfer] Sent notification for task_id="
                          << task_id << ", status=" << status;
            }
        }
    }

    std::lock_guard<std::mutex> lock(task_results_mutex_);
    auto it = task_results_.find(task_id);
    if (it != task_results_.end()) {
        if (it->second.status == Status::PENDING && status != Status::PENDING) {
            it->second.status = status;
            auto end_time = std::chrono::steady_clock::now();
            auto duration_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time_)
                    .count();
            it->second.duration_ms = static_cast<double>(duration_ms);

            if (status == Status::SUCCESS) {
                if (transferred_bytes) {
                    bytes_sent_.fetch_add(*transferred_bytes);
                }
            }

            task_cv_.notify_all();
        }
    }

    // Always return the actual status from UD context
    return status;
}

Status RapidTransfer::Impl::wait(TaskID task_id,
                                 std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;

    std::unique_lock<std::mutex> lock(task_results_mutex_);
    auto it = task_results_.find(task_id);
    if (it == task_results_.end()) {
        return Status::UNKNOWN;
    }

    while (it->second.status == Status::PENDING) {
        // Check underlying status
        lock.unlock();
        Status status = ud_context_.getStatus(task_id, nullptr);
        lock.lock();

        if (status != Status::PENDING) {
            it->second.status = status;
            auto end_time = std::chrono::steady_clock::now();
            auto duration_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time_)
                    .count();
            it->second.duration_ms = static_cast<double>(duration_ms);

            if (status == Status::SUCCESS) {
                size_t transferred = 0;
                ud_context_.getStatus(task_id, &transferred);
                bytes_sent_.fetch_add(transferred);
            }

            break;
        }

        if (task_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            LOG(WARNING) << "[RapidTransfer] Wait timeout for task_id="
                         << task_id;
            return Status::PENDING;
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

    // Update status if still pending
    if (it->second.status == Status::PENDING) {
        Status status = ud_context_.getStatus(task_id, nullptr);
        if (status != Status::PENDING) {
            it->second.status = status;
            auto end_time = std::chrono::steady_clock::now();
            auto duration_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time_)
                    .count();
            it->second.duration_ms = static_cast<double>(duration_ms);

            if (status == Status::SUCCESS) {
                size_t transferred = 0;
                ud_context_.getStatus(task_id, &transferred);
                bytes_sent_.fetch_add(transferred);
            }
        }
    }

    return it->second;
}

int RapidTransfer::Impl::freeTask(TaskID task_id) {
    ud_context_.freeTask(task_id);

    std::lock_guard<std::mutex> lock(task_results_mutex_);
    task_results_.erase(task_id);
    return 0;
}

// ============================================================================
// Notifications
// ============================================================================

void RapidTransfer::Impl::setNotificationCallback(
    NotificationCallback callback) {
    std::lock_guard<std::mutex> lock(notification_mutex_);
    user_notification_callback_ = std::move(callback);
}

int RapidTransfer::Impl::notify(const std::string& peer_name, TaskID task_id,
                                const std::string& message) {
    // Use UDControlManager to send notification via UD control message
    int ret = ud_control_manager_->sendNotification(peer_name, task_id, message);
    if (ret < 0) {
        LOG(ERROR) << "[RapidTransfer] Failed to send notification to peer";
        return -1;
    }

    // Also trigger local callback if set
    {
        std::lock_guard<std::mutex> lock(notification_mutex_);
        if (user_notification_callback_) {
            user_notification_callback_(peer_name, task_id, message);
        }
    }

    return 0;
}

// ============================================================================
// Statistics
// ============================================================================

RailStats RapidTransfer::Impl::getRailStats() const {
    RailStats stats;
    stats.device_name = rail_config_.device_name;
    stats.state = rail_state_;
    stats.bytes_transferred = bytes_sent_.load() + bytes_recv_.load();
    stats.transfer_count = send_count_.load() + recv_count_.load();
    stats.avg_latency_us = 0.0;
    stats.current_bandwidth_gbps = 0.0;

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
    if (ud_control_manager_) {
        ud_control_manager_->setBufferInfo(
            ::rapid::BufferInfo{info.addr, info.length, info.rkey});
    } else {
        LOG(WARNING) << "[RapidTransfer] No UDControlManager available, cannot "
                        "set buffer info";
    }
}

std::optional<RapidTransfer::BufferInfo>
RapidTransfer::Impl::getRemoteBufferInfo(const std::string& peer_address) {
    if (!ud_control_manager_) {
        LOG(ERROR) << "[RapidTransfer] No UDControlManager available";
        return std::nullopt;
    }

    // Ensure connection exists
    int ret = makeConnectionIfNeeded(peer_address);
    if (ret) {
        LOG(ERROR) << "[RapidTransfer] Failed to connect to peer";
        return std::nullopt;
    }

    // Use UDControlManager to get buffer info via UD control message
    std::optional<::rapid::BufferInfo> result =
        ud_control_manager_->getBufferInfo(peer_address);

    if (!result) {
        return std::nullopt;
    }

    // Convert to public BufferInfo type
    return RapidTransfer::BufferInfo{result->addr, result->length,
                                     result->rkey};
}

// ============================================================================
// Internal Implementation
// ============================================================================

void RapidTransfer::Impl::startProgressThread() {
    if (running_.exchange(true)) {
        return;
    }

    progress_thread_ = std::thread([this]() {
        while (running_.load()) {
            runStep();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
}

void RapidTransfer::Impl::stopProgressThread() {
    if (!running_.exchange(false)) {
        return;
    }

    if (progress_thread_.joinable()) {
        progress_thread_.join();
    }
}

}  // namespace v1
}  // namespace rapid
