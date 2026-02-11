// impl.h
//
// RapidTransfer v1 Implementation
// Wraps existing RDMA UD Context
//
// Copyright (C) 2024 Feng Ren

#ifndef TRANSFER_ENGINE_IMPL_H
#define TRANSFER_ENGINE_IMPL_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "context.h"
#include "rapid_transfer.h"

// Forward declaration
namespace rapid {
class SessionManager;
}

namespace rapid {
namespace v1 {

class RapidTransfer::Impl {
   public:
    // UD Context configuration constants
    static const size_t kMtuSize = 4096;
    static const size_t kMaxPackets = 102400 / 4 * 6;
    static const size_t kQueueCapacity = 4096;

    Impl() : ud_context_(kMtuSize, kMaxPackets, kQueueCapacity) {}
    ~Impl() { shutdown(); }

    // ========================================================================
    // Initialization
    // ========================================================================

    int initialize(const RailConfig& rail_config,
                   const std::string& listen_address);

    int shutdown();

    int runStep();

    // ========================================================================
    // Connection Management
    // ========================================================================

    int prepareConnection(const std::string& peer_name,
                          Attributes& local_attrs);
    int setupConnection(const std::string& peer_name,
                        const Attributes& peer_attrs);
    int ensureConnection(const std::string& peer_name, bool for_write = true);

    // ========================================================================
    // Write/Read Operations (using UD send/receive)
    // ========================================================================

    TaskID write(const std::string& peer_name,
                 const std::vector<Buffer>& local_buffers,
                 const std::vector<Buffer>& remote_buffers,
                 const std::string& notify_message);

    TaskID read(const std::string& peer_name,
                const std::vector<Buffer>& local_buffers,
                const std::vector<Buffer>& remote_buffers,
                const std::string& notify_message);

    // ========================================================================
    // Status and Operations
    // ========================================================================

    Status getStatus(TaskID task_id, size_t* transferred_bytes);
    Status wait(TaskID task_id, std::chrono::milliseconds timeout);
    std::optional<TransferResult> getTransferResult(TaskID task_id);
    int freeTask(TaskID task_id);

    // ========================================================================
    // Notifications
    // ========================================================================

    void setNotificationCallback(NotificationCallback callback);
    int notify(const std::string& peer_name, TaskID task_id,
               const std::string& message);

    // ========================================================================
    // Rail Information
    // ========================================================================

    RailConfig getRailConfig() const { return rail_config_; }
    RailStats getRailStats() const;
    RailState getRailState() const { return rail_state_; }

    // ========================================================================
    // Memory Registration
    // ========================================================================

    int registerLocalMemory(void* addr, size_t length);
    int unregisterLocalMemory(void* addr);

    // ========================================================================
    // Buffer Info Exchange
    // ========================================================================

    void setBufferInfo(const BufferInfo& info);
    std::optional<BufferInfo> getRemoteBufferInfo(const std::string& peer_address);

   private:
    // ========================================================================
    // Internal Implementation
    // ========================================================================

    int makeConnectionIfNeeded(const std::string& peer_name);
    void startProgressThread();
    void stopProgressThread();

   private:
    // ========================================================================
    // Member Variables
    // ========================================================================

    // UD Context (direct use of existing UD implementation)
    Context ud_context_;

    // Session Manager for automatic connection establishment
    ::rapid::SessionManager* session_manager_{nullptr};

    // Map from peer address to session name (e.g., "localhost:12348" ->
    // "server/0")
    std::unordered_map<std::string, std::string> peer_to_session_map_;

    // Configuration
    RailConfig rail_config_;
    std::string listen_address_;
    RailState rail_state_{RailState::ACTIVE};
    std::chrono::steady_clock::time_point start_time_;

    // Task tracking
    std::unordered_map<TaskID, TransferResult> task_results_;
    mutable std::mutex task_results_mutex_;
    std::condition_variable task_cv_;

    // Pending notifications (task_id -> {peer_name, message})
    struct PendingNotification {
        std::string peer_name;
        std::string message;
        bool sent{false};  // Track if notification was sent
    };
    std::unordered_map<TaskID, PendingNotification> pending_notifications_;
    mutable std::mutex notifications_mutex_;

    // Notification callback
    NotificationCallback user_notification_callback_;
    mutable std::mutex notification_mutex_;

    // Statistics
    std::atomic<uint64_t> bytes_sent_{0};
    std::atomic<uint64_t> bytes_recv_{0};
    std::atomic<uint64_t> send_count_{0};
    std::atomic<uint64_t> recv_count_{0};

    // Progress thread
    std::thread progress_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace v1
}  // namespace rapid

#endif  // TRANSFER_ENGINE_IMPL_H
