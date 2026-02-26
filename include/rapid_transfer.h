// rapid_transfer.h
//
// RapidTransfer v1 API (New Engine)
// Single-rail instance per class, multi-rail orchestration by upper layer
//
// Copyright (C) 2024 Feng Ren

#ifndef RAPID_TRANSFER_V1_H
#define RAPID_TRANSFER_V1_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace rapid {

// ============================================================================
// Common Data Types
// ============================================================================

/// Memory buffer descriptor
struct Buffer {
    void* addr;     // Buffer address
    size_t length;  // Buffer size in bytes
};

/// Transfer status enumeration
enum Status { UNKNOWN, PENDING, SUCCESS, FAILED };

/// Task identifier type
using TaskID = int;

/// Connection attributes map
using Attributes = std::unordered_map<std::string, std::string>;

}  // namespace rapid

namespace rapid {
namespace v1 {

// ============================================================================
// Rail Configuration
// ============================================================================

struct RailConfig {
    std::string device_name;  // RDMA device name (e.g., "mlx5_0")
    uint8_t rdma_port;        // RDMA port number
    int gid_index;            // GID index

    RailConfig() : rdma_port(1), gid_index(0) {}
};

// ============================================================================
// Rail State and Statistics
// ============================================================================

enum class RailState {
    ACTIVE,    // Rail is operational
    DEGRADED,  // Rail is slow but working
    FAILED,    // Rail has failed
    DISABLED   // Rail is administratively disabled
};

struct RailStats {
    std::string device_name;
    RailState state;
    uint64_t bytes_transferred;
    uint64_t transfer_count;
    double avg_latency_us;
    double current_bandwidth_gbps;
};

// ============================================================================
// Transfer Result
// ============================================================================

struct TransferResult {
    std::string task_id;
    Status status;
    size_t total_bytes{0};
    std::string device_name;
    double duration_ms{0.0};
    std::string error_message;
};

// ============================================================================
// RapidTransfer v1 - New Engine API
// ============================================================================

class RapidTransfer {
   public:
    // ========================================================================
    // Factory and Lifecycle
    // ========================================================================

    /// Create transfer engine instance
    /// Returns shared_ptr on success, nullptr on failure
    static std::shared_ptr<RapidTransfer> Create(
        const RailConfig& rail_config, const std::string& listen_address = "");

    virtual ~RapidTransfer();

    // Non-copyable, non-movable
    RapidTransfer(const RapidTransfer&) = delete;
    RapidTransfer& operator=(const RapidTransfer&) = delete;

    // ========================================================================
    // Single-Sided Write
    // ========================================================================

    /// Write to remote memory
    /// notify_message: optional message to send to remote peer
    TaskID write(const std::string& peer_name,
                 const std::vector<Buffer>& local_buffers,
                 const std::vector<Buffer>& remote_buffers,
                 const std::string& notify_message = "");

    // ========================================================================
    // Single-Sided Read
    // ========================================================================

    /// Read from remote memory
    /// notify_message: optional message to send to remote peer
    TaskID read(const std::string& peer_name,
                const std::vector<Buffer>& local_buffers,
                const std::vector<Buffer>& remote_buffers,
                const std::string& notify_message = "");

    // ========================================================================
    // Notification
    // ========================================================================

    /// Notification callback type for receiving messages from remote peers
    using NotificationCallback =
        std::function<void(const std::string& peer_name, TaskID task_id,
                           const std::string& message)>;

    /// Register callback for receiving notification messages from remote peers
    void setNotificationCallback(NotificationCallback callback);

    /// Send a notification message to remote peer
    int notify(const std::string& peer_name, TaskID task_id,
               const std::string& message);

    // ========================================================================
    // Status and Monitoring
    // ========================================================================

    /// Get transfer status
    Status getStatus(TaskID task_id, size_t* transferred_bytes = nullptr);

    /// Wait for transfer completion (blocking)
    Status wait(TaskID task_id,
                std::chrono::milliseconds timeout = std::chrono::milliseconds{
                    5000});

    /// Get detailed transfer result
    std::optional<TransferResult> getTransferResult(TaskID task_id);

    /// Free task resources
    int freeTask(TaskID task_id);

    // ========================================================================
    // Rail Information
    // ========================================================================

    /// Get rail configuration
    RailConfig getRailConfig() const;

    /// Get rail statistics
    RailStats getRailStats() const;

    /// Get current rail state
    RailState getRailState() const;

    // ========================================================================
    // Memory Registration
    // ========================================================================

    /// Register local memory region
    int registerLocalMemory(void* addr, size_t length);

    /// Unregister local memory region
    int unregisterLocalMemory(void* addr);

    // ========================================================================
    // Buffer Info Exchange (for testing)
    // ========================================================================

    /// Buffer info structure for sharing buffer information between peers
    struct BufferInfo {
        uint64_t addr;   // Buffer address as uint64_t for safe serialization
        uint64_t length; // Buffer size
        uint32_t rkey;   // Remote key
    };

    /// Set local buffer info to share with remote peers
    void setBufferInfo(const BufferInfo& info);

    /// Get remote peer's buffer info via RPC
    std::optional<BufferInfo> getRemoteBufferInfo(const std::string& peer_address);

    // ========================================================================
    // TCP Bootstrap Server
    // ========================================================================

    /// Start TCP bootstrap listener for handling peer connection requests
    /// The listener will automatically handle UD connection exchange and buffer info requests
    int startBootstrapListener(const std::string& tcp_address);

    /// Stop TCP bootstrap listener
    void stopBootstrapListener();

    // ========================================================================
    // Lifecycle Management
    // ========================================================================

    /// Run one step of progress engine
    int runStep();

    /// Shutdown the transfer engine
    int shutdown();

   private:
    // ========================================================================
    // Internal Implementation
    // ========================================================================

    // Pimpl idiom - forward declaration
    class Impl;
    std::unique_ptr<Impl> impl_;

    RapidTransfer() = default;
};

// Forward declaration - TcpBootstrap is defined in tcp_bootstrap.h
class TcpBootstrap;

}  // namespace v1
}  // namespace rapid

#endif  // RAPID_TRANSFER_V1_H
