// ud_control_manager.h
//
// UD-based control plane manager
// Replaces SessionManager with RDMA UD-based control messaging
//
// Copyright (C) 2024 Feng Ren

#ifndef UD_CONTROL_MANAGER_H_
#define UD_CONTROL_MANAGER_H_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "concurrency.h"
#include "context.h"
#include "rapid_transfer.h"
#include "ud_control_protocol.h"

namespace rapid {

// Forward declarations
class UDControlManager {

/**
 * UD-based Control Plane Manager
 *
 * Uses unreliable UDP (UD) for control messaging with automatic retry mechanism.
 *
 * RETRY STRATEGY:
 * - All RPC operations automatically retry up to kMaxRetries (default: 3) times
 * - Each attempt uses a shorter timeout (kRetryTimeout: 200ms) for faster failure detection
 * - Total timeout for an operation: kMaxRetries * kRetryTimeout (default: 600ms)
 *
 * AT-MOST-ONCE SEMANTICS:
 * Server-side deduplication ensures operations execute at most once, even with retries:
 *
 * 1. Buffer Info Request (BUFFER_INFO_REQUEST):
 *    - INHERENTLY IDEMPOTENT: Multiple requests return the same buffer information
 *    - No deduplication needed (read-only operation)
 *
 * 2. Read Request (READ_REQUEST):
 *    - AT-MOST-ONCE: Server deduplicates based on hash of (peer_name, session_name, buffers_json)
 *    - Duplicate requests return the same task_id without creating new tasks
 *    - Cache entries expire after kDedupTTL (60 seconds) to prevent unbounded growth
 *
 * 3. Notification (NOTIFICATION):
 *    - AT-MOST-ONCE: Server deduplicates based on hash of (peer_name, task_id, message)
 *    - Duplicate notifications are silently ignored (callback invoked only once)
 *    - Cache entries expire after kDedupTTL (60 seconds) to prevent unbounded growth
 *
 * 4. Connection Request/Response:
 *    - IDEMPOTENT: Repeated connection requests return the same session info
 *    - TCP bootstrap ensures connection is established only once
 */
public:
    // Callback types (compatible with SessionManager)
    using OnAcceptCallback =
        std::function<int(const std::string&, const Attributes&, Attributes&)>;

    using OnErrorCallback = std::function<void(const std::string&)>;

    using OnReadRequestCallback =
        std::function<int(const std::string& peer_name,
                          const std::vector<Buffer>& local_targets,
                          const std::vector<Buffer>& data_sources)>;

    using OnNotificationCallback = std::function<void(
        const std::string& peer_name, int task_id, const std::string& message)>;

    UDControlManager(Context& ud_context);
    ~UDControlManager();

    // Disable copy and assignment
    UDControlManager(const UDControlManager&) = delete;
    UDControlManager& operator=(const UDControlManager&) = delete;

    // Start/stop TCP bootstrap listener for exchanging UD connection info
    int startListener(const std::string& tcp_address,
                      const OnAcceptCallback& on_accept,
                      const OnErrorCallback& on_error);

    int shutdownListener();

    // Connect to remote peer (establishes UD connection via TCP bootstrap)
    int connect(const std::string& address, const Attributes& request,
                Attributes& response);

    int disconnect(const std::string& address);

    bool hasConnection(const std::string& address);

    // Get the generated session name for a peer address
    std::string getSessionName(const std::string& peer_address);

    // Set callbacks
    void setReadCallback(const OnReadRequestCallback& on_read);
    void setNotificationCallback(const OnNotificationCallback& on_notification);

    // Send control messages (replace RPC calls)
    int sendReadRequest(const std::string& peer_name,
                       const std::string& session_name,
                       const std::string& buffers_json,
                       int& task_id);

    int sendNotification(const std::string& peer_name, int task_id,
                       const std::string& message);

    std::optional<BufferInfo> getBufferInfo(const std::string& peer_address);

    // Buffer info management
    void setBufferInfo(const BufferInfo& info);

    // Handle received control message (called from Context)
    int handleControlPacket(const std::string& peer_name,
                           const uint8_t* data,
                           size_t length);

    // Progress engine (call regularly to check timeouts)
    int runStep();

private:
    // TCP bootstrap implementation
    int startBootstrapListener(const std::string& tcp_address);
    int stopBootstrapListener();
    void bootstrapAcceptThread();
    void handleBootstrapConnection(int client_fd);

    // Send control message over UD
    int sendControlMessage(const std::string& peer_name,
                          const std::vector<uint8_t>& message);

    // Request/response correlation
    struct PendingRequest {
        std::chrono::steady_clock::time_point deadline;
        std::function<void(const uint8_t*, size_t)> callback;
        std::condition_variable* cv;  // Pointer to allow move
        bool completed{false};
    };

    uint32_t allocateRequestId();
    int registerPendingRequest(uint32_t request_id,
                               std::chrono::milliseconds timeout,
                               const std::function<void(const uint8_t*, size_t)>& callback);
    int waitForResult(uint32_t request_id, std::chrono::milliseconds timeout);
    int completeRequest(uint32_t request_id, const uint8_t* data, size_t length);

    // Message handlers
    int handleConnectionRequest(const std::string& peer_name,
                               uint32_t request_id,
                               const std::vector<uint8_t>& payload);

    int handleConnectionResponse(const std::string& peer_name,
                                uint32_t request_id,
                                const std::vector<uint8_t>& payload);

    int handleReadRequestMessage(const std::string& peer_name,
                                uint32_t request_id,
                                const std::vector<uint8_t>& payload);

    int handleReadResponseMessage(const std::string& peer_name,
                                 uint32_t request_id,
                                 const std::vector<uint8_t>& payload);

    int handleNotificationMessage(const std::string& peer_name,
                                 uint32_t request_id,
                                 const std::vector<uint8_t>& payload);

    int handleBufferInfoRequest(const std::string& peer_name,
                               uint32_t request_id);

    int handleBufferInfoResponse(const std::string& peer_name,
                                uint32_t request_id,
                                const std::vector<uint8_t>& payload);

    int handleErrorResponse(const std::string& peer_name,
                           uint32_t request_id,
                           const std::vector<uint8_t>& payload);

private:
    // UD Context for sending control messages
    Context& ud_context_;

    // Callbacks
    OnAcceptCallback on_accept_;
    OnErrorCallback on_error_;
    OnReadRequestCallback on_read_request_;
    OnNotificationCallback on_notification_;

    // Connection tracking
    RWSpinlock sessions_lock_;
    std::unordered_set<std::string> sessions_;
    std::atomic<int> uid_{0};

    // Map from peer address to session name
    std::unordered_map<std::string, std::string> peer_to_session_map_;

    // Map from session name to peer address (reverse lookup)
    std::unordered_map<std::string, std::string> session_to_peer_map_;

    // Map from GID:QP to peer address (for reverse lookup of incoming UD packets)
    std::unordered_map<std::string, std::string> gid_qp_to_peer_map_;

    // Pending requests for request/response correlation
    std::unordered_map<uint32_t, std::unique_ptr<PendingRequest>> pending_requests_;
    std::mutex pending_requests_mutex_;
    std::atomic<uint32_t> next_request_id_{1};

    // TCP bootstrap listener
    int tcp_listen_fd_{-1};
    std::thread tcp_accept_thread_;
    std::atomic<bool> tcp_listener_running_{false};
    std::string tcp_listen_address_;

    // Local buffer info for sharing with peers
    BufferInfo local_buffer_info_{0, 0, 0};
    std::atomic<bool> buffer_info_available_{false};

    // Retry configuration for control messages
    static constexpr int kMaxRetries = 3;
    static constexpr std::chrono::milliseconds kRetryTimeout{200};  // 200ms per attempt

    // At-most-once semantics: Deduplication cache for requests
    // Key: hash of (peer_name, request_type, request_params)
    // Value: For read requests = task_id, for notifications = seen timestamp
    struct DedupKey {
        std::string peer_name;
        uint16_t request_type;      // ControlMessageType
        std::string request_hash;   // Hash of request parameters

        bool operator==(const DedupKey& other) const {
            return peer_name == other.peer_name &&
                   request_type == other.request_type &&
                   request_hash == other.request_hash;
        }
    };

    struct DedupKeyHash {
        std::size_t operator()(const DedupKey& k) const {
            return std::hash<std::string>{}(k.peer_name) ^
                   (std::hash<uint16_t>{}(k.request_type) << 1) ^
                   (std::hash<std::string>{}(k.request_hash) << 2);
        }
    };

    struct DedupEntry {
        int task_id;                    // For read requests
        int64_t timestamp_ns;           // When request was first processed
        std::chrono::steady_clock::time_point expiry_time;
    };

    std::unordered_map<DedupKey, DedupEntry, DedupKeyHash> dedup_cache_;
    std::mutex dedup_mutex_;

    // Clean up expired dedup entries (call periodically)
    void cleanupExpiredDedupEntries();
    static constexpr std::chrono::seconds kDedupTTL{60};  // TTL 60 seconds
};

} // namespace rapid

#endif  // UD_CONTROL_MANAGER_H_
