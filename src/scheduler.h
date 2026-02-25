// scheduler.h
//
// RapidXfer QP Pool Scheduler
// Manages dynamic QP allocation for virtual sessions
//
// Copyright (C) 2026 RapidXfer Team

#ifndef SCHEDULER_H_
#define SCHEDULER_H_

#include <atomic>
#include <queue>
#include <vector>
#include <unordered_map>

#include "rapid_transfer.h"

namespace rapid {
namespace rapidxfer {

// Forward declaration
class Context;

// Physical QP state
struct PhysicalQP {
    uint32_t qp_num;           // QP number
    int bound_session_id;      // Bound session ID (-1 = idle)
    uint64_t last_active_ts;   // Last active timestamp (microseconds)
};

// Virtual session state
struct VirtualSession {
    int session_id;            // Session ID
    std::string peer_addr;     // Peer address

    // QP binding
    int bound_qp_id;           // Bound physical QP ID (-1 = unbound)

    // Send state (for write operations)
    uint32_t next_chunk_id;     // Next chunk ID to send
    uint16_t next_seq_num;      // Next sequence number within chunk
    uint64_t send_bitmap;       // Send bitmap (which packets are ACKed)
    uint32_t total_pkts_in_chunk; // Total packets in current chunk
    uint64_t chunk_start_ts;    // Chunk start timestamp (for timeout)

    // Receive state (for read operations)
    uint64_t recv_bitmap;       // Receive bitmap (which packets received)
    uint32_t expected_chunk_id; // Expected chunk ID
    uint32_t expected_seq_num;  // Expected sequence number

    // Data buffers
    std::vector<Buffer> local_buffers;   // Local buffers (for write)
    std::vector<Buffer> remote_buffers;  // Remote buffers (for read)

    // Task tracking
    TaskID task_id;             // Associated task ID
    enum class State { IDLE, SENDING, RECEIVING, COMPLETE, FAILED };
    State state;

    // Statistics
    uint64_t last_active_ts;    // Last activity timestamp
    uint32_t bytes_sent;        // Bytes sent in current chunk
    uint32_t bytes_acked;       // Bytes acknowledged
};

// Scheduler configuration
struct SchedulerConfig {
    size_t pool_size = 32;                 // QP pool size
    uint64_t idle_timeout_us = 100;        // QP idle timeout (100us)
    size_t max_sessions = 100000;          // Maximum virtual sessions
};

class Scheduler {
public:
    Scheduler(const SchedulerConfig& config);
    ~Scheduler() = default;

    // ========== QP Pool Management ==========

    // Allocate a QP for a session
    // Returns QP ID, -1 if pool is full
    int acquireQP(int session_id);

    // Release a QP from a session
    void releaseQP(int session_id);

    // Reclaim idle QPs (call periodically)
    void reclaimIdleQPs();

    // ========== Session Management ==========

    // Create or get a session
    int getOrCreateSession(const std::string& peer_addr);

    // Destroy a session
    void destroySession(int session_id);

    // Get session by ID
    VirtualSession* getSession(int session_id);

    // Get session by peer address
    VirtualSession* getSessionByPeer(const std::string& peer_addr);

    // ========== Progress Loop ==========

    // Run one step of scheduler (call in event loop)
    void runStep();

    // ========== Accessors ==========

    const SchedulerConfig& getConfig() const { return config_; }
    size_t getActiveSessionCount() const { return sessions_.size(); }
    size_t getFreeQPCount() const { return free_qp_ids_.size(); }

private:
    // Configuration
    const SchedulerConfig config_;

    // QP pool
    std::vector<PhysicalQP> qp_pool_;
    std::queue<int> free_qp_ids_;

    // Session table
    std::unordered_map<int, VirtualSession> sessions_;
    std::unordered_map<std::string, int> peer_to_session_;

    // Session ID allocator
    std::atomic<int> next_session_id_{1};

    // Get current timestamp in microseconds
    uint64_t getCurrentTimestamp() const;
};

} // namespace rapidxfer
} // namespace rapid

#endif  // SCHEDULER_H_
