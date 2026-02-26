// scheduler.cpp
//
// RapidTransfer QP Pool Scheduler Implementation
//
// Copyright (C) 2026 RapidTransfer Team

#include "scheduler.h"

#include <glog/logging.h>
#include <chrono>

namespace rapid {
namespace v1 {

Scheduler::Scheduler(const SchedulerConfig& config)
    : config_(config) {

    // Initialize QP pool
    qp_pool_.resize(config_.pool_size);
    for (size_t i = 0; i < config_.pool_size; ++i) {
        qp_pool_[i].qp_num = 0;      // Will be set during initialization
        qp_pool_[i].bound_session_id = -1;
        qp_pool_[i].last_active_ts = 0;
        free_qp_ids_.push(i);
    }

    LOG(INFO) << "[Scheduler] Initialized with QP pool size: " << config_.pool_size;
}

int Scheduler::acquireQP(int session_id) {
    // Try to allocate from pool
    if (free_qp_ids_.empty()) {
        // Pool is full, try to reclaim idle QPs
        reclaimIdleQPs();

        if (free_qp_ids_.empty()) {
            // Still no available QP
            LOG(WARNING) << "[Scheduler] QP pool exhausted, session " << session_id << " waits";
            return -1;
        }
    }

    // Allocate QP
    int qp_id = free_qp_ids_.front();
    free_qp_ids_.pop();

    // Bind to session
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
        LOG(ERROR) << "[Scheduler] Session " << session_id << " not found";
        free_qp_ids_.push(qp_id);
        return -1;
    }

    it->second.bound_qp_id = qp_id;
    qp_pool_[qp_id].bound_session_id = session_id;
    qp_pool_[qp_id].last_active_ts = getCurrentTimestamp();

    LOG(INFO) << "[Scheduler] Allocated QP " << qp_id << " to session " << session_id;
    return qp_id;
}

void Scheduler::releaseQP(int session_id) {
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return;

    int qp_id = it->second.bound_qp_id;
    if (qp_id < 0) return;  // Not bound

    // Unbind
    it->second.bound_qp_id = -1;
    qp_pool_[qp_id].bound_session_id = -1;
    qp_pool_[qp_id].last_active_ts = 0;

    // Return to pool
    free_qp_ids_.push(qp_id);

    LOG(INFO) << "[Scheduler] Released QP " << qp_id << " from session " << session_id;
}

void Scheduler::reclaimIdleQPs() {
    uint64_t now = getCurrentTimestamp();
    int reclaimed = 0;

    for (auto& qp : qp_pool_) {
        if (qp.bound_session_id < 0) continue;  // Already idle

        uint64_t idle_time = now - qp.last_active_ts;
        if (idle_time > config_.idle_timeout_us) {
            // Reclaim this QP
            LOG(INFO) << "[Scheduler] Reclaiming idle QP (idle for " << idle_time << "us)";
            releaseQP(qp.bound_session_id);
            reclaimed++;
        }
    }

    if (reclaimed > 0) {
        LOG(INFO) << "[Scheduler] Reclaimed " << reclaimed << " idle QPs";
    }
}

VirtualSession* Scheduler::getOrCreateSession(const std::string& peer_addr) {
    // Check if session already exists
    auto it = peer_to_session_.find(peer_addr);
    if (it != peer_to_session_.end()) {
        return getSession(it->second);
    }

    // Create new session
    int session_id = next_session_id_.fetch_add(1);

    VirtualSession session;
    session.session_id = session_id;
    session.peer_addr = peer_addr;
    session.bound_qp_id = -1;
    session.next_chunk_id = 0;
    session.next_seq_num = 0;
    session.recv_bitmap = 0;
    session.expected_chunk_id = 0;
    session.task_id = -1;
    session.state = VirtualSession::State::IDLE;

    sessions_[session_id] = session;
    peer_to_session_[peer_addr] = session_id;

    LOG(INFO) << "[Scheduler] Created session " << session_id << " for peer " << peer_addr;
    return getSession(session_id);
}

void Scheduler::destroySession(int session_id) {
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return;

    // Release QP if bound
    if (it->second.bound_qp_id >= 0) {
        releaseQP(session_id);
    }

    // Remove from peer mapping
    peer_to_session_.erase(it->second.peer_addr);

    // Remove session
    sessions_.erase(it);

    LOG(INFO) << "[Scheduler] Destroyed session " << session_id;
}

VirtualSession* Scheduler::getSession(int session_id) {
    auto it = sessions_.find(session_id);
    if (it == sessions_.end()) return nullptr;
    return &it->second;
}

void Scheduler::runStep() {
    // Reclaim idle QPs
    reclaimIdleQPs();

    // Note: WFQ scheduling is an optional enhancement
    // Current round-robin scheduling is sufficient for basic functionality
}

uint64_t Scheduler::getCurrentTimestamp() const {
    auto now = std::chrono::steady_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

} // namespace v1
} // namespace rapid
