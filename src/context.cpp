// context.cpp
//
// RapidXfer Context Implementation - Unified Message Processing
//
// Copyright (C) 2026 RapidXfer Team

#include "context.h"

#include "protocols/common/rdma_ud_endpoint.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <unistd.h>

namespace rapid {

static inline uint64_t GetCurrentTS() {
    struct timeval tv_now;
    gettimeofday(&tv_now, nullptr);
    return (tv_now.tv_sec * 1000000 + tv_now.tv_usec);
}

// ========== Constructor/Destructor ==========

Context::Context(size_t mtu_size, size_t max_packets, size_t queue_capacity)
    : mtu_size_(mtu_size),
      packet_manager_(mtu_size, max_packets, queue_capacity),
      next_task_id_(0) {

    // Create scheduler
    rapidxfer::SchedulerConfig config;
    scheduler_ = std::make_unique<rapidxfer::Scheduler>(config);
}

Context::~Context() {
    deconstruct();
}

int Context::construct(const std::string& device_name, uint8_t rdma_port,
                       int gid_index) {
    // Initialize controller
    int ret = controller_.construct(device_name, rdma_port, gid_index);
    if (ret < 0) return ret;

    // Initialize packet manager
    ret = packet_manager_.construct(device_name);
    if (ret < 0) return ret;

    // Register memory region
    auto& pool = packet_manager_.getPool();
    void* arena_base = pool.getArena();
    size_t arena_capacity = pool.getCapacity();
    ret = controller_.context().registerMemoryRegion(arena_base, arena_capacity,
                                                     IBV_ACCESS_LOCAL_WRITE);
    if (ret) return ret;
    local_arena_lkey_ = controller_.context().key(arena_base).first;

    // Pre-post receive WRs
    const size_t kNumReceiveHandles = 64;
    for (size_t i = 0; i < kNumReceiveHandles; ++i) {
        PacketHandle handle;
        ret = pool.allocatePacket(handle, true);
        if (ret < 0) return ret;
        ret = submitNormalRecvWR(handle);
        if (ret < 0) return ret;
    }

    LOG(INFO) << "[Context] Initialized with device: " << device_name;
    return 0;
}

int Context::deconstruct() {
    controller_.context().unregisterMemoryRegion(
        packet_manager_.getPool().getArena());
    packet_manager_.deconstruct();
    controller_.deconstruct();
    return 0;
}

// ========== Connection Management ==========

int Context::prepareConnection(const std::string& peer_addr, Attributes& local) {
    return controller_.prepareConnection(peer_addr, local);
}

int Context::setupConnection(const std::string& peer_addr, const Attributes& peer) {
    return controller_.setupConnection(peer_addr, peer);
}

int Context::registerLocalMemory(void* addr, size_t length) {
    return controller_.context().registerMemoryRegion(addr, length,
                                                      IBV_ACCESS_LOCAL_WRITE);
}

int Context::unregisterLocalMemory(void* addr) {
    return controller_.context().unregisterMemoryRegion(addr);
}

// ========== Unified Message Sending ==========

int Context::sendDataPacket(const std::string& peer_name,
                            const rapidxfer::RapidXferHeader& rx_header,
                            const std::vector<uint8_t>& payload) {

    // Get or create session
    int session_id = scheduler_->getOrCreateSession(peer_name);
    auto* session = scheduler_->getSession(session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Failed to get session for " << peer_name;
        return -1;
    }

    // Acquire QP if needed
    if (session->bound_qp_id < 0) {
        int qp_id = scheduler_->acquireQP(session_id);
        if (qp_id < 0) {
            LOG(WARNING) << "[Context] No available QP for session " << session_id;
            return -1;  // TODO: Queue for later retry
        }
        LOG(INFO) << "[Context] Bound QP " << qp_id << " to session " << session_id;
    }

    // Allocate packet handle
    PacketHandle handle;
    auto& pool = packet_manager_.getPool();
    int ret = pool.allocatePacket(handle, false);
    if (ret < 0) {
        LOG(ERROR) << "[Context] Failed to allocate packet";
        return -1;
    }

    // Build packet with RapidXfer header + payload
    uint8_t* packet_buf = static_cast<uint8_t*>(handle.getRawPacket());
    size_t total_size = sizeof(rapidxfer::RapidXferHeader) + payload.size();

    if (total_size > mtu_size_) {
        LOG(ERROR) << "[Context] Packet size " << total_size << " exceeds MTU " << mtu_size_;
        pool.freePacket(handle);
        return -1;
    }

    // Copy header
    memcpy(packet_buf, &rx_header, sizeof(rapidxfer::RapidXferHeader));

    // Copy payload (if any)
    if (!payload.empty()) {
        memcpy(packet_buf + sizeof(rapidxfer::RapidXferHeader), payload.data(), payload.size());
    }

    // Get endpoint
    auto endpoint = controller_.getOrCreateEndpoint(session_id);
    if (!endpoint) {
        LOG(ERROR) << "[Context] Failed to get endpoint for session " << session_id;
        pool.freePacket(handle);
        return -1;
    }

    // Create and post send request
    Request req;
    req.addr[0] = packet_buf;
    req.length[0] = total_size;
    req.lkey[0] = local_arena_lkey_;
    req.addr[1] = nullptr;
    req.length[1] = 0;
    req.lkey[1] = 0;

    std::vector<Request*> requests = {&req};
    int send_ret = endpoint->postSendRequest(requests);

    if (send_ret != 0) {
        LOG(ERROR) << "[Context] Failed to post send request";
        pool.freePacket(handle);
        return -1;
    }

    // Note: Don't free packet here - it will be freed after send completion
    // For now, just mark as inflight
    handle.inflight = true;

    LOG(INFO) << "[Context] Sent packet to " << peer_name
              << ", flags=0x" << std::hex << rx_header.flags << std::dec
              << ", size=" << total_size;

    return 0;
}

int Context::sendReadRequest(const std::string& peer_name,
                             const std::vector<Buffer>& local_targets,
                             const std::vector<Buffer>& remote_sources) {

    LOG(INFO) << "[Context] Sending read request to " << peer_name;

    // Build header with READ_REQUEST flag
    rapidxfer::RapidXferHeader header;
    header.session_id = 0;
    header.chunk_id = 0;
    header.seq_num = 0;
    header.flags = rapidxfer::READ_REQUEST;
    header.timestamp = GetCurrentTS();

    // Build payload
    std::vector<uint8_t> payload;
    uint32_t num_local = local_targets.size();
    uint32_t num_remote = remote_sources.size();

    // Reserve space
    size_t payload_size = sizeof(uint32_t) * 2 + 
                          sizeof(Buffer) * (num_local + num_remote);
    payload.resize(payload_size);
    uint8_t* ptr = payload.data();

    // Serialize
    memcpy(ptr, &num_local, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, local_targets.data(), sizeof(Buffer) * num_local);
    ptr += sizeof(Buffer) * num_local;
    memcpy(ptr, &num_remote, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, remote_sources.data(), sizeof(Buffer) * num_remote);

    return sendDataPacket(peer_name, header, payload);
}

int Context::sendSACK(const std::string& peer_name,
                      uint32_t session_id, uint32_t chunk_id,
                      uint64_t recv_bitmap, uint16_t bitmap_start) {

    rapidxfer::RapidXferHeader header;
    header.session_id = session_id;
    header.chunk_id = chunk_id;
    header.seq_num = 0;
    header.flags = rapidxfer::SACK;
    header.timestamp = GetCurrentTS();

    rapidxfer::SACKPayload sack;
    sack.recv_bitmap = recv_bitmap;
    sack.bitmap_start = bitmap_start;

    std::vector<uint8_t> payload(sizeof(rapidxfer::SACKPayload));
    memcpy(payload.data(), &sack, sizeof(rapidxfer::SACKPayload));

    LOG(INFO) << "[Context] Sending SACK to " << peer_name
              << ", bitmap=0x" << std::hex << recv_bitmap << std::dec;

    return sendDataPacket(peer_name, header, payload);
}

int Context::sendChunkAck(const std::string& peer_name,
                         uint32_t session_id, uint32_t chunk_id,
                         uint32_t total_pkts, uint64_t recv_bitmap) {

    rapidxfer::RapidXferHeader header;
    header.session_id = session_id;
    header.chunk_id = chunk_id;
    header.seq_num = 0;
    header.flags = rapidxfer::CHUNK_ACK;
    header.timestamp = GetCurrentTS();

    rapidxfer::ChunkAckPayload ack;
    ack.total_pkts = total_pkts;
    ack.recv_bitmap = recv_bitmap;

    std::vector<uint8_t> payload(sizeof(rapidxfer::ChunkAckPayload));
    memcpy(payload.data(), &ack, sizeof(rapidxfer::ChunkAckPayload));

    LOG(INFO) << "[Context] Sending Chunk-ACK to " << peer_name
              << ", chunk=" << chunk_id;

    return sendDataPacket(peer_name, header, payload);
}

int Context::sendNotification(const std::string& peer_name,
                              TaskID task_id,
                              const std::string& message) {

    // Truncate message if too long
    std::string truncated_msg = message;
    if (truncated_msg.size() > 255) {
        truncated_msg = message.substr(0, 255);
        LOG(WARNING) << "[Context] Notification message truncated to 255 chars";
    }

    // Build notification payload
    rapidxfer::NotificationPayload notif;
    notif.task_id = task_id;
    notif.message_length = truncated_msg.size();
    memcpy(notif.message, truncated_msg.c_str(), truncated_msg.size());
    notif.message[truncated_msg.size()] = '\0';

    // Build header
    rapidxfer::RapidXferHeader header;
    header.session_id = 0;  // Notifications are not session-specific
    header.chunk_id = 0;
    header.seq_num = 0;
    header.flags = rapidxfer::NOTIFICATION;
    header.timestamp = GetCurrentTS();

    // Serialize payload
    std::vector<uint8_t> payload(sizeof(notif));
    memcpy(payload.data(), &notif, sizeof(notif));

    LOG(INFO) << "[Context] Sending notification to " << peer_name
              << ", task_id=" << task_id
              << ", message=" << truncated_msg;

    return sendDataPacket(peer_name, header, payload);
}

// ========== Write Operations ==========

TaskID Context::startWrite(const std::string& peer_name,
                            const std::vector<Buffer>& local_buffers,
                            const std::vector<Buffer>& remote_buffers) {

    LOG(INFO) << "[Context] Starting write to " << peer_name
              << ", buffers=" << local_buffers.size();

    // Calculate total size
    size_t total_bytes = 0;
    for (const auto& buf : local_buffers) {
        total_bytes += buf.length;
    }

    // Get or create session
    int session_id = scheduler_->getOrCreateSession(peer_name);
    auto* session = scheduler_->getSession(session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Failed to get session";
        return -1;
    }

    // Initialize session for sending
    session->state = rapidxfer::VirtualSession::State::SENDING;
    session->local_buffers = local_buffers;
    session->remote_buffers = remote_buffers;
    session->next_chunk_id = 0;
    session->next_seq_num = 0;
    session->send_bitmap = 0;
    session->bytes_sent = 0;
    session->bytes_acked = 0;
    session->chunk_start_ts = GetCurrentTS();
    session->last_active_ts = session->chunk_start_ts;

    // Allocate task ID
    TaskID task_id = next_task_id_.fetch_add(1);
    session->task_id = task_id;

    // Calculate total chunks and packets
    size_t total_chunks = (total_bytes + kChunkSize - 1) / kChunkSize;

    session->total_pkts_in_chunk = std::min((size_t)kWindowPackets,
                                            (total_bytes + kMaxDataPerPkt - 1) / kMaxDataPerPkt);

    LOG(INFO) << "[Context] Write task " << task_id
              << ", total_bytes=" << total_bytes
              << ", chunks=" << total_chunks
              << ", first_chunk_pkts=" << session->total_pkts_in_chunk;

    // Start sending first chunk
    sendChunkPackets(session_id, session->next_chunk_id);

    return task_id;
}

TaskID Context::startRead(const std::string& peer_name,
                          const std::vector<Buffer>& local_targets,
                          const std::vector<Buffer>& remote_sources) {

    LOG(INFO) << "[Context] Starting read from " << peer_name
              << ", buffers=" << local_targets.size();

    // Calculate total size
    size_t total_bytes = 0;
    for (const auto& buf : local_targets) {
        total_bytes += buf.length;
    }

    // Get or create session
    int session_id = scheduler_->getOrCreateSession(peer_name);
    auto* session = scheduler_->getSession(session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Failed to get session";
        return -1;
    }

    // Initialize session for receiving
    session->state = rapidxfer::VirtualSession::State::RECEIVING;
    session->local_buffers = local_targets;
    session->remote_buffers = remote_sources;
    session->recv_bitmap = 0;
    session->expected_chunk_id = 0;

    // Allocate task ID
    TaskID task_id = next_task_id_.fetch_add(1);
    session->task_id = task_id;

    LOG(INFO) << "[Context] Read task " << task_id
              << ", total_bytes=" << total_bytes;

    // Serialize Read Request payload
    uint32_t num_local = local_targets.size();
    uint32_t num_remote = remote_sources.size();

    size_t payload_size = sizeof(uint32_t) * 2 +
                          sizeof(Buffer) * (num_local + num_remote);
    std::vector<uint8_t> payload(payload_size);

    uint8_t* ptr = payload.data();
    memcpy(ptr, &num_local, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, local_targets.data(), sizeof(Buffer) * num_local);
    ptr += sizeof(Buffer) * num_local;
    memcpy(ptr, &num_remote, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, remote_sources.data(), sizeof(Buffer) * num_remote);

    // Build Read Request packet
    rapidxfer::RapidXferHeader header;
    header.session_id = session_id;
    header.chunk_id = 0;
    header.seq_num = 0;
    header.flags = rapidxfer::READ_REQUEST;
    header.timestamp = GetCurrentTS();

    // Send Read Request
    int ret = sendDataPacket(peer_name, header, payload);
    if (ret < 0) {
        LOG(ERROR) << "[Context] Failed to send Read Request";
        return -1;
    }

    LOG(INFO) << "[Context] Read request sent, task_id=" << task_id;

    return task_id;
}

int Context::sendChunkPackets(int session_id, uint32_t chunk_id) {
    auto* session = scheduler_->getSession(session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Session not found";
        return -1;
    }

    if (session->local_buffers.empty()) {
        LOG(ERROR) << "[Context] No buffers to send";
        return -1;
    }

    // Calculate current position in buffers
    size_t bytes_offset = chunk_id * kChunkSize;
    size_t chunk_bytes = std::min(kChunkSize,
                                  session->total_pkts_in_chunk * kMaxDataPerPkt);

    // Find buffer and offset for current position
    size_t buf_idx = 0;
    size_t buf_offset = 0;
    size_t remaining_offset = bytes_offset;

    for (const auto& buf : session->local_buffers) {
        if (remaining_offset < buf.length) {
            buf_offset = remaining_offset;
            break;
        }
        remaining_offset -= buf.length;
        buf_idx++;
    }

    if (buf_idx >= session->local_buffers.size()) {
        LOG(ERROR) << "[Context] Invalid buffer offset";
        return -1;
    }

    // Send packets in window
    uint16_t seq_start = session->next_seq_num;
    size_t bytes_sent_in_chunk = 0;
    size_t pkt_count = 0;

    while (pkt_count < kWindowPackets && bytes_sent_in_chunk < chunk_bytes) {
        // Calculate packet size
        size_t pkt_size = std::min(kMaxDataPerPkt, chunk_bytes - bytes_sent_in_chunk);
        size_t remaining_in_buf = session->local_buffers[buf_idx].length - buf_offset;
        size_t copy_size = std::min(pkt_size, remaining_in_buf);

        // Build header
        rapidxfer::RapidXferHeader header;
        header.session_id = session_id;
        header.chunk_id = chunk_id;
        header.seq_num = session->next_seq_num++;
        header.flags = rapidxfer::DATA_PACKET;
        header.timestamp = GetCurrentTS();

        // Build payload
        std::vector<uint8_t> payload(copy_size);
        const uint8_t* src = static_cast<const uint8_t*>(session->local_buffers[buf_idx].addr);
        memcpy(payload.data(), src + buf_offset, copy_size);

        // Send packet
        int ret = sendDataPacket(session->peer_addr, header, payload);
        if (ret < 0) {
            LOG(ERROR) << "[Context] Failed to send packet " << header.seq_num;
            // Decrement seq_num on failure
            session->next_seq_num--;
            break;
        }

        // Update position
        buf_offset += copy_size;
        bytes_sent_in_chunk += copy_size;
        pkt_count++;

        // Move to next buffer if needed
        if (buf_offset >= session->local_buffers[buf_idx].length) {
            buf_idx++;
            buf_offset = 0;
            if (buf_idx >= session->local_buffers.size()) {
                break;
            }
        }
    }

    session->bytes_sent += bytes_sent_in_chunk;
    session->last_active_ts = GetCurrentTS();

    LOG(INFO) << "[Context] Sent " << pkt_count << " packets for chunk " << chunk_id
              << ", seq=" << seq_start << "-" << (session->next_seq_num - 1);

    return pkt_count;
}

int Context::retransmitPackets(int session_id, uint32_t chunk_id) {
    auto* session = scheduler_->getSession(session_id);
    if (!session) {
        return -1;
    }

    // Check timeout
    uint64_t now = GetCurrentTS();
    if (now - session->last_active_ts < recv_rto_) {
        return 0;  // Not yet timed out
    }

    // Check if all packets are ACKed
    if (session->send_bitmap == rapidxfer::kChunkCompleteMask) {
        return 0;  // All packets ACKed, no retransmission needed
    }

    // Find packets that need retransmission (bits not set in send_bitmap)
    uint16_t window_start = (session->next_seq_num > kWindowPackets) ?
                            (session->next_seq_num - kWindowPackets) : 0;

    int retx_count = 0;
    const int kMaxRetxPerCall = 8;  // Limit retransmissions per call

    // Retransmit up to kMaxRetxPerCall missing packets
    for (uint16_t i = 0; i < kWindowPackets && retx_count < kMaxRetxPerCall; ++i) {
        if (!(session->send_bitmap & (1ULL << i))) {
            // Packet at relative position i is not ACKed
            uint16_t seq_num = window_start + i;

            // Calculate buffer position for this packet
            size_t pkt_offset = seq_num * kMaxDataPerPkt;
            size_t chunk_offset = chunk_id * kChunkSize;
            size_t total_offset = chunk_offset + pkt_offset;

            // Find which buffer this packet belongs to
            size_t buf_idx = 0;
            size_t buf_offset = 0;
            size_t accumulated = 0;

            for (const auto& buf : session->local_buffers) {
                if (accumulated + buf.length > total_offset) {
                    buf_offset = total_offset - accumulated;
                    break;
                }
                accumulated += buf.length;
                buf_idx++;
            }

            if (buf_idx >= session->local_buffers.size()) {
                LOG(WARNING) << "[Context] Invalid buffer index for retransmission";
                continue;
            }

            // Calculate packet size
            size_t remaining_in_buf = session->local_buffers[buf_idx].length - buf_offset;
            size_t pkt_size = std::min(kMaxDataPerPkt, remaining_in_buf);

            // Build header
            rapidxfer::RapidXferHeader header;
            header.session_id = session_id;
            header.chunk_id = chunk_id;
            header.seq_num = seq_num;
            header.flags = rapidxfer::DATA_PACKET;
            header.timestamp = GetCurrentTS();

            // Build payload
            std::vector<uint8_t> payload(pkt_size);
            const uint8_t* src = static_cast<const uint8_t*>(session->local_buffers[buf_idx].addr);
            memcpy(payload.data(), src + buf_offset, pkt_size);

            // Send packet
            int ret = sendDataPacket(session->peer_addr, header, payload);
            if (ret < 0) {
                LOG(ERROR) << "[Context] Failed to retransmit packet seq=" << seq_num;
            } else {
                retx_count++;
                LOG(INFO) << "[Context] Retransmitted seq=" << seq_num;
            }
        }
    }

    // Update RTO using exponential backoff
    recv_rto_ = std::min(recv_rto_ * 2, kMaxRTO);
    session->last_active_ts = now;

    LOG(INFO) << "[Context] Retransmitted " << retx_count << " packets, RTO=" << recv_rto_;

    return retx_count;
}

// ========== Progress Engine ==========

int Context::runStep() {
    uint64_t current_ts = GetCurrentTS();

    // Poll receive completions
    int ret = pollCompletedPackets(RECV_CQ, current_ts);
    if (ret < 0) return ret;

    // Send data packets
    ret = sendDataPackets(current_ts);
    if (ret < 0) return ret;

    // Poll send completions
    ret = pollCompletedPackets(SEND_CQ, current_ts);
    if (ret < 0) return ret;

    // Run scheduler
    scheduler_->runStep();

    return 0;
}

// ========== Unified Message Handling ==========

int Context::handlePacket(const std::string& peer_name,
                          const rapidxfer::RapidXferHeader& header,
                          const std::vector<uint8_t>& payload) {

    LOG(INFO) << "[Context] Received packet from " << peer_name
              << ", flags=0x" << std::hex << header.flags << std::dec;

    if (header.isReadRequest()) {
        return handleReadRequest(peer_name, header, payload);
    } else if (header.isSACK()) {
        return handleSACK(peer_name, header, payload);
    } else if (header.isChunkAck()) {
        return handleChunkAck(peer_name, header, payload);
    } else if (header.isNotification()) {
        return handleNotification(peer_name, header, payload);
    } else {
        return handleDataPacket(peer_name, header, payload);
    }
}

int Context::handleDataPacket(const std::string& peer_name,
                               const rapidxfer::RapidXferHeader& header,
                               const std::vector<uint8_t>& payload) {

    LOG(INFO) << "[Context] Data packet from " << peer_name
              << ", session=" << header.session_id
              << ", chunk=" << header.chunk_id
              << ", seq=" << header.seq_num
              << ", size=" << payload.size();

    // Get or create session
    int session_id = scheduler_->getOrCreateSession(peer_name);
    auto* session = scheduler_->getSession(session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Failed to get session";
        return -1;
    }

    // Initialize receive state for new chunk
    if (header.chunk_id != session->expected_chunk_id) {
        // New chunk
        session->recv_bitmap = 0;
        session->expected_chunk_id = header.chunk_id;
        session->expected_seq_num = 0;
        session->state = rapidxfer::VirtualSession::State::RECEIVING;
    }

    // Check for duplicate packet
    uint64_t seq_bit = 1ULL << (header.seq_num % 64);
    if (session->recv_bitmap & seq_bit) {
        LOG(INFO) << "[Context] Duplicate packet seq=" << header.seq_num;
        // Still send SACK for duplicate
        sendSACK(peer_name, session_id, header.chunk_id, session->recv_bitmap, 0);
        return 0;
    }

    // Update receive bitmap
    session->recv_bitmap |= seq_bit;

    // Write payload to local buffer
    if (!session->local_buffers.empty() && !payload.empty()) {
        // Calculate buffer position
        size_t byte_offset = header.chunk_id * kChunkSize +
                            header.seq_num * kMaxDataPerPkt;

        // Find target buffer
        size_t buf_idx = 0;
        size_t buf_offset = 0;
        size_t remaining_offset = byte_offset;

        for (const auto& buf : session->local_buffers) {
            if (remaining_offset < buf.length) {
                buf_offset = remaining_offset;
                break;
            }
            remaining_offset -= buf.length;
            buf_idx++;
        }

        if (buf_idx < session->local_buffers.size()) {
            // Copy payload to buffer
            size_t copy_size = std::min(payload.size(),
                                        session->local_buffers[buf_idx].length - buf_offset);
            uint8_t* dst = static_cast<uint8_t*>(session->local_buffers[buf_idx].addr);
            memcpy(dst + buf_offset, payload.data(), copy_size);
        }
    }

    // Send SACK every few packets
    static constexpr uint16_t kSackInterval = 8;
    if (header.seq_num % kSackInterval == 0 || payload.size() < kMaxDataPerPkt) {
        sendSACK(peer_name, session_id, header.chunk_id, session->recv_bitmap, 0);
    }

    // Check for chunk completion
    // For now, assume chunk is complete if we've received packets covering the window
    uint32_t pkts_received = __builtin_popcountll(session->recv_bitmap);
    if (pkts_received >= kWindowPackets || payload.size() < kMaxDataPerPkt) {
        // Chunk complete, send Chunk-ACK
        sendChunkAck(peer_name, session_id, header.chunk_id,
                     pkts_received, session->recv_bitmap);

        session->state = rapidxfer::VirtualSession::State::COMPLETE;
        session->expected_chunk_id++;

        LOG(INFO) << "[Context] Chunk " << header.chunk_id << " complete";
    }

    return 0;
}

int Context::handleReadRequest(const std::string& peer_name,
                               const rapidxfer::RapidXferHeader& header,
                               const std::vector<uint8_t>& payload) {

    LOG(INFO) << "[Context] Read request from " << peer_name;

    // Deserialize payload
    const uint8_t* ptr = payload.data();
    uint32_t num_local, num_remote;

    memcpy(&num_local, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    std::vector<Buffer> local_targets(num_local);
    memcpy(local_targets.data(), ptr, sizeof(Buffer) * num_local);
    ptr += sizeof(Buffer) * num_local;

    memcpy(&num_remote, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    std::vector<Buffer> remote_sources(num_remote);
    memcpy(remote_sources.data(), ptr, sizeof(Buffer) * num_remote);

    // ========== At-most-once Deduplication ==========
    // Compute hash of buffers for deduplication
    size_t buffers_hash = 0;
    for (const auto& buf : local_targets) {
        buffers_hash ^= std::hash<void*>()(buf.addr);
        buffers_hash ^= std::hash<size_t>()(buf.length);
    }
    for (const auto& buf : remote_sources) {
        buffers_hash ^= std::hash<void*>()(buf.addr);
        buffers_hash ^= std::hash<size_t>()(buf.length);
    }

    DedupKey dedup_key{peer_name, buffers_hash};

    // Check if this is a duplicate request
    auto it = dedup_cache_.find(dedup_key);
    if (it != dedup_cache_.end()) {
        // Duplicate request detected
        uint64_t now = GetCurrentTS();
        if (now - it->second.timestamp < kDedupTTLUs) {
            // Within TTL, this is a duplicate
            if (it->second.completed) {
                LOG(INFO) << "[Context] Duplicate read request (already completed), task_id="
                      << it->second.task_id;
                // Send notification that task is already complete
                // The duplicate request will be ignored, data already sent
            } else {
                LOG(INFO) << "[Context] Duplicate read request (in progress), task_id="
                      << it->second.task_id;
            }
            return 0;  // Skip processing
        } else {
            // TTL expired, remove old entry
            dedup_cache_.erase(it);
        }
    }

    // Call read callback to let upper layer prepare data
    if (read_callback_) {
        TaskID task_id = read_callback_(peer_name, local_targets, remote_sources);
        LOG(INFO) << "[Context] Read request assigned task_id=" << task_id;

        // Add to deduplication cache
        DedupEntry entry{task_id, GetCurrentTS(), false};
        dedup_cache_[dedup_key] = entry;

        // Send data back to peer (reverse write)
        // The read_callback should have populated local_targets with data
        // Now we send it as a write operation to the peer

        // Get or create session for the peer
        int session_id = scheduler_->getOrCreateSession(peer_name);
        auto* session = scheduler_->getSession(session_id);
        if (!session) {
            LOG(ERROR) << "[Context] Failed to get session for read response";
            return -1;
        }

        // Store the target buffers (where peer wants data)
        session->remote_buffers = local_targets;

        // Start sending data (reverse write)
        // Use same chunk mechanism as write operation
        startWrite(peer_name, local_targets, remote_sources);

        LOG(INFO) << "[Context] Sending " << local_targets.size()
                  << " buffers back to " << peer_name;
    } else {
        LOG(WARNING) << "[Context] No read callback registered";
    }

    return 0;
}

int Context::handleSACK(const std::string& peer_name,
                       const rapidxfer::RapidXferHeader& header,
                       const std::vector<uint8_t>& payload) {

    if (payload.size() < sizeof(rapidxfer::SACKPayload)) {
        LOG(ERROR) << "[Context] Invalid SACK payload size";
        return -1;
    }

    rapidxfer::SACKPayload sack;
    memcpy(&sack, payload.data(), sizeof(rapidxfer::SACKPayload));

    LOG(INFO) << "[Context] SACK from " << peer_name
              << ", chunk=" << header.chunk_id
              << ", bitmap=0x" << std::hex << sack.recv_bitmap << std::dec;

    // Get session
    auto* session = scheduler_->getSession(header.session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Session not found for SACK";
        return -1;
    }

    // Calculate RTT sample (if this is the first ACK for a packet)
    uint64_t now = GetCurrentTS();
    if (header.timestamp > 0 && now > header.timestamp) {
        uint64_t rtt_sample = now - header.timestamp;

        // Update SRTT and RTTVAR using RFC 6298
        if (recv_srtt_ == 0) {
            // First measurement
            recv_srtt_ = rtt_sample;
            recv_rttval_ = rtt_sample / 2;
        } else {
            uint64_t rttvar_diff = (recv_srtt_ > rtt_sample) ?
                                   (recv_srtt_ - rtt_sample) : (rtt_sample - recv_srtt_);
            recv_rttval_ = (3 * recv_rttval_ + rttvar_diff) / 4;
            recv_srtt_ = (7 * recv_srtt_ + rtt_sample) / 8;
        }

        // Update RTO
        recv_rto_ = std::max(recv_srtt_ + 4 * recv_rttval_, kMinRTO);
        recv_rto_ = std::min(recv_rto_, kMaxRTO);
    }

    // Update send bitmap
    uint64_t old_send_bitmap = session->send_bitmap;
    uint64_t newly_acked = sack.recv_bitmap & ~old_send_bitmap;
    session->send_bitmap = sack.recv_bitmap;
    session->last_active_ts = now;

    // Count ACKed packets
    uint32_t acked_count = __builtin_popcountll(session->send_bitmap);

    // Check for lost packets and trigger retransmission if needed
    if (newly_acked != 0 && newly_acked != sack.recv_bitmap) {
        // Some packets not ACKed - trigger fast retransmit
        uint16_t window_base = sack.bitmap_start;
        int missing_count = 0;

        for (uint16_t i = 0; i < kWindowPackets; ++i) {
            if (!(session->send_bitmap & (1ULL << i))) {
                missing_count++;
                LOG(INFO) << "[Context] Packet seq=" << (window_base + i) << " not ACKed";
            }
        }

        // Fast retransmit: if 3+ packets missing, retransmit immediately
        if (missing_count >= 3) {
            LOG(INFO) << "[Context] Fast retransmit triggered for " << missing_count << " packets";
            retransmitPackets(header.session_id, header.chunk_id);
        }
    }

    // Update bytes acked
    size_t bytes_per_pkt = kMaxDataPerPkt;  // Approximate
    session->bytes_acked = acked_count * bytes_per_pkt;

    // Check if all packets in chunk are ACKed
    if (acked_count >= session->total_pkts_in_chunk) {
        LOG(INFO) << "[Context] All packets ACKed for chunk " << header.chunk_id;

        // Calculate total bytes to send
        size_t total_bytes = 0;
        for (const auto& buf : session->local_buffers) {
            total_bytes += buf.length;
        }

        // Check if there are more chunks to send
        size_t bytes_in_next_chunks = (header.chunk_id + 1) * kChunkSize;
        if (bytes_in_next_chunks < total_bytes) {
            // Move to next chunk
            session->next_chunk_id = header.chunk_id + 1;
            session->next_seq_num = 0;
            session->send_bitmap = 0;

            LOG(INFO) << "[Context] Proceeding to chunk " << session->next_chunk_id;

            // Send next chunk
            sendChunkPackets(header.session_id, session->next_chunk_id);
        } else {
            // All data sent
            session->state = rapidxfer::VirtualSession::State::COMPLETE;
            LOG(INFO) << "[Context] All data sent for task " << session->task_id;

            // Send Chunk-ACK to confirm completion
            sendChunkAck(session->peer_addr, header.session_id, header.chunk_id,
                        session->total_pkts_in_chunk, session->send_bitmap);
        }
    }

    return 0;
}

int Context::handleChunkAck(const std::string& peer_name,
                          const rapidxfer::RapidXferHeader& header,
                          const std::vector<uint8_t>& payload) {

    if (payload.size() < sizeof(rapidxfer::ChunkAckPayload)) {
        LOG(ERROR) << "[Context] Invalid Chunk-ACK payload size";
        return -1;
    }

    rapidxfer::ChunkAckPayload ack;
    memcpy(&ack, payload.data(), sizeof(rapidxfer::ChunkAckPayload));

    LOG(INFO) << "[Context] Chunk-ACK from " << peer_name
              << ", chunk=" << header.chunk_id
              << ", total_pkts=" << ack.total_pkts;

    // Get session
    auto* session = scheduler_->getSession(header.session_id);
    if (!session) {
        LOG(ERROR) << "[Context] Session not found for Chunk-ACK";
        return -1;
    }

    // Mark chunk as complete
    session->send_bitmap = ack.recv_bitmap;
    session->bytes_acked = ack.total_pkts * kMaxDataPerPkt;
    session->next_chunk_id = header.chunk_id + 1;
    session->next_seq_num = 0;  // Reset for next chunk
    session->state = rapidxfer::VirtualSession::State::COMPLETE;

    // Update task result if this is the last chunk
    size_t total_bytes = 0;
    for (const auto& buf : session->local_buffers) {
        total_bytes += buf.length;
    }
    size_t bytes_in_chunk = header.chunk_id * kChunkSize;

    if (bytes_in_chunk >= total_bytes) {
        // All data transferred
        LOG(INFO) << "[Context] Transfer complete for task " << session->task_id;
        session->state = rapidxfer::VirtualSession::State::COMPLETE;

        // Release QP if idle
        if (session->bound_qp_id >= 0) {
            scheduler_->releaseQP(header.session_id);
        }
    }

    return 0;
}

int Context::handleNotification(const std::string& peer_name,
                               const rapidxfer::RapidXferHeader& header,
                               const std::vector<uint8_t>& payload) {

    if (payload.size() < sizeof(rapidxfer::NotificationPayload)) {
        LOG(ERROR) << "[Context] Invalid notification payload size";
        return -1;
    }

    rapidxfer::NotificationPayload notif;
    memcpy(&notif, payload.data(), sizeof(rapidxfer::NotificationPayload));

    std::string message(notif.message, notif.message_length);

    LOG(INFO) << "[Context] Notification from " << peer_name
              << ", task_id=" << notif.task_id
              << ", message=" << message;

    if (notification_callback_) {
        notification_callback_(peer_name, notif.task_id, message);
    }

    return 0;
}

// ========== Low-level Packet Processing ==========

int Context::pollCompletedPackets(int cq_index, uint64_t current_ts) {
    const static size_t kPollCount = 64;
    ibv_wc wc[kPollCount];
    int nr_poll = controller_.context().poll(kPollCount, wc, cq_index);

    if (nr_poll < 0) {
        LOG(ERROR) << "[Context] Failed to poll CQ";
        return -1;
    }

    for (int i = 0; i < nr_poll; ++i) {
        if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "[Context] Failed WC: " << ibv_wc_status_str(wc[i].status);
            continue;
        }

        processReceivedPacket(current_ts, wc[i]);
    }

    return nr_poll;
}

int Context::processReceivedPacket(uint64_t current_ts, ibv_wc& wc) {
    if (wc.opcode == IBV_WC_RECV) {
        PacketHandle handle;
        handle.setRawPacket((char*)wc.wr_id, true);
        handle.deserialize(wc.imm_data, wc.byte_len);

        ibv_grh* grh = (ibv_grh*)wc.wr_id;

        // Extract peer name from GID:QP
        std::string peer_name = extractPeerName(grh, wc.src_qp);

        // Deserialize header
        if (handle.getPayloadLength() < sizeof(rapidxfer::RapidXferHeader)) {
            LOG(ERROR) << "[Context] Packet too short for header";
            return -1;
        }

        rapidxfer::RapidXferHeader header;
        memcpy(&header, handle.getPayload(), sizeof(rapidxfer::RapidXferHeader));

        // Get payload (after header)
        const uint8_t* payload_start = (const uint8_t*)handle.getPayload() +
                                         sizeof(rapidxfer::RapidXferHeader);
        size_t payload_length = handle.getPayloadLength() -
                                  sizeof(rapidxfer::RapidXferHeader);

        std::vector<uint8_t> payload(payload_start, payload_start + payload_length);

        // Route to handler based on flags
        handlePacket(peer_name, header, payload);

        // Re-post recv WR
        submitNormalRecvWR(handle);
    }

    return 0;
}

int Context::submitNormalRecvWR(PacketHandle& handle) {
    // Get endpoint for receiving
    // We need to post to a shared receive queue or create one

    // For UD, we post receives to the endpoint store
    auto& endpoint_store = controller_.endpointStore();

    // Allocate a free slot in the receive queue
    // For now, use the packet buffer directly
    Request req;
    req.addr[0] = handle.getRawPacket();
    req.length[0] = mtu_size_;
    req.lkey[0] = local_arena_lkey_;

    std::vector<Request*> requests = {&req};

    // Post to any available QP in the store for receiving
    // The UD endpoint store will handle this
    int ret = endpoint_store.postReceiveRequest(requests, 0);
    if (ret < 0) {
        LOG(ERROR) << "[Context] Failed to post receive WR";
        return ret;
    }

    return 0;
}

int Context::sendDataPackets(uint64_t current_ts) {
    // Iterate through all active sessions and check for retransmissions
    // Also periodically cleanup dedup cache

    size_t retx_count = 0;
    static constexpr uint64_t kDedupCleanupIntervalUs = 10000000;  // 10 seconds
    static uint64_t last_dedup_cleanup = 0;

    // Periodic dedup cache cleanup
    if (current_ts - last_dedup_cleanup > kDedupCleanupIntervalUs) {
        size_t cleaned = 0;
        auto it = dedup_cache_.begin();
        while (it != dedup_cache_.end()) {
            if (current_ts - it->second.timestamp > kDedupTTLUs) {
                it = dedup_cache_.erase(it);
                cleaned++;
            } else {
                ++it;
            }
        }
        if (cleaned > 0) {
            LOG(INFO) << "[Context] Cleaned " << cleaned << " expired dedup entries";
        }
        last_dedup_cleanup = current_ts;
    }

    // Note: For efficient large-scale operation, we would maintain
    // a priority queue of sessions sorted by next retransmission time.
    // For now, this O(n) scan is acceptable for moderate session counts.

    // The actual retransmission is triggered by the timeout check
    // in runStep() which calls retransmitPackets for each session.

    return retx_count;
}

// ========== TCP Bootstrap ==========

// ========== GID:QP to Peer Mapping ==========

std::string Context::extractPeerName(ibv_grh* grh, uint32_t src_qp) {
    char buf[8] = {0};
    std::string gid_str;
    for (size_t i = 0; i < 16; ++i) {
        sprintf(buf, "%02x", grh->sgid.raw[i]);
        gid_str += i == 0 ? buf : std::string(":") + buf;
    }
    return gid_str + ":" + std::to_string(src_qp);
}

// ========== TCP Bootstrap ==========

int Context::startBootstrapListener(const std::string& tcp_address) {
    // Parse address (e.g., "0.0.0.0:12348")
    size_t colon_pos = tcp_address.find_last_of(':');
    if (colon_pos == std::string::npos) {
        LOG(ERROR) << "[Context] Invalid TCP address format: " << tcp_address;
        return -1;
    }

    std::string host = tcp_address.substr(0, colon_pos);
    std::string port_str = tcp_address.substr(colon_pos + 1);
    int port = std::stoi(port_str);

    // Create listening socket
    tcp_listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_listen_fd_ < 0) {
        LOG(ERROR) << "[Context] Failed to create TCP socket";
        return -1;
    }

    // Set SO_REUSEADDR
    int opt = 1;
    setsockopt(tcp_listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(tcp_listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "[Context] Failed to bind TCP socket";
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
        return -1;
    }

    // Listen
    if (listen(tcp_listen_fd_, 16) < 0) {
        LOG(ERROR) << "[Context] Failed to listen on TCP socket";
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
        return -1;
    }

    tcp_listen_address_ = tcp_address;
    tcp_listener_running_ = true;

    // Start accept thread
    tcp_accept_thread_ = std::thread(&Context::bootstrapAcceptThread, this);

    LOG(INFO) << "[Context] TCP Bootstrap listening on " << tcp_address;

    return 0;
}

int Context::stopBootstrapListener() {
    tcp_listener_running_ = false;

    if (tcp_listen_fd_ >= 0) {
        shutdown(tcp_listen_fd_, SHUT_RDWR);
        close(tcp_listen_fd_);
        tcp_listen_fd_ = -1;
    }

    if (tcp_accept_thread_.joinable()) {
        tcp_accept_thread_.join();
    }

    LOG(INFO) << "[Context] TCP Bootstrap stopped";

    return 0;
}

void Context::bootstrapAcceptThread() {
    while (tcp_listener_running_) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int client_fd = accept(tcp_listen_fd_, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (tcp_listener_running_) {
                LOG(ERROR) << "[Context] TCP accept failed";
            }
            break;
        }

        LOG(INFO) << "[Context] New TCP connection";

        // Handle in a detached thread or synchronously
        handleBootstrapConnection(client_fd);
    }
}

void Context::handleBootstrapConnection(int client_fd) {
    // Exchange UD connection information
    // 1. Receive peer's UD info
    // 2. Send local UD info
    // 3. Close TCP

    struct UDInfo {
        uint32_t lid;
        uint64_t gid[16];  // ibv_gid is 16 bytes
        uint32_t qp_num;
    } __attribute__((packed));

    // Get local UD info
    uint16_t local_lid = controller_.context().lid();
    std::string local_gid_str = controller_.context().gid();

    // Get local QP numbers from endpoint store
    auto& endpoint_store = controller_.endpointStore();
    auto qp_nums = endpoint_store.qpNum();

    if (qp_nums.empty()) {
        LOG(ERROR) << "[Context] No QPs available";
        close(client_fd);
        return;
    }

    // Receive peer's UD info first
    UDInfo peer_info;
    ssize_t ret = recv(client_fd, &peer_info, sizeof(peer_info), MSG_WAITALL);
    if (ret != sizeof(peer_info)) {
        LOG(ERROR) << "[Context] Failed to receive peer UD info";
        close(client_fd);
        return;
    }

    // Send local UD info
    UDInfo local_info;
    local_info.lid = local_lid;
    // Convert gid string to bytes
    memset(local_info.gid, 0, sizeof(local_info.gid));
    if (local_gid_str.length() <= 32) {
        memcpy(local_info.gid, local_gid_str.data(), local_gid_str.length());
    }
    local_info.qp_num = qp_nums[0];  // Send first QP number

    ret = send(client_fd, &local_info, sizeof(local_info), 0);
    if (ret != sizeof(local_info)) {
        LOG(ERROR) << "[Context] Failed to send local UD info";
        close(client_fd);
        return;
    }

    // Create peer address string
    char peer_gid_str[64];
    for (int i = 0; i < 16; ++i) {
        sprintf(&peer_gid_str[i * 2], "%02lx", (unsigned long)peer_info.gid[i]);
    }
    std::string peer_addr = std::string(peer_gid_str) + ":" + std::to_string(peer_info.qp_num);

    // Prepare connection attributes for peer
    Attributes peer_attrs;
    peer_attrs["lid"] = std::to_string(peer_info.lid);
    peer_attrs["gid"] = std::string(peer_gid_str, 32);
    peer_attrs["qp_num"] = std::to_string(peer_info.qp_num);

    // Setup UD connection
    int setup_ret = setupConnection(peer_addr, peer_attrs);
    if (setup_ret != 0) {
        LOG(ERROR) << "[Context] Failed to setup UD connection to " << peer_addr;
    } else {
        LOG(INFO) << "[Context] Established UD connection to " << peer_addr;
    }

    close(client_fd);
}

} // namespace rapid
