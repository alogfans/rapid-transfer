// context.h
//
// RapidXfer Context - Unified Message Processing
// All messages (data, read request, ACK) are unified packets
//
// Copyright (C) 2026 RapidXfer Team

#ifndef CONTEXT_H_
#define CONTEXT_H_

#include <atomic>
#include <functional>
#include <unordered_map>
#include <vector>

#include "concurrency.h"
#include "controller.h"
#include "packet_manager.h"
#include "rapid_transfer_protocol.h"
#include "scheduler.h"
#include "rapid_transfer.h"

namespace rapid {

class Context {
public:
    // ========== Callback Types ==========

    // Callback for handling read requests from peers
    using ReadCallback = std::function<TaskID(
        const std::string& peer_name,
        const std::vector<Buffer>& local_targets,
        const std::vector<Buffer>& remote_sources)>;

    // Callback for receiving notifications
    using NotificationCallback = std::function<void(
        const std::string& peer_name,
        TaskID task_id,
        const std::string& message)>;

    // Callback for transfer completion
    using TransferCompleteCallback = std::function<void(
        TaskID task_id,
        const std::string& peer_name,
        Status status)>;

    // ========== Lifecycle ==========

    Context(size_t mtu_size, size_t max_packets, size_t queue_capacity);
    ~Context();

    int construct(const std::string& device_name, uint8_t rdma_port, int gid_index);
    int deconstruct();

    // ========== Connection Management ==========

    int prepareConnection(const std::string& peer_addr, Attributes& local);
    int setupConnection(const std::string& peer_addr, const Attributes& peer);

    // ========== Memory Registration ==========

    int registerLocalMemory(void* addr, size_t length);
    int unregisterLocalMemory(void* addr);

    // ========== Unified Message Sending ==========

    // Send data packet (flags = DATA_PACKET)
    int sendDataPacket(const std::string& peer_name,
                       const rapid::v1::RapidTransferHeader& header,
                       const std::vector<uint8_t>& payload);

    // Send read request (flags = READ_REQUEST)
    int sendReadRequest(const std::string& peer_name,
                        const std::vector<Buffer>& local_targets,
                        const std::vector<Buffer>& remote_sources);

    // Send SACK (flags = SACK)
    int sendSACK(const std::string& peer_name,
                 uint32_t session_id, uint32_t chunk_id,
                 uint64_t recv_bitmap, uint16_t bitmap_start);

    // Send Chunk-ACK (flags = CHUNK_ACK)
    int sendChunkAck(const std::string& peer_name,
                     uint32_t session_id, uint32_t chunk_id,
                     uint32_t total_pkts, uint64_t recv_bitmap);

    // Send Notification (flags = NOTIFICATION)
    int sendNotification(const std::string& peer_name,
                        TaskID task_id,
                        const std::string& message);

    // ========== Write Operations ==========

    // Start a write operation (send data to peer)
    TaskID startWrite(const std::string& peer_name,
                      const std::vector<Buffer>& local_buffers,
                      const std::vector<Buffer>& remote_buffers);

    // ========== Read Operations ==========

    // Start a read operation (pull data from peer)
    // Sends a Read Request packet and waits for peer to respond with data
    TaskID startRead(const std::string& peer_name,
                     const std::vector<Buffer>& local_targets,
                     const std::vector<Buffer>& remote_sources);

    // Send next batch of packets for a chunk
    int sendChunkPackets(int session_id, uint32_t chunk_id);

    // Check and retransmit lost packets
    int retransmitPackets(int session_id, uint32_t chunk_id);

    // ========== Progress Engine ==========

    int runStep();

    // ========== Callback Registration ==========

    void setReadCallback(ReadCallback callback) { read_callback_ = std::move(callback); }
    void setNotificationCallback(NotificationCallback callback) {
        notification_callback_ = std::move(callback);
    }
    void setTransferCompleteCallback(TransferCompleteCallback callback) {
        transfer_complete_callback_ = std::move(callback);
    }

    // ========== Buffer Info ==========

    // Buffer info structure for sharing buffer information between peers
    struct BufferInfo {
        uint64_t addr;   // Buffer address as uint64_t for safe serialization
        uint64_t length; // Buffer size
        uint32_t rkey;   // Remote key
    };

    void setBufferInfo(const BufferInfo& info) {
        std::lock_guard<std::mutex> lock(buffer_info_mutex_);
        local_buffer_info_ = info;
    }

    std::optional<BufferInfo> getBufferInfo() const {
        std::lock_guard<std::mutex> lock(buffer_info_mutex_);
        return local_buffer_info_;
    }

    // ========== Scheduler Access ==========

    rapid::v1::Scheduler* getScheduler() { return scheduler_.get(); }

    // ========== RDMA Context Access ==========
    // Accessor methods for TcpBootstrap server integration

    uint16_t getLid() { return controller_.context().lid(); }
    std::string getGid() { return controller_.context().gid(); }
    auto& endpointStore() { return controller_.endpointStore(); }

private:
    // ========== Unified Message Handling ==========

    int handlePacket(const std::string& peer_name,
                     const rapid::v1::RapidTransferHeader& header,
                     const std::vector<uint8_t>& payload);

    int handleDataPacket(const std::string& peer_name,
                         const rapid::v1::RapidTransferHeader& header,
                         const std::vector<uint8_t>& payload);

    int handleReadRequest(const std::string& peer_name,
                          const rapid::v1::RapidTransferHeader& header,
                          const std::vector<uint8_t>& payload);

    int handleSACK(const std::string& peer_name,
                   const rapid::v1::RapidTransferHeader& header,
                   const std::vector<uint8_t>& payload);

    int handleChunkAck(const std::string& peer_name,
                      const rapid::v1::RapidTransferHeader& header,
                      const std::vector<uint8_t>& payload);

    int handleNotification(const std::string& peer_name,
                          const rapid::v1::RapidTransferHeader& header,
                          const std::vector<uint8_t>& payload);

    // ========== Low-level Packet Processing ==========

    int pollCompletedPackets(int cq_index, uint64_t current_ts);
    int processReceivedPacket(uint64_t current_ts, ibv_wc& wc);
    int submitNormalRecvWR(PacketHandle& handle);
    int sendDataPackets(uint64_t current_ts);

    // ========== GID:QP to Peer Mapping ==========

    std::string extractPeerName(ibv_grh* grh, uint32_t src_qp);

private:
    // ========== Configuration ==========
    const size_t mtu_size_;
    static constexpr size_t kChunkSize = 4 * 1024 * 1024;  // 4MB per chunk
    static constexpr size_t kMaxDataPerPkt = 4096 - 24;     // MTU - header
    static constexpr uint16_t kWindowPackets = 64;          // Send window size
    static constexpr uint64_t kRetxTimeoutUs = 100;         // Retransmission timeout (100us)

    // ========== Core Components ==========
    Controller controller_;
    PacketManager packet_manager_;
    std::unique_ptr<rapid::v1::Scheduler> scheduler_;

    // ========== Memory Region ==========
    uint32_t local_arena_lkey_;  // LKey for arena memory

    // ========== Task Tracking ==========
    struct Task {
        int session;
        uint32_t last_sn;
        std::vector<Buffer> local_targets;
        std::vector<Buffer> remote_sources;
    };
    std::unordered_map<TaskID, Task> task_map_;
    std::atomic<TaskID> next_task_id_;

    // GID:QP to peer address mapping (for routing incoming UD packets)
    std::unordered_map<std::string, std::string> gid_qp_to_peer_map_;

    // ========== At-most-once Deduplication ==========
    struct DedupKey {
        std::string peer_name;
        size_t buffers_hash;  // Hash of local_targets + remote_sources

        bool operator==(const DedupKey& other) const {
            return peer_name == other.peer_name && buffers_hash == other.buffers_hash;
        }
    };
    struct DedupKeyHash {
        size_t operator()(const DedupKey& key) const {
            return std::hash<std::string>()(key.peer_name) ^ key.buffers_hash;
        }
    };
    struct DedupEntry {
        TaskID task_id;
        uint64_t timestamp;
        bool completed;
    };
    std::unordered_map<DedupKey, DedupEntry, DedupKeyHash> dedup_cache_;
    static constexpr uint64_t kDedupTTLUs = 60000000;  // 60 seconds TTL

    // ========== Callbacks ==========
    ReadCallback read_callback_;
    NotificationCallback notification_callback_;
    TransferCompleteCallback transfer_complete_callback_;

    // ========== Buffer Info ==========
    std::optional<BufferInfo> local_buffer_info_;
    mutable std::mutex buffer_info_mutex_;

    // ========== Statistics ==========
    static constexpr uint64_t kMinRTO = 100;  // 100us
    static constexpr uint64_t kMaxRTO = 10000; // 10ms
    uint64_t recv_srtt_{0};
    uint64_t recv_rttval_{0};
    uint64_t recv_rto_{kMinRTO};
};

} // namespace rapid

#endif  // CONTEXT_H_
