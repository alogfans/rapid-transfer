// Copyright 2024 Feng Ren

#ifndef PACKET_MANAGER_H_
#define PACKET_MANAGER_H_

#include <endian.h>
#include <linux/types.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stack>

#include "protocols/common/rdma_context.h"
#include "rapid_transfer.h"

#define PKT_CMD_DATA (0)
#define PKT_CMD_ACK (1)
#define SHORT_SN(x) (uint32_t((x)) & 0xffffff)

namespace rapid {
union PktHdrImm {
    uint32_t raw;
    struct {
        uint8_t session : 6;
        uint8_t cmd : 1;
        uint8_t compacted : 1;
        uint8_t sn[3];
    };
};  // 4 bytes

struct PktHdr {
    // PktHdrImm hdr_imm;
    uint16_t wnd;
    uint16_t ts_lo;
    uint32_t ts_hi;
    uint64_t remote_addr;  // Target address on receiver (0 = normal mode)
};  // 16 bytes

class PacketHandle {
   public:
    friend class PacketBufferPool;

    PacketHandle();

    int setRawPacket(void* packet_buf, bool with_grh = false);

    void* getRawPacket() { return packet_buf; }

    int setPayload(void* data, size_t length, bool do_copy = false);

    void* getPayload();

    uint32_t getPayloadLength() { return data_len; }

    int deserialize(uint32_t imm_data, uint32_t packet_length);

    int serialize(Buffer* slices, uint32_t& imm_data);

    int serialize(std::vector<Buffer>& slices, uint32_t& imm_data);

   public:
    uint8_t session;
    uint8_t cmd;
    uint16_t wnd;
    uint32_t sn;
    uint64_t ts;
    bool compacted;
    bool inflight;
    uint64_t remote_addr{0};

   private:
    int encode();
    int decode();

    PktHdrImm pkt_hdr_imm;
    bool with_grh;
    void* packet_buf;
    uint32_t data_len;
    void* data_buf;
};

class PacketBufferPool {
   public:
    PacketBufferPool(size_t mtu_size, size_t max_packets);

    ~PacketBufferPool();

    PacketBufferPool(const PacketBufferPool&) = delete;
    PacketBufferPool& operator=(const PacketBufferPool&) = delete;

    int construct(const std::string& device_name = "");

    int deconstruct();

    void* getArena() const { return arena_; }

    size_t getCapacity() const { return mtu_size_ * max_packets_; }

    int allocatePacket(PacketHandle& handle, bool with_grh = false);

    int freePacket(PacketHandle& handle);

   private:
    const size_t mtu_size_, max_packets_;
    void *arena_, *global_free_buffer_;
    RWSpinlock arena_lock_;
};

struct Fragment {
    void* local_addr;
    void* remote_addr;
    size_t length;
};

struct SecondaryQueue {
    SecondaryQueue(size_t fragment_size)
        : local_offset_(0),
          remote_offset_(0),
          fragment_id_(0),
          fragment_size_(fragment_size) {}

    bool hasRemainingFragment() const { return !local_slice_list_.empty(); }

    Fragment popFragment() {
        if (local_slice_list_.empty()) return {nullptr, nullptr, 0};

        auto& l_slice = local_slice_list_[0];
        size_t l_rem = l_slice.length - local_offset_;

        uint64_t r_addr_val = 0;
        size_t r_rem = SIZE_MAX;
        bool has_remote = !remote_slice_list_.empty();
        if (has_remote) {
            auto& r_slice = remote_slice_list_[0];
            r_addr_val = reinterpret_cast<uint64_t>(r_slice.addr);
            r_rem = r_slice.length - remote_offset_;
        }

        size_t length = std::min({fragment_size_, l_rem, r_rem});

        void* l_addr = static_cast<char*>(l_slice.addr) + local_offset_;
        void* r_addr =
            has_remote ? reinterpret_cast<void*>(r_addr_val + remote_offset_)
                       : nullptr;

        if (local_offset_ + length == l_slice.length) {
            local_slice_list_.erase(local_slice_list_.begin());
            local_offset_ = 0;
        } else {
            local_offset_ += length;
        }

        if (has_remote) {
            if (remote_offset_ + length == remote_slice_list_[0].length) {
                remote_slice_list_.erase(remote_slice_list_.begin());
                remote_offset_ = 0;
            } else {
                remote_offset_ += length;
            }
        }

        return {l_addr, r_addr, length};
    }

    std::pair<uint64_t, uint64_t> push(
        const std::vector<Buffer>& local_buffers,
        const std::vector<Buffer>& remote_buffers = {}) {
        auto start_fragment_id = fragment_id_;

        for (const auto& b : local_buffers) {
            local_slice_list_.push_back(b);
            fragment_id_ += (b.length + fragment_size_ - 1) / fragment_size_;
        }

        for (const auto& b : remote_buffers) {
            remote_slice_list_.push_back(b);
        }

        return {start_fragment_id, fragment_id_};
    }

    uint64_t fragment_id() const { return fragment_id_; }

   private:
    std::vector<Buffer> local_slice_list_;
    std::vector<Buffer> remote_slice_list_;
    size_t local_offset_;
    size_t remote_offset_;
    uint64_t fragment_id_;
    const size_t fragment_size_;
};

class SendQueue {
   public:
    SendQueue(size_t mtu_size, size_t queue_capacity, size_t wnd_size,
              PacketBufferPool& pool, uint8_t session);

    ~SendQueue();

    int push(const std::vector<Buffer>& slice_list, uint32_t& last_sn);

    int push(const std::vector<Buffer>& slice_list, uint32_t& last_sn,
             const std::vector<Buffer>& remote_targets);

    int markCompleted(uint32_t ack_sn);

    uint32_t getNextSN() const { return SHORT_SN(head_); }

    uint32_t getAckSN() const { return SHORT_SN(tail_); }

    int getIndexRange(uint64_t& head, uint64_t& tail);

    int forEach(std::function<int(PacketHandle&)> func);

    PacketHandle& getMutableEntry(uint64_t index) {
        return handle_[index % queue_capacity_];
    }

    void setWndSize(uint16_t wnd_size) {
        wnd_size_ = std::min(wnd_size, (uint16_t)queue_capacity_);
    }

    uint16_t getWndSize() const { return wnd_size_; }

    uint8_t getSessionID() const { return session_; }

   private:
    int fillPrimaryQueue();

   private:
    const size_t mtu_size_, queue_capacity_;
    const uint8_t session_;
    std::atomic<uint64_t> head_, tail_;
    std::atomic<uint16_t> wnd_size_;
    std::vector<PacketHandle> handle_;
    PacketBufferPool& pool_;
    SecondaryQueue secondary_queue_;
    RWSpinlock queue_lock_;
};

class McastSendQueue {
   public:
    McastSendQueue(size_t mtu_size, size_t queue_capacity, size_t wnd_size,
                   PacketBufferPool& pool, uint8_t session, size_t replica_num);

    ~McastSendQueue();

    int push(const std::vector<Buffer>& slice_list, uint32_t& last_sn);

    int markCompleted(int index, uint32_t ack_sn);

    uint32_t getNextSN() const { return SHORT_SN(head_); }

    uint32_t getAckSN(int index = -1) const;

    int getIndexRange(uint64_t& head, uint64_t& tail);

    int forEach(std::function<int(PacketHandle&)> func);

    PacketHandle& getMutableEntry(uint64_t index) {
        return handle_[index % queue_capacity_];
    }

    void setWndSize(uint16_t wnd_size) {
        wnd_size_ = std::min(wnd_size, (uint16_t)queue_capacity_);
    }

    uint16_t getWndSize() const { return wnd_size_; }

    uint8_t getSessionID() const { return session_; }

   private:
    int fillPrimaryQueue();

    uint64_t getMinTailIndex() const;

   private:
    const size_t mtu_size_, queue_capacity_;
    const uint8_t session_;
    const size_t replica_num_;
    uint64_t head_;
    std::vector<uint64_t> tail_list_;
    uint16_t wnd_size_;
    std::vector<PacketHandle> handle_;
    PacketBufferPool& pool_;
    SecondaryQueue secondary_queue_;
    RWSpinlock queue_lock_;
};

class ReceiveQueue {
   public:
    ReceiveQueue(size_t mtu_size, size_t queue_capacity, size_t wnd_size,
                 uint8_t session);

    int push(const std::vector<Buffer>& slice_list, uint32_t& last_sn);

    int markCompleted(PacketHandle& handle);

    uint32_t getNextSN() const { return SHORT_SN(head_); }

    uint32_t getAckSN() const { return SHORT_SN(tail_); }

    uint64_t getLastTS() const { return last_packet_ts_; };

    int getIndexRange(uint64_t& head, uint64_t& tail);

    void setWndSize(uint16_t wnd_size) {
        wnd_size_ = std::min(wnd_size, (uint16_t)queue_capacity_);
    }

    uint16_t getWndSize() const { return wnd_size_; }

    uint16_t getAvailableWndSize() const;

    uint8_t getSessionID() const { return session_; }

   private:
    int fillPrimaryQueue();

    struct Request {
        void* addr;
        size_t length;
        bool inflight;
    };

    const size_t mtu_size_, queue_capacity_;
    const uint8_t session_;
    std::atomic<uint64_t> head_, tail_;
    std::atomic<uint16_t> wnd_size_;
    std::vector<Request> requests_;
    SecondaryQueue secondary_queue_;
    uint64_t last_packet_ts_;
    RWSpinlock queue_lock_;
};

// Direct write queue for tracking received packets in direct write mode
// Similar to ReceiveQueue but without push() semantics (no pre-posted buffers)
// Data is copied directly to remote_addr, we only track sequence numbers for
// ACK
class DirectWriteQueue {
   public:
    DirectWriteQueue() : tail_(0), last_packet_ts_(0) {}

    // Mark a packet as received and copy data to remote_addr
    // Similar to ReceiveQueue::markCompleted but data goes to
    // handle.remote_addr
    int markCompleted(PacketHandle& handle) {
        // Check if sequence number is within receive window
        auto wnd_start = SHORT_SN(tail_);
        auto wnd_end = SHORT_SN(tail_ + kMaxWndSize);
        uint32_t sn = handle.sn;

        if (wnd_start <= wnd_end) {
            if (sn < wnd_start || sn >= wnd_end) return 0;
        } else {
            if (sn < wnd_start && sn >= wnd_end) return 0;
        }

        // Copy payload to remote_addr (direct write)
        if (handle.remote_addr != 0) {
            const char* payload = static_cast<const char*>(handle.getPayload());
            size_t payload_len = handle.getPayloadLength();
            memcpy(reinterpret_cast<void*>(handle.remote_addr), payload,
                   payload_len);
        }

        // Advance tail to received sequence number (optimistic ACK)
        tail_ = sn;
        last_packet_ts_ = handle.ts;
        return 0;
    }

    // Get next sequence number we expect (not used, for compatibility)
    uint32_t getNextSN() const { return SHORT_SN(tail_ + 1); }

    // Get ACK sequence number (last received + 1)
    uint32_t getAckSN() const { return SHORT_SN(tail_ + 1); }

    // Get last received packet timestamp
    uint64_t getLastTS() const { return last_packet_ts_; }

    // Get index range (for ACK window calculation)
    int getIndexRange(uint64_t& head, uint64_t& tail) {
        head = tail_ + 1;  // Next expected
        tail = tail_;      // Last received
        return 0;
    }

   private:
    static const size_t kMaxWndSize = 64;
    uint64_t tail_;            // Last received sequence number
    uint64_t last_packet_ts_;  // Last received packet timestamp
};

class PacketManager {
   public:
    const static size_t kWndSize = 64;

    PacketManager(size_t mtu_size, size_t max_packets, size_t queue_capacity,
                  size_t wnd_size = kWndSize);

    ~PacketManager();

    PacketManager(const PacketManager&) = delete;
    PacketManager& operator=(const PacketManager&) = delete;

    int construct(const std::string& device_name = "");

    int deconstruct();

    PacketBufferPool& getPool() { return pool_; }

    SendQueue& getSendQueue(int sid);

    ReceiveQueue& getReceiveQueue(int sid);

    McastSendQueue& getMcastSendQueue(int sid);

    size_t mtuSize() const { return mtu_size_; }

    int setMulticastReplicaNum(int group_id, size_t replica_num);

   private:
    const size_t mtu_size_, max_packets_, queue_capacity_, wnd_size_;
    RWSpinlock queue_lock_;
    PacketBufferPool pool_;
    std::unordered_map<int, SendQueue*> send_queue_;
    std::unordered_map<int, ReceiveQueue*> receive_queue_;
    std::unordered_map<int, McastSendQueue*> mcast_send_queue_;
    std::unordered_map<int, int> mcast_replica_num_;
};

}  // namespace rapid

#endif  // PACKET_MANAGER_H_
