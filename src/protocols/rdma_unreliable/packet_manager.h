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
    PktHdrImm hdr_imm;
    uint16_t wnd;
    uint16_t ts_lo;
    uint32_t ts_hi;
};  // 12 bytes

class PacketHandle {
   public:
    friend class PacketBufferPool;

    PacketHandle();

    int setRawPacket(void *packet_buf, bool with_grh = false);

    void *getRawPacket() { return packet_buf; }

    int setPayload(void *data, size_t length, bool do_copy = false);

    void *getPayload();

    uint32_t getPayloadLength() { return data_len; }

    int deserialize(uint32_t imm_data, uint32_t packet_length);

    int serialize(std::vector<Buffer> &slices, uint32_t &imm_data);

   public:
    uint8_t session;
    uint8_t cmd;
    uint16_t wnd;
    uint32_t sn;
    uint64_t ts;
    bool compacted;
    bool inflight;

   private:
    int encode();
    int decode();

    PktHdrImm pkt_hdr_imm;
    bool with_grh;
    void *packet_buf;
    uint32_t data_len;
    void *data_buf;
};

class PacketBufferPool {
   public:
    PacketBufferPool(size_t mtu_size, size_t max_packets);

    ~PacketBufferPool();

    PacketBufferPool(const PacketBufferPool &) = delete;
    PacketBufferPool &operator=(const PacketBufferPool &) = delete;

    int construct();

    int deconstruct();

    void *getArena() const { return arena_; }

    size_t getCapacity() const { return mtu_size_ * max_packets_; }

    int allocatePacket(PacketHandle &handle, bool with_grh = false);

    int freePacket(PacketHandle &handle);

   private:
    const size_t mtu_size_, max_packets_;
    void *arena_, *global_free_buffer_;
    RWSpinlock arena_lock_;
};

struct SecondaryQueue {
    SecondaryQueue(size_t fragment_size)
        : offset_(0), fragment_id_(0), fragment_size_(fragment_size) {}

    bool hasRemainingFragment() { return !slice_list_.empty(); }

    Buffer popFragment() {
        if (slice_list_.empty()) return {nullptr, 0};
        auto &slice = slice_list_[0];
        auto addr = static_cast<char *>(slice.addr) + offset_;
        auto length = std::min(fragment_size_, slice.length - offset_);
        if (offset_ + length == slice.length) {
            slice_list_.erase(slice_list_.begin());
            offset_ = 0;
        } else
            offset_ += length;
        return {addr, length};
    }

    std::pair<uint64_t, uint64_t> push(const std::vector<Buffer> &slice_list) {
        auto start_fragment_id = fragment_id_;
        for (auto &entry : slice_list) {
            slice_list_.push_back(entry);
            fragment_id_ +=
                (entry.length + fragment_size_ - 1) / fragment_size_;
        }
        auto end_fragment_id = fragment_id_;
        return {start_fragment_id, end_fragment_id};
    }

    uint64_t fragment_id() const { return fragment_id_; }

   private:
    std::vector<Buffer> slice_list_;
    size_t offset_;
    uint64_t fragment_id_;
    const size_t fragment_size_;
};

class SendQueue {
   public:
    SendQueue(size_t mtu_size, size_t queue_capacity, size_t wnd_size,
              PacketBufferPool &pool, uint8_t session);

    ~SendQueue();

    int push(const std::vector<Buffer> &slice_list, uint32_t &last_sn);

    int markCompleted(uint32_t ack_sn);

    uint32_t getNextSN() const { return SHORT_SN(head_); }

    uint32_t getAckSN() const { return SHORT_SN(tail_); }

    int getIndexRange(uint64_t &head, uint64_t &tail);

    int forEach(std::function<int(PacketHandle &)> func);

    void setWndSize(uint16_t wnd_size) {
        wnd_size_ = std::min(wnd_size, (uint16_t)queue_capacity_);
    }

    uint16_t getWndSize() const { return wnd_size_; }

   private:
    int fillPrimaryQueue();

   private:
    const size_t mtu_size_, queue_capacity_;
    const uint8_t session_;
    std::atomic<uint64_t> head_, tail_;
    std::atomic<uint16_t> wnd_size_;
    std::vector<PacketHandle> handle_;
    PacketBufferPool &pool_;
    SecondaryQueue secondary_queue_;
    RWSpinlock queue_lock_;
};

class ReceiveQueue {
   public:
    ReceiveQueue(size_t mtu_size, size_t queue_capacity, size_t wnd_size,
                 uint8_t session);

    int push(const std::vector<Buffer> &slice_list, uint32_t &last_sn);

    int markCompleted(PacketHandle &handle);

    uint32_t getNextSN() const { return SHORT_SN(head_); }

    uint32_t getAckSN() const { return SHORT_SN(tail_); }

    uint64_t getLastTS() const { return last_packet_ts_; };

    int getIndexRange(uint64_t &head, uint64_t &tail);

    void setWndSize(uint16_t wnd_size) {
        wnd_size_ = std::min(wnd_size, (uint16_t)queue_capacity_);
    }

    uint16_t getWndSize() const { return wnd_size_; }

   private:
    int fillPrimaryQueue();

    struct Request {
        void *addr;
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

class PacketManager {
   public:
    const static size_t kDefaultMTUSize = 4096;
    const static size_t kMaxPackets = 102400;
    const static size_t kQueueCapacity = 4096;
    const static size_t kWndSize = 256;

    PacketManager(size_t mtu_size = kDefaultMTUSize,
                  size_t max_packets = kMaxPackets,
                  size_t queue_capacity = kQueueCapacity,
                  size_t wnd_size = kWndSize);

    ~PacketManager();

    PacketManager(const PacketManager &) = delete;
    PacketManager &operator=(const PacketManager &) = delete;

    int construct();

    int deconstruct();

    PacketBufferPool &getPool() { return pool_; }

    SendQueue &getSendQueue(int sid);

    ReceiveQueue &getReceiveQueue(int sid);

    size_t mtuSize() const { return mtu_size_; }

   private:
    const size_t mtu_size_, max_packets_, queue_capacity_, wnd_size_;
    RWSpinlock queue_lock_;
    PacketBufferPool pool_;
    std::unordered_map<int, SendQueue *> send_queue_;
    std::unordered_map<int, ReceiveQueue *> receive_queue_;
};

}  // namespace rapid

#endif  // PACKET_MANAGER_H_
