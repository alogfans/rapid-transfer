// packet_manager.h
// Copyright (C) 2026 RapidXfer Team
// Simplified - Only PacketBufferPool and PacketHandle

#ifndef PACKET_MANAGER_H_
#define PACKET_MANAGER_H_

#include <endian.h>
#include <linux/types.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stack>

#include "concurrency.h"
#include "protocols/common/rdma_context.h"
#include "rapid_transfer.h"

#define PKT_CMD_DATA (0)
#define PKT_CMD_ACK (1)
#define SHORT_SN(x) (uint32_t((x)) & 0xffffff)

namespace rapid {

// ========== Packet Handle ==========
// Minimal wrapper for packet buffer access

class PacketHandle {
   public:
    friend class PacketBufferPool;

    PacketHandle() = default;

    int setRawPacket(void* packet_buf, bool with_grh = false) {
        packet_buf = packet_buf;
        with_grh = with_grh;
        return 0;
    }

    void* getRawPacket() { return packet_buf; }

    void* getPayload() {
        if (with_grh) {
            // Skip GRH (40 bytes)
            return static_cast<uint8_t*>(packet_buf) + 40;
        }
        return packet_buf;
    }

    uint32_t getPayloadLength() { return data_len; }

    int deserialize(uint32_t imm_data, uint32_t packet_length) {
        data_len = packet_length;
        if (with_grh) {
            data_len -= 40;  // Subtract GRH
        }
        return 0;
    }

   public:
    bool inflight{false};
    void* packet_buf{nullptr};
    uint32_t data_len{0};
    bool with_grh{false};

   private:
    int encode() { return 0; }
    int decode() { return 0; }
};

// ========== Packet Buffer Pool ==========
// Manages pre-allocated RDMA packet buffers

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

// ========== Packet Manager ==========
// Wrapper for packet buffer pool

class PacketManager {
   public:
    PacketManager(size_t mtu_size, size_t max_packets, size_t queue_capacity)
        : mtu_size_(mtu_size),
          max_packets_(max_packets),
          queue_capacity_(queue_capacity),
          pool_(mtu_size, max_packets) {}

    ~PacketManager() { deconstruct(); }

    PacketManager(const PacketManager&) = delete;
    PacketManager& operator=(const PacketManager&) = delete;

    int construct(const std::string& device_name = "") {
        return pool_.construct(device_name);
    }

    int deconstruct() {
        return pool_.deconstruct();
    }

    PacketBufferPool& getPool() { return pool_; }

    size_t mtuSize() const { return mtu_size_; }

   private:
    const size_t mtu_size_, max_packets_, queue_capacity_;
    PacketBufferPool pool_;
};

}  // namespace rapid

#endif  // PACKET_MANAGER_H_
