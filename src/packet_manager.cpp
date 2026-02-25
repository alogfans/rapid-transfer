// packet_manager.cpp
// Copyright (C) 2026 RapidXfer Team
// Simplified - Only PacketBufferPool implementation

#include "packet_manager.h"

#include <glog/logging.h>
#include <linux/limits.h>
#include <numa.h>

#include <fstream>
#include <libgen.h>

namespace rapid {

const static size_t kGRHSize = sizeof(ibv_grh);

// ========== Packet Buffer Pool ==========

PacketBufferPool::PacketBufferPool(size_t mtu_size, size_t max_packets)
    : mtu_size_(mtu_size),
      max_packets_(max_packets),
      arena_(nullptr),
      global_free_buffer_(nullptr) {}

PacketBufferPool::~PacketBufferPool() { deconstruct(); }

int PacketBufferPool::construct(const std::string& device_name) {
    int socket_id = 0;
    if (!device_name.empty()) {
        char path[PATH_MAX + 32];
        char resolved_path[PATH_MAX];
        // Get the PCI bus id for the infiniband device
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 device_name.c_str());
        if (realpath(path, resolved_path)) {
            std::string pci_bus_id = basename(resolved_path);
            snprintf(path, sizeof(path), "%s/numa_node", resolved_path);
            std::ifstream(path) >> socket_id;
        }
    }

    // Allocate memory on specific NUMA node
    arena_ = numa_alloc_onnode(mtu_size_ * max_packets_, socket_id);
    if (!arena_) {
        LOG(ERROR) << "[PacketBufferPool] Failed to allocate arena";
        return -1;
    }

    // Initialize free list (linked list through packet buffers)
    for (size_t index = 0; index < max_packets_; ++index) {
        uint8_t* ptr = static_cast<uint8_t*>(arena_) + mtu_size_ * index;
        *reinterpret_cast<uintptr_t*>(ptr) = reinterpret_cast<uintptr_t>(global_free_buffer_);
        global_free_buffer_ = ptr;
    }

    LOG(INFO) << "[PacketBufferPool] Allocated " << max_packets_
              << " packets of " << mtu_size_ << " bytes on socket " << socket_id;

    return 0;
}

int PacketBufferPool::deconstruct() {
    if (arena_) {
        numa_free(arena_, mtu_size_ * max_packets_);
        arena_ = nullptr;
        global_free_buffer_ = nullptr;
    }
    return 0;
}

int PacketBufferPool::allocatePacket(PacketHandle& handle, bool with_grh) {
    // Note: RWSpinlock could be used here for thread safety
    // For now, assuming single-threaded allocation or external locking

    void* packet_buf = global_free_buffer_;
    if (!packet_buf) {
        LOG(WARNING) << "[PacketBufferPool] No free packets available";
        return -1;
    }

    // Pop from free list
    uintptr_t next = *reinterpret_cast<uintptr_t*>(packet_buf);
    global_free_buffer_ = reinterpret_cast<void*>(next);

    return handle.setRawPacket(packet_buf, with_grh);
}

int PacketBufferPool::freePacket(PacketHandle& handle) {
    auto packet_buf = handle.packet_buf;
    if (!packet_buf) {
        LOG(ERROR) << "[PacketBufferPool] Invalid packet handle";
        return -1;
    }

    // Push to free list
    *reinterpret_cast<uintptr_t*>(packet_buf) = reinterpret_cast<uintptr_t>(global_free_buffer_);
    global_free_buffer_ = packet_buf;

    // Clear handle
    handle.packet_buf = nullptr;
    handle.data_len = 0;

    return 0;
}

}  // namespace rapid
