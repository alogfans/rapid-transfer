// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_PROTOCOL_H
#define RDMA_UNRELIABLE_PROTOCOL_H

#include <atomic>
#include <map>

#include "concurrency.h"
#include "context.h"
#include "context_mcast.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"

namespace rapid {
struct RdmaUnreliableProtocol : public Protocol {
    const static size_t kDefaultMTUSize = 4096;
    const static size_t kMaxPackets = 102400;
    const static size_t kQueueCapacity = 4096;

    RdmaUnreliableProtocol(size_t mtu_size = kDefaultMTUSize,
                           size_t max_packets = kMaxPackets,
                           size_t queue_capacity = kQueueCapacity,
                           bool spawn_worker = false);

    virtual ~RdmaUnreliableProtocol();
    RdmaUnreliableProtocol(const RdmaUnreliableProtocol &) = delete;
    RdmaUnreliableProtocol &operator=(const RdmaUnreliableProtocol &) = delete;

    virtual int construct(const std::string &device_name, uint8_t rdma_port,
                          int gid_index);

    virtual int deconstruct();

    virtual int joinMulticast(const std::string &multicast_addr);

    virtual int leaveMulticast(const std::string &multicast_addr);

    virtual int setMulticastReplicas(
        const std::string &multicast_addr,
        const std::vector<std::string> &peer_name_list);

    virtual int prepareConnection(const std::string &peer_name,
                                  Attributes &local);

    virtual int setupConnection(const std::string &peer_name,
                                const Attributes &peer);

    virtual TaskID send(const std::string &peer_name,
                        const std::vector<Buffer> &buffer_list);

    virtual TaskID receive(const std::string &peer_name,
                           const std::vector<Buffer> &buffer_list);

    virtual Status getStatus(TaskID task_id, size_t *transferred_bytes);

    virtual int freeTask(TaskID task_id);

    virtual int registerLocalMemory(void *addr, size_t length);

    virtual int unregisterLocalMemory(void *addr);

    int doEventLoop(int64_t timeout = -1);

   private:
#ifdef CONFIG_MCAST
    ContextMcast context_;
#else
    Context context_;
#endif
    const bool spawn_worker_;
    std::atomic<bool> worker_running_;
    std::thread worker_;
};
}  // namespace rapid

#endif  // RDMA_UNRELIABLE_PROTOCOL_H
