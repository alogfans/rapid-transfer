// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_PROTOCOL_H
#define RDMA_UNRELIABLE_PROTOCOL_H

#include <atomic>
#include <map>

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "queue_entry.h"
#include "session_id_manager.h"

namespace rapid {
class PacketProcessor;

struct RdmaUnreliableProtocol : public Protocol {
    friend class PacketProcessor;

    RdmaUnreliableProtocol();

    virtual ~RdmaUnreliableProtocol();
    RdmaUnreliableProtocol(const RdmaUnreliableProtocol &) = delete;
    RdmaUnreliableProtocol &operator=(const RdmaUnreliableProtocol &) = delete;

    virtual int construct(const std::string &device_name, uint8_t rdma_port,
                          int gid_index);

    virtual int deconstruct();

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

   private:
    struct TaskInfo {
        RequestType type;
        std::unordered_map<std::string, uint32_t> next_sn;
    };

   private:
    bool running_;
    RdmaContext context_;
    SessionIdManager session_id_manager_;
    PacketProcessor *processors_;

    RWSpinlock task_lock_;
    std::unordered_map<TaskID, TaskInfo> task_info_;
    std::atomic<TaskID> next_task_id_;
};
}  // namespace rapid

#endif  // RDMA_UNRELIABLE_PROTOCOL_H
