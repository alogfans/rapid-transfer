// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_PROTOCOL_H
#define RDMA_UNRELIABLE_PROTOCOL_H

#include <atomic>
#include <map>

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "context.h"

namespace rapid {
struct RdmaUnreliableProtocol : public Protocol {
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
    Context context_;
};
}  // namespace rapid

#endif  // RDMA_UNRELIABLE_PROTOCOL_H
