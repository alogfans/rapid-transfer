// protocol.h
// Copyright (C) 2024 Feng Ren

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "rapid_transfer.h"
#include <glog/logging.h>

namespace rapid {
enum RequestType { SEND, RECEIVE };

struct Protocol {
    Protocol() {}
    virtual ~Protocol() {}
    Protocol(const Protocol &) = delete;
    Protocol &operator=(const Protocol &) = delete;

    virtual int construct(const std::string &device_name, uint8_t rdma_port,
                          int gid_index) = 0;

    virtual int deconstruct() = 0;

    virtual int joinMulticast(const std::string &multicast_addr) {
        LOG(INFO) << "not implemented";
        return -1;
    }

    virtual int leaveMulticast(const std::string &multicast_addr) {
        LOG(INFO) << "not implemented";
        return -1;
    }

    virtual int setMulticastReplicas(const std::string &multicast_addr,
                                     const std::vector<std::string> &peer_name_list) {
        LOG(INFO) << "not implemented";
        return -1;
    }

    virtual int prepareConnection(const std::string &peer_name,
                                  Attributes &local) = 0;

    virtual int setupConnection(const std::string &peer_name,
                                const Attributes &peer) = 0;

    virtual TaskID send(const std::string &peer_name,
                        const std::vector<Buffer> &buffer_list) = 0;

    virtual TaskID receive(const std::string &peer_name,
                           const std::vector<Buffer> &buffer_list) = 0;

    virtual Status getStatus(TaskID task_id, size_t *transferred_bytes) = 0;

    virtual int freeTask(TaskID task_id) = 0;

    virtual int registerLocalMemory(void *addr, size_t length) = 0;

    virtual int unregisterLocalMemory(void *addr) = 0;
};
}  // namespace rapid

#endif  // PROTOCOL_H
