// Copyright 2024 Feng Ren

#include "impl.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

namespace rapid {
RdmaUnreliableProtocol::RdmaUnreliableProtocol(size_t mtu_size,
                                               size_t max_packets,
                                               size_t queue_capacity,
                                               bool spawn_worker)
    : context_(mtu_size, max_packets, queue_capacity),
      spawn_worker_(spawn_worker) {}

RdmaUnreliableProtocol::~RdmaUnreliableProtocol() { deconstruct(); }

int RdmaUnreliableProtocol::construct(const std::string &device_name,
                                      uint8_t rdma_port, int gid_index) {
    int ret = context_.construct(device_name, rdma_port, gid_index);
    if (ret) return ret;
    if (!spawn_worker_) return 0;
    worker_running_ = true;
    worker_ = std::thread([this]() {
        while (worker_running_) {
            int rc = context_.runStep();
            if (rc) {
                LOG(WARNING) << "worker terminated unexceptedly";
                return rc;
            }
        }
        return 0;
    });
    return 0;
}

int RdmaUnreliableProtocol::deconstruct() {
    if (spawn_worker_ && worker_running_.exchange(false)) worker_.join();
    return context_.deconstruct();
}

int RdmaUnreliableProtocol::prepareConnection(const std::string &peer_name,
                                              Attributes &local) {
    return context_.prepareConnection(peer_name, local);
}

int RdmaUnreliableProtocol::setupConnection(const std::string &peer_name,
                                            const Attributes &peer) {
    return context_.setupConnection(peer_name, peer);
}

int RdmaUnreliableProtocol::freeTask(TaskID task_id) {
    return context_.freeTask(task_id);
}

TaskID RdmaUnreliableProtocol::send(const std::string &peer_name,
                                    const std::vector<Buffer> &buffer_list) {
    return context_.send(peer_name, buffer_list);
}

TaskID RdmaUnreliableProtocol::receive(const std::string &peer_name,
                                       const std::vector<Buffer> &buffer_list) {
    return context_.receive(peer_name, buffer_list);
}

Status RdmaUnreliableProtocol::getStatus(TaskID task_id,
                                         size_t *transferred_bytes) {
    Status status = context_.getStatus(task_id, transferred_bytes);
    if (status == Status::PENDING && !spawn_worker_) doEventLoop(0);
    return status;
}

int RdmaUnreliableProtocol::registerLocalMemory(void *addr, size_t length) {
    return context_.registerLocalMemory(addr, length);
}

int RdmaUnreliableProtocol::unregisterLocalMemory(void *addr) {
    return context_.unregisterLocalMemory(addr);
}

int RdmaUnreliableProtocol::joinMulticast(const std::string &multicast_addr) {
#ifdef CONFIG_MCAST
    return context_.joinMulticast(multicast_addr);
#else
    LOG(ERROR) << "not implemented";
    return -1;
#endif 
}

int RdmaUnreliableProtocol::leaveMulticast(const std::string &multicast_addr) {
#ifdef CONFIG_MCAST
    return context_.leaveMulticast(multicast_addr);
#else
    LOG(ERROR) << "not implemented";
    return -1;
#endif 
}

int RdmaUnreliableProtocol::setMulticastReplicas(
    const std::string &multicast_addr,
    const std::vector<std::string> &peer_name_list) {
#ifdef CONFIG_MCAST
    return context_.setMulticastReplicas(multicast_addr, peer_name_list);
#else
    LOG(ERROR) << "not implemented";
    return -1;
#endif 
}

int RdmaUnreliableProtocol::doEventLoop(int64_t timeout) {
    if (lrand48() % 8) return 0;  // drop requests
    do {
        int rc = context_.runStep();
        if (rc) {
            LOG(WARNING) << "worker terminated unexceptedly";
            return rc;
        }
    } while (timeout < 0);
    return 0;
}
}  // namespace rapid
