// Copyright 2024 Feng Ren

#include "impl_mcast.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

namespace rapid {
RdmaUnreliableMcastProtocol::RdmaUnreliableMcastProtocol(size_t mtu_size,
                                                         size_t max_packets,
                                                         size_t queue_capacity,
                                                         bool spawn_worker)
    : context_(mtu_size, max_packets, queue_capacity),
      spawn_worker_(spawn_worker) {}

RdmaUnreliableMcastProtocol::~RdmaUnreliableMcastProtocol() { deconstruct(); }

int RdmaUnreliableMcastProtocol::construct(const std::string &device_name,
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

int RdmaUnreliableMcastProtocol::deconstruct() {
    if (spawn_worker_ && worker_running_.exchange(false)) worker_.join();
    return context_.deconstruct();
}

int RdmaUnreliableMcastProtocol::runStep() {
    if (!spawn_worker_) return context_.runStep();
    return 0;
}

int RdmaUnreliableMcastProtocol::prepareConnection(const std::string &peer_name,
                                                   Attributes &local) {
    return context_.prepareConnection(peer_name, local);
}

int RdmaUnreliableMcastProtocol::setupConnection(const std::string &peer_name,
                                                 const Attributes &peer) {
    return context_.setupConnection(peer_name, peer);
}

int RdmaUnreliableMcastProtocol::freeTask(TaskID task_id) {
    return context_.freeTask(task_id);
}

TaskID RdmaUnreliableMcastProtocol::send(
    const std::string &peer_name, const std::vector<Buffer> &buffer_list) {
    return context_.send(peer_name, buffer_list);
}

TaskID RdmaUnreliableMcastProtocol::receive(
    const std::string &peer_name, const std::vector<Buffer> &buffer_list) {
    return context_.receive(peer_name, buffer_list);
}

Status RdmaUnreliableMcastProtocol::getStatus(TaskID task_id,
                                              size_t *transferred_bytes) {
    Status status = context_.getStatus(task_id, transferred_bytes);
    if (status == Status::PENDING && !spawn_worker_) doEventLoop(0);
    return status;
}

int RdmaUnreliableMcastProtocol::registerLocalMemory(void *addr,
                                                     size_t length) {
    return context_.registerLocalMemory(addr, length);
}

int RdmaUnreliableMcastProtocol::unregisterLocalMemory(void *addr) {
    return context_.unregisterLocalMemory(addr);
}

int RdmaUnreliableMcastProtocol::joinMulticast(
    const std::string &multicast_addr) {
    return context_.joinMulticast(multicast_addr);
}

int RdmaUnreliableMcastProtocol::leaveMulticast(
    const std::string &multicast_addr) {
    return context_.leaveMulticast(multicast_addr);
}

int RdmaUnreliableMcastProtocol::setMulticastReplicas(
    const std::string &multicast_addr,
    const std::vector<std::string> &peer_name_list) {
    return context_.setMulticastReplicas(multicast_addr, peer_name_list);
}

int RdmaUnreliableMcastProtocol::doEventLoop(int64_t timeout) {
    thread_local uint64_t last_ts = 4000;
    uint64_t current_ts = getCurrentTimeInNano();
    const static uint64_t kThreshold = 0;             // 1us
    if (current_ts - last_ts < kThreshold) return 0;  // drop requests
    last_ts = current_ts;
    return context_.runStep();
}
}  // namespace rapid
