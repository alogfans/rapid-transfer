// Copyright 2024 Feng Ren

#include "impl.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

static std::string getSocketAddress(const std::string &device_name) {
    struct ifreq ifr;
    struct sockaddr_in *sin;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        perror("socket");
        return "";
    }
    memset(&ifr, 0, sizeof(ifr));
    strncpy(ifr.ifr_name, device_name.c_str(), IFNAMSIZ - 1);
    if (ioctl(fd, SIOCGIFADDR, &ifr) < 0) {
        perror("ioctl");
        close(fd);
        return "";
    }
    close(fd);
    sin = (struct sockaddr_in *)&ifr.ifr_addr;
    return std::string(inet_ntoa(sin->sin_addr));
}

namespace rapid {
RdmaUnreliableProtocol::RdmaUnreliableProtocol() {}

RdmaUnreliableProtocol::~RdmaUnreliableProtocol() { deconstruct(); }

int RdmaUnreliableProtocol::construct(const std::string &device_name,
                                      uint8_t rdma_port, int gid_index) {
    int ret = context_.construct(getSocketAddress(device_name), device_name, rdma_port, gid_index);
    if (ret)
        return ret;
    worker_running_ = true;
    worker_ = std::thread([this](){
        while (worker_running_) {
            int rc = context_.runStep();
            if (rc) return rc;
        }
        return 0;
    });
    return 0;
}

int RdmaUnreliableProtocol::deconstruct() {
    if (worker_running_.exchange(false)) {
        worker_.join();
    }
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

int RdmaUnreliableProtocol::freeTask(TaskID task_id) { return 0; }

TaskID RdmaUnreliableProtocol::send(const std::string &peer_name,
                                    const std::vector<Buffer> &buffer_list) {
    return context_.send(peer_name, buffer_list, false);
}

TaskID RdmaUnreliableProtocol::receive(const std::string &peer_name,
                                       const std::vector<Buffer> &buffer_list) {
    return context_.receive(peer_name, buffer_list);
}

Status RdmaUnreliableProtocol::getStatus(TaskID task_id,
                                         size_t *transferred_bytes) {
    return context_.getStatus(task_id, transferred_bytes);
}

int RdmaUnreliableProtocol::registerLocalMemory(void *addr, size_t length) {
    return context_.registerLocalMemory(addr, length);
}

int RdmaUnreliableProtocol::unregisterLocalMemory(void *addr) {
    return context_.unregisterLocalMemory(addr);
}
}  // namespace rapid
