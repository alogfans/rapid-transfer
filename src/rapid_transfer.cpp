// rapid_transfer.cpp
// Copyright (C) 2024 Feng Ren

#include "rapid_transfer.h"

#include "protocol.h"
#include "protocols/rdma_reliable/impl.h"
#include "protocols/rdma_unreliable/impl.h"
#include "protocols/rdma_unreliable/impl_mcast.h"
#include "session_manager.h"

namespace rapid {
std::shared_ptr<RapidTransfer> RapidTransfer::Create(
    const std::string &protocol, const std::string &device_name,
    uint8_t rdma_port, int gid_index) {
    size_t mtu_size = 1024;
    size_t max_packets = 102400 / 4 * 6;
    size_t queue_capacity = 4096;
    if (getenv("RT_MTU_SIZE")) {
        mtu_size = std::atoi(getenv("RT_MTU_SIZE"));
    }
    if (getenv("RT_MAX_PACKETS")) {
        max_packets = std::atoi(getenv("RT_MAX_PACKETS"));
    }
    if (getenv("RT_QUEUE_CAPACITY")) {
        queue_capacity = std::atoi(getenv("RT_QUEUE_CAPACITY"));
    }
    auto engine = std::make_shared<RapidTransfer>(device_name);
    engine->session_manager_ = new SessionManager();
    if (protocol == "rdma_reliable") {
        engine->protocol_ = new RdmaReliableProtocol();
    } else if (protocol == "rdma_unreliable") {
        engine->protocol_ =
            new RdmaUnreliableProtocol(mtu_size, max_packets, queue_capacity);
    } else if (protocol == "rdma_unreliable_mcast") {
        engine->protocol_ =
            new RdmaUnreliableMcastProtocol(mtu_size, max_packets, queue_capacity);
    } else {
        LOG(ERROR) << "Unrecognized protocol";
        return nullptr;
    }

    int ret = engine->protocol_->construct(device_name, rdma_port, gid_index);
    if (ret) {
        LOG(ERROR) << "Failed to construct protocol";
        return nullptr;
    }

    return engine;
}

RapidTransfer::RapidTransfer(const std::string &device_name)
    : session_manager_(nullptr), protocol_(nullptr) {}

RapidTransfer::~RapidTransfer() {
    delete protocol_;
    delete session_manager_;
}

int RapidTransfer::joinMulticast(const std::string &multicast_addr) {
    return protocol_->joinMulticast(multicast_addr);
}

int RapidTransfer::leaveMulticast(const std::string &multicast_addr) {
    return protocol_->leaveMulticast(multicast_addr);
}

int RapidTransfer::setMulticastReplicas(
    const std::string &multicast_addr,
    const std::vector<std::string> &peer_name_list) {
    for (auto &entry : peer_name_list) {
        int ret = makeConnectionIfNeeded(entry);
        if (ret) return ret;
    }
    return protocol_->setMulticastReplicas(multicast_addr, peer_name_list);
}

TaskID RapidTransfer::send(const std::string &peer_name,
                           const std::vector<Buffer> &buffer_list) {
    int ret = makeConnectionIfNeeded(peer_name);
    if (ret) return ret;
    ret = protocol_->send(peer_name, buffer_list);
    if (ret < 0) session_manager_->disconnect(peer_name);
    return ret;
}

TaskID RapidTransfer::receive(const std::string &peer_name,
                              const std::vector<Buffer> &buffer_list) {
    // No need to connect because this is called by listener
    int ret = protocol_->receive(peer_name, buffer_list);
    return ret;
}

Status RapidTransfer::getStatus(TaskID task, size_t *transferred_bytes) {
    return protocol_->getStatus(task, transferred_bytes);
}

int RapidTransfer::freeTask(TaskID task_id) {
    return protocol_->freeTask(task_id);
}

int RapidTransfer::registerLocalMemory(void *addr, size_t length) {
    return protocol_->registerLocalMemory(addr, length);
}

int RapidTransfer::unregisterLocalMemory(void *addr) {
    return protocol_->unregisterLocalMemory(addr);
}

int RapidTransfer::runStep() {
    return protocol_->runStep();
}

int RapidTransfer::startListener(const std::string &listen_address,
                                 const OnConnectionStateChange &callback) {
    auto on_accept = [=](const std::string &peer_name,
                         const Attributes &request,
                         Attributes &response) -> int {
        int ret = protocol_->prepareConnection(peer_name, response);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: get local attributes";
            return -1;
        }

        ret = protocol_->setupConnection(peer_name, request);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: set peer attributes";
            return -1;
        }

        if (callback) callback(peer_name, true);

        return 0;
    };

    auto on_error = [=](const std::string &peer_name) -> void {
        if (callback) callback(peer_name, false);
    };

    return session_manager_->startListener(listen_address, on_accept, on_error);
}

int RapidTransfer::shutdownListener() {
    return session_manager_->shutdownListener();
}

int RapidTransfer::makeConnectionIfNeeded(const std::string &peer_name) {
    if (session_manager_->isMulticastAddress(peer_name)) return 0;
    if (!session_manager_->hasConnection(peer_name)) {
        Attributes request, response;
        int ret = protocol_->prepareConnection(peer_name, request);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: get local attributes";
            return -1;
        }

        ret = session_manager_->connect(peer_name, request, response);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: perform out-of-band "
                          "communication";
            return -1;
        }

        ret = protocol_->setupConnection(peer_name, response);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: set peer attributes";
            session_manager_->disconnect(peer_name);
            return -1;
        }
    }
    return 0;
}
}  // namespace rapid
