// rapid_transfer.cpp
// Copyright (C) 2024 Feng Ren

#include "rapid_transfer.h"

#include "protocol.h"
#include "protocols/rdma_reliable/impl.h"
#include "protocols/rdma_unreliable/impl.h"
#include "protocols/rdma_unreliable/impl_mcast.h"
#include "session_manager.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <glog/logging.h>
#include <json/json.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

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
        engine->protocol_ = new RdmaUnreliableMcastProtocol(
            mtu_size, max_packets, queue_capacity);
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

int RapidTransfer::runStep() { return protocol_->runStep(); }

int RapidTransfer::startListener(const std::string &listen_address,
                                 const OnConnectionStateChange &callback) {
    auto on_accept = [=](const std::string &peer_name,
                         const Attributes &request,
                         Attributes &response) -> int {
        std::cout << "[DEBUG] on_accept called for peer: " << peer_name << std::endl;
        std::cout.flush();

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

        std::cout << "[DEBUG] RDMA connection established for peer: " << peer_name << std::endl;
        std::cout.flush();

        if (callback) callback(peer_name, true);

        return 0;
    };

    auto on_error = [=](const std::string &peer_name) -> void {
        if (callback) callback(peer_name, false);
    };

    // Register callbacks for write/read requests
    auto on_write_request = [this](const std::string &peer_name,
                                    const std::vector<RemoteBuffer> &remote_buffers) -> int {
        std::cerr << "[on_write_request] Called from peer: " << peer_name << std::endl;

        // Remote peer wants us to receive data into remote_buffers
        // Convert RemoteBuffer to Buffer (ignoring rkey for now)
        std::vector<Buffer> buffers;
        for (const auto &rbuf : remote_buffers) {
            Buffer buf;
            buf.addr = rbuf.remote_addr;
            buf.length = rbuf.length;
            buffers.push_back(buf);
        }

        // Use peer_name directly as session name (now matches after our fix)
        int ret = protocol_->receive(peer_name, buffers);
        if (ret < 0) {
            LOG(ERROR) << "Failed to post receive buffers for write request, ret=" << ret;
            return -1;
        }
        std::cerr << "[on_write_request] Successfully posted receive, task_id=" << ret << std::endl;
        return 0;
    };

    auto on_read_request = [this](const std::string &peer_name,
                                   const std::vector<RemoteBuffer> &remote_buffers) -> int {
        std::cerr << "[on_read_request] Called from peer: " << peer_name << std::endl;

        // Remote peer wants us to send data from remote_buffers
        // Convert RemoteBuffer to Buffer (ignoring rkey for now)
        std::vector<Buffer> buffers;
        for (const auto &rbuf : remote_buffers) {
            Buffer buf;
            buf.addr = rbuf.remote_addr;
            buf.length = rbuf.length;
            buffers.push_back(buf);
        }

        // Use peer_name directly as session name (now matches after our fix)
        int ret = protocol_->send(peer_name, buffers);
        if (ret < 0) {
            LOG(ERROR) << "Failed to post send buffers for read request, ret=" << ret;
            return -1;
        }
        std::cerr << "[on_read_request] Successfully posted send, task_id=" << ret << std::endl;
        return 0;
    };

    session_manager_->setWriteReadCallbacks(on_write_request, on_read_request);

    return session_manager_->startListener(listen_address, on_accept, on_error);
}

int RapidTransfer::shutdownListener() {
    return session_manager_->shutdownListener();
}

int RapidTransfer::makeConnectionIfNeeded(const std::string &peer_name) {
    if (session_manager_->isMulticastAddress(peer_name)) return 0;
    if (!session_manager_->hasConnection(peer_name)) {
        std::cerr << "[makeConnectionIfNeeded] Establishing connection to " << peer_name << std::endl;

        Attributes request, response;
        int ret = protocol_->prepareConnection(peer_name, request);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: get local attributes";
            return -1;
        }

        std::cerr << "[makeConnectionIfNeeded] prepareConnection done, connecting..." << std::endl;
        ret = session_manager_->connect(peer_name, request, response);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: perform out-of-band "
                          "communication";
            return -1;
        }

        std::cerr << "[makeConnectionIfNeeded] TCP connect done, setting up connection..." << std::endl;
        ret = protocol_->setupConnection(peer_name, response);
        if (ret) {
            LOG(ERROR) << "Unable to setup endpoint: set peer attributes";
            session_manager_->disconnect(peer_name);
            return -1;
        }

        std::cerr << "[makeConnectionIfNeeded] Connection established successfully for peer: " << peer_name << std::endl;

        // Get the session name that was assigned by the server
        std::string session_name = session_manager_->getSessionName(peer_name);
        std::cerr << "[makeConnectionIfNeeded] Server assigned session name: " << session_name << std::endl;

        // Now we need to also register the session_name (e.g., "server/0") on this side
        // Call prepareConnection and setupConnection again with the session name
        Attributes session_request, session_response;
        ret = protocol_->prepareConnection(session_name, session_request);
        if (ret) {
            LOG(WARNING) << "Unable to prepare connection for session name, will use peer name";
            return 0;  // Not fatal, can continue with peer_name
        }

        // Copy the RDMA info from response (which contains server's RDMA details)
        ret = protocol_->setupConnection(session_name, response);
        if (ret) {
            LOG(WARNING) << "Unable to setup connection for session name, will use peer name";
            return 0;  // Not fatal
        }

        std::cerr << "[makeConnectionIfNeeded] Also registered session name: " << session_name << std::endl;
    } else {
        std::cerr << "[makeConnectionIfNeeded] Connection already exists for " << peer_name << std::endl;
    }
    return 0;
}

// Helper function to serialize RemoteBuffer to JSON
static std::string serializeRemoteBuffers(const std::vector<RemoteBuffer> &buffers) {
    Json::Value json_array(Json::arrayValue);
    for (const auto &buf : buffers) {
        Json::Value item;
        item["addr"] = std::to_string(reinterpret_cast<uintptr_t>(buf.remote_addr));
        item["length"] = Json::Value::UInt64(buf.length);
        item["rkey"] = buf.rkey;
        json_array.append(item);
    }
    Json::StreamWriterBuilder writer;
    return Json::writeString(writer, json_array);
}

TaskID RapidTransfer::write(const std::string &peer_name,
                            const std::vector<Buffer> &local_buffers,
                            const std::vector<RemoteBuffer> &remote_buffers) {
    using namespace async_simple::coro;

    // 1. Make sure RDMA connection is established
    int ret = makeConnectionIfNeeded(peer_name);
    if (ret) return ret;

    // 2. Get the correct session name for this peer
    std::string session_name = session_manager_->getSessionName(peer_name);
    std::cerr << "[write] Using session name: " << session_name << " for peer " << peer_name << std::endl;
    std::cerr.flush();

    // 3. Serialize remote_buffers to JSON
    std::string buffers_json = serializeRemoteBuffers(remote_buffers);

    // 4. RPC call to notify remote peer to prepare receive
    // Use cached client instead of creating new one
    coro_rpc::coro_rpc_client* client = session_manager_->getRPCClient(peer_name);
    if (!client) {
        LOG(ERROR) << "No cached RPC client for peer: " << peer_name;
        return -1;
    }

    std::cerr << "[write] Sending RPC to notify peer..." << std::endl;
    auto rpc_result = client->send_request<&SessionManager::handleWriteRequest>(peer_name, session_name, buffers_json);
    std::optional<int> result = syncAwait(
        [&]() -> async_simple::coro::Lazy<std::optional<int>> {
            auto r = co_await co_await rpc_result;
            if (!r) {
                LOG(ERROR) << "Write RPC failed: " << r.error().msg;
                co_return std::nullopt;
            }
            co_return r->result();
        }());

    if (!result || result.value() != 0) {
        LOG(ERROR) << "Remote peer failed to prepare receive";
        return -1;
    }

    std::cerr << "[write] Peer ready, sending data..." << std::endl;

    // 5. Call local send to transfer data
    // Use the correct session name that matches the RDMA connection
    ret = protocol_->send(session_name, local_buffers);
    if (ret < 0) {
        LOG(ERROR) << "Send failed after RPC";
        session_manager_->disconnect(peer_name);
        return ret;
    }

    std::cerr << "[write] Data sent, task_id=" << ret << std::endl;
    return ret;
}

TaskID RapidTransfer::read(const std::string &peer_name,
                           const std::vector<Buffer> &local_buffers,
                           const std::vector<RemoteBuffer> &remote_buffers) {
    using namespace async_simple::coro;

    // 1. Make sure RDMA connection is established
    int ret = makeConnectionIfNeeded(peer_name);
    if (ret) return ret;

    // 2. Get the correct session name for this peer
    std::string session_name = session_manager_->getSessionName(peer_name);

    // 3. Serialize remote_buffers to JSON
    std::string buffers_json = serializeRemoteBuffers(remote_buffers);

    // 4. RPC call to notify remote peer to prepare send
    coro_rpc::coro_rpc_client* client = session_manager_->getRPCClient(peer_name);
    if (!client) {
        LOG(ERROR) << "No cached RPC client for peer: " << peer_name;
        return -1;
    }

    auto rpc_result = client->send_request<&SessionManager::handleReadRequest>(peer_name, session_name, buffers_json);
    std::optional<int> result = syncAwait(
        [&]() -> async_simple::coro::Lazy<std::optional<int>> {
            auto r = co_await co_await rpc_result;
            if (!r) {
                LOG(ERROR) << "Read RPC failed: " << r.error().msg;
                co_return std::nullopt;
            }
            co_return r->result();
        }());

    if (!result || result.value() != 0) {
        LOG(ERROR) << "Remote peer failed to prepare send";
        return -1;
    }

    // 5. Call local receive to get data
    ret = protocol_->receive(session_name, local_buffers);
    if (ret < 0) {
        LOG(ERROR) << "Receive failed after RPC";
        return ret;
    }

    return ret;
}
}  // namespace rapid
