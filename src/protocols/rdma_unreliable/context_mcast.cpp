// Copyright 2024 Feng Ren

#include "context_mcast.h"

#include "protocols/common/rdma_ud_endpoint.h"

namespace rapid {
static inline uint64_t GetCurrentTS() {
    struct timeval tv_now;
    gettimeofday(&tv_now, nullptr);
    return (tv_now.tv_sec * 1000000 + tv_now.tv_usec);
}

ContextMcast::ContextMcast(size_t mtu_size, size_t max_packets,
                           size_t queue_capacity)
    : mtu_size_(mtu_size),
      packet_manager_(mtu_size, max_packets, queue_capacity),
      next_task_id_(0) {}

ContextMcast::~ContextMcast() {}

int ContextMcast::construct(const std::string &device_name, uint8_t rdma_port,
                            int gid_index) {
    int ret = 0;
    ret = controller_.construct(device_name, rdma_port, gid_index);
    if (ret < 0) return ret;
    ret = packet_manager_.construct();
    if (ret < 0) return ret;

    auto &pool = packet_manager_.getPool();
    void *arena_base = pool.getArena();
    size_t arena_capacity = pool.getCapacity();
    ret = controller_.context().registerMemoryRegion(arena_base, arena_capacity,
                                                     IBV_ACCESS_LOCAL_WRITE);
    if (ret) return ret;
    local_arena_lkey_ = controller_.context().key(arena_base).first;

    recv_handles_.resize(kNumReceiveHandles);
    for (size_t i = 0; i < recv_handles_.size(); ++i) {
        ret = pool.allocatePacket(recv_handles_[i], true);
        if (ret < 0) return ret;
        ret = submitNormalRecvWR(recv_handles_[i]);
        if (ret < 0) return ret;
        auto index = i % controller_.endpointStore().qpNum().size();
        recv_handles_qp_index_map_[recv_handles_[i].getRawPacket()] = index;
    }
    return 0;
}

int ContextMcast::deconstruct() {
    active_session_map_.clear();
    controller_.context().unregisterMemoryRegion(
        packet_manager_.getPool().getArena());
    packet_manager_.deconstruct();
    controller_.deconstruct();
    return 0;
}

int ContextMcast::joinMulticast(const std::string &multicast_addr) {
    int ret = controller_.joinMulticast(multicast_addr);
    if (ret) return ret;
    auto &recv_handles = multicast_recv_info_map_[multicast_addr].recv_handles;
    auto &pool = packet_manager_.getPool();
    recv_handles.resize(kNumReceiveHandles);
    void *arena_base = pool.getArena();
    size_t arena_capacity = pool.getCapacity();
    auto context = controller_.getMulticastContext(multicast_addr);
    ret = context->registerMemoryRegion(arena_base, arena_capacity,
                                        IBV_ACCESS_LOCAL_WRITE);
    if (ret) return ret;
    for (size_t i = 0; i < recv_handles.size(); ++i) {
        ret = pool.allocatePacket(recv_handles[i], true);
        if (ret < 0) return ret;
        ret = submitMulticastRecvWR(multicast_addr, recv_handles[i]);
        if (ret < 0) return ret;
        auto index = 0;
        recv_handles_qp_index_map_[recv_handles[i].getRawPacket()] = index;
    }
    return 0;
}

int ContextMcast::leaveMulticast(const std::string &multicast_addr) {
    auto &pool = packet_manager_.getPool();
    auto context = controller_.getMulticastContext(multicast_addr);
    context->unregisterMemoryRegion(pool.getArena());
    if (multicast_recv_info_map_.count(multicast_addr)) {
        auto &recv_handles =
            multicast_recv_info_map_[multicast_addr].recv_handles;
        for (size_t i = 0; i < recv_handles.size(); ++i)
            pool.freePacket(recv_handles[i]);
        multicast_recv_info_map_.erase(multicast_addr);
    }
    return controller_.leaveMulticast(multicast_addr);
}

int ContextMcast::setMulticastReplicas(
    const std::string &multicast_addr,
    const std::vector<std::string> &peer_name_list) {
    auto context = controller_.getMulticastContext(multicast_addr);
    if (!context) {
        LOG(INFO) << "multicast address not available";
        return -1;
    }
    context->setReplicas(peer_name_list);
    return packet_manager_.setMulticastReplicaNum(context->groupId(),
                                                  peer_name_list.size());
}

int ContextMcast::runStep() {
    uint64_t current_ts = GetCurrentTS();

    int ret = pollCompletedPackets(RECV_CQ, current_ts);
    if (ret < 0) return ret;

    // for receiver side
    for (auto &entry : controller_.getMulticastContextMap()) {
        ret = pollMcastCompletedPackets(entry.second, current_ts);
        if (ret < 0) return ret;
    }

    sendAckPackets(current_ts);

    ret = sendDataPackets(current_ts);
    if (ret) return ret;

    ret = pollCompletedPackets(SEND_CQ, current_ts);
    if (ret < 0) return ret;

#ifdef DEBUG
    thread_local uint64_t last_ts = 0;
    if (current_ts - last_ts > 1000000) {
        thread_local uint64_t last_recv_packets = 0;
        thread_local uint64_t last_send_packets = 0;
        thread_local uint64_t last_ack_packets = 0;
        LOG(INFO) << stats_.recv_packets.load() - last_recv_packets << " "
                  << stats_.send_packets.load() - last_send_packets << " "
                  << stats_.ack_packets.load() - last_ack_packets << " ";
        last_recv_packets = stats_.recv_packets.load();
        last_send_packets = stats_.send_packets.load();
        last_ack_packets = stats_.ack_packets.load();
        last_ts = current_ts;
    }
#endif

    return 0;
}

TaskID ContextMcast::send(const std::string &peer_name,
                          const std::vector<Buffer> &buffer_list) {
    auto context = controller_.getMulticastContext(peer_name);
    if (!context) {
        LOG(ERROR) << "peer_name should be valid multicast address";
        return -1;
    }
    int session = controller_.findSession(peer_name, 0);
    assert(session >= 0);
    if (!active_session_map_.count(session)) {
        int ret = packet_manager_.getPool().allocatePacket(
            active_session_map_[session].ack_handle);
        if (ret) return ret;
        active_session_map_[session].setup(packet_manager_, session);
    }
    uint32_t last_sn;
    auto &queue = *active_session_map_[session].mcast_send_queue;
    int ret = queue.push(buffer_list, last_sn);
    if (ret) return ret;
    int task_id = next_task_id_.fetch_add(1);
    task_map_[task_id] = Task{session, last_sn, true, &queue};
    return task_id;
}

TaskID ContextMcast::receive(const std::string &peer_name,
                             const std::vector<Buffer> &buffer_list) {
    if (controller_.getMulticastContext(peer_name)) {
        LOG(ERROR) << "peer_name should be valid device address";
        return -1;
    }
    int session = controller_.findSession(peer_name, 0);
    if (session < 0) {
        LOG(ERROR) << "cannot assign session id";
        return -1;
    }
    if (!active_session_map_.count(session)) {
        int ret = packet_manager_.getPool().allocatePacket(
            active_session_map_[session].ack_handle);
        if (ret) return ret;
        active_session_map_[session].setup(packet_manager_, session);
    }
    uint32_t last_sn;
    auto &queue = *active_session_map_[session].receive_queue;
    int ret = queue.push(buffer_list, last_sn);
    if (ret) return ret;
    int task_id = next_task_id_.fetch_add(1);
    task_map_[task_id] = Task{session, last_sn, false, &queue};
    return task_id;
}

Status ContextMcast::getStatus(TaskID task_id, size_t *transferred_bytes) {
    if (!task_map_.count(task_id)) return Status::UNKNOWN;
    auto &task = task_map_[task_id];
    if (task.is_send) {
        auto &queue = *(McastSendQueue *)task.queue;
        auto ack_sn = queue.getAckSN();
        auto next_sn = queue.getNextSN();
        if (ack_sn <= next_sn) {
            if (task.last_sn <= ack_sn) return Status::SUCCESS;
        } else {
            if (task.last_sn >= next_sn) return Status::SUCCESS;
        }
    } else {
        auto &queue = *(ReceiveQueue *)task.queue;
        auto ack_sn = queue.getAckSN();
        auto next_sn = queue.getNextSN();
        if (ack_sn <= next_sn) {
            if (task.last_sn <= ack_sn) return Status::SUCCESS;
        } else {
            if (task.last_sn >= next_sn) return Status::SUCCESS;
        }
    }
    return Status::PENDING;
}

int ContextMcast::freeTask(TaskID task_id) {
    if (!task_map_.count(task_id)) return -1;
    task_map_.erase(task_id);
    return 0;
}

int ContextMcast::prepareConnection(const std::string &peer_addr,
                                    Attributes &local) {
    return controller_.prepareConnection(peer_addr, local);
}

int ContextMcast::setupConnection(const std::string &peer_addr,
                                  const Attributes &peer) {
    return controller_.setupConnection(peer_addr, peer);
}

int ContextMcast::registerLocalMemory(void *addr, size_t length) {
    int ret = controller_.context().registerMemoryRegion(
        addr, length, IBV_ACCESS_LOCAL_WRITE);
    if (ret) return ret;
    auto &context_map = controller_.getMulticastContextMap();
    for (auto &entry : context_map) {
        ret = entry.second->registerMemoryRegion(addr, length,
                                                 IBV_ACCESS_LOCAL_WRITE);
        if (ret) return ret;
    }
    return 0;
}

int ContextMcast::unregisterLocalMemory(void *addr) {
    controller_.context().unregisterMemoryRegion(addr);
    auto &context_map = controller_.getMulticastContextMap();
    for (auto &entry : context_map) {
        entry.second->unregisterMemoryRegion(addr);
    }
    return 0;
}

int ContextMcast::submitNormalRecvWR(PacketHandle &handle) {
    auto &endpoint_store = controller_.endpointStore();
    int index = recv_handles_qp_index_map_[handle.getRawPacket()];
    Request *request = new Request{.addr = {handle.getRawPacket()},
                                   .length = {packet_manager_.mtuSize()},
                                   .lkey = {local_arena_lkey_}};
    return endpoint_store.postReceiveRequest({request}, index);
}

int ContextMcast::submitMulticastRecvWR(const std::string &multicast_addr,
                                        PacketHandle &handle) {
    auto context = controller_.getMulticastContext(multicast_addr);
    if (!context) {
        LOG(ERROR) << "invalid multicast address";
        return -1;
    }
    int index = recv_handles_qp_index_map_[handle.getRawPacket()];
    Request *request =
        new Request{.addr = {handle.getRawPacket()},
                    .length = {packet_manager_.mtuSize()},
                    .lkey = {context->key(handle.getRawPacket()).first}};
    return context->postReceiveRequest({request}, index);
}

int ContextMcast::pollCompletedPackets(int cq_index, uint64_t current_ts) {
    const static size_t kPollCount = 64;
    ibv_wc wc[kPollCount];
    int nr_poll = controller_.context().poll(kPollCount, wc, cq_index);
    if (nr_poll < 0) {
        LOG(ERROR) << "worker: failed to poll completion queues";
        return -1;
    }

    for (int i = 0; i < nr_poll; ++i) {
        auto request = (Request *)wc[i].wr_id;
        __sync_fetch_and_sub(request->qp_depth, 1);
        if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "worker: process failed for slice (addr: "
                       << request->addr[0] << "+" << request->addr[1]
                       << ", length: " << request->length[0] << "+"
                       << request->length[1] << ", lkey: " << request->lkey[0]
                       << "+" << request->lkey[1]
                       << ", local_nic: " << controller_.context().deviceName()
                       << "): " << ibv_wc_status_str(wc[i].status);
            abort();  // fuse
            continue;
        }

        int rc = processReceivedPacket(current_ts, wc[i]);
        if (rc < 0) {
            LOG(ERROR) << "Process received packet failed";
        }

        delete request;
    }

    return nr_poll;
}

int ContextMcast::pollMcastCompletedPackets(
    std::shared_ptr<RdmaMulticastContext> context, uint64_t current_ts) {
    int total_nr_poll = 0;
    for (int conn_id = 0; conn_id < (int)context->config().num_qp_per_endpoint;
         ++conn_id) {
        const static size_t kPollCount = 64;
        ibv_wc wc[kPollCount];
        int nr_poll = context->poll(kPollCount, wc, conn_id);
        if (nr_poll < 0) {
            LOG(ERROR) << "worker: failed to poll completion queues";
            return -1;
        }
        for (int i = 0; i < nr_poll; ++i) {
            auto request = (Request *)wc[i].wr_id;
            __sync_fetch_and_sub(request->qp_depth, 1);
            if (wc[i].status != IBV_WC_SUCCESS) {
                LOG(ERROR) << "worker: process failed for slice (addr: "
                           << request->addr[0] << "+" << request->addr[1]
                           << ", length: " << request->length[0] << "+"
                           << request->length[1]
                           << ", lkey: " << request->lkey[0] << "+"
                           << request->lkey[1] << ", local_nic: "
                           << controller_.context().deviceName()
                           << "): " << ibv_wc_status_str(wc[i].status);
                abort();  // fuse
                continue;
            }
            int rc = processReceivedPacket(current_ts, wc[i],
                                           context->multicastAddress());
            if (rc < 0) {
                LOG(ERROR) << "Process received packet failed";
            }
            delete request;
        }
        total_nr_poll += nr_poll;
    }
    return total_nr_poll;
}

int ContextMcast::sendDataPackets(uint64_t current_ts) {
    std::vector<Buffer> slices;
    slices.reserve(2);
    for (auto session : active_session_map_) {
        auto context = controller_.getMulticastContext(session.first);
        if (!context) continue;
        std::vector<Request *> request_list;
        auto &send_queue = *session.second.mcast_send_queue;
        send_queue.forEach([&](PacketHandle &handle) -> int {
            if (current_ts - handle.ts < recv_rto_) return 0;
            if (handle.ts) {
                session.second.ssthresh =
                    std::max(kMinSSThreshValue, session.second.cwnd / 2);
                session.second.cwnd = 1;
                session.second.incr = mtu_size_;
                send_queue.setWndSize(session.second.cwnd);
            }
            handle.ts = current_ts;
            slices.clear();
            uint32_t imm_data;
            if (handle.serialize(slices, imm_data)) return -1;
            Request *request = nullptr;
            // LOG(INFO) << "send data ... sn = " << handle.sn;
            if (slices.size() == 1)
                request =
                    new Request{.addr = {slices[0].addr},
                                .length = {slices[0].length},
                                .lkey = {context->key(slices[0].addr).first},
                                .imm_data = imm_data};
            else if (slices.size() == 2)
                request =
                    new Request{.addr = {slices[0].addr, slices[1].addr},
                                .length = {slices[0].length, slices[1].length},
                                .lkey = {context->key(slices[0].addr).first,
                                         context->key(slices[1].addr).first},
                                .imm_data = imm_data};
            else
                return -1;
            request_list.push_back(request);
            if (request_list.size() == 4) {
                auto request_list_len = request_list.size();
                context->postSendRequest(request_list);
                session.second.send_packets += request_list_len;
                stats_.send_packets.fetch_add(request_list_len,
                                              std::memory_order_relaxed);
                request_list.clear();
            }
            return 0;
        });
        auto request_list_len = request_list.size();
        if (request_list_len) {
            context->postSendRequest(request_list);
            session.second.send_packets += request_list_len;
            stats_.send_packets.fetch_add(request_list_len,
                                          std::memory_order_relaxed);
        }
    }
    return 0;
}

int ContextMcast::sendAckPackets(uint64_t current_ts) {
    std::vector<Buffer> slices;
    slices.reserve(2);
    for (auto &session : active_session_map_) {
        auto &queue = *session.second.receive_queue;
        PacketHandle &handle = session.second.ack_handle;
        if (session.second.ack_packets >= session.second.recv_packets ||
            handle.inflight)
            continue;
        handle.session = uint8_t(session.first % 256);
        handle.cmd = PKT_CMD_ACK;
        handle.wnd = queue.getWndSize();
        handle.sn = queue.getAckSN();
        handle.ts = queue.getLastTS();
        handle.inflight = true;
        slices.clear();
        uint32_t imm_data;
        if (handle.serialize(slices, imm_data)) return -1;
        Request *request = nullptr;
        if (slices.size() == 1)
            request = new Request{.addr = {slices[0].addr},
                                  .length = {slices[0].length},
                                  .lkey = {local_arena_lkey_},
                                  .imm_data = imm_data,
                                  .context = &handle};
        else
            return -1;
        auto endpoint = controller_.getOrCreateEndpoint(session.first);
        if (!endpoint) return -1;
        int ret = endpoint->postSendRequest({request});
        // LOG(INFO) << "send ack ... " << handle.sn;
        if (ret != 1) return -1;
        session.second.ack_packets = session.second.recv_packets;
        stats_.ack_packets.fetch_add(1, std::memory_order_relaxed);
    }
    return 0;
}

int ContextMcast::processReceivedPacket(uint64_t current_ts, ibv_wc &wc,
                                        const std::string &multicast_addr) {
    Request *request = (Request *)wc.wr_id;
    if (wc.opcode == IBV_WC_SEND) {
        auto handle = (PacketHandle *)request->context;
        if (handle && handle->cmd == PKT_CMD_ACK) handle->inflight = false;
        return 0;
    }
    if (wc.opcode == IBV_WC_RECV) {
        uint32_t imm_data = wc.imm_data;
        if (!(wc.wc_flags & IBV_WC_WITH_IMM)) imm_data = 0;
        PacketHandle handle;
        int ret = handle.setRawPacket((char *)request->addr[0], true);
        if (ret) return ret;
        ret = handle.deserialize(imm_data, wc.byte_len);
        if (ret) return ret;
        ibv_grh *grh = (ibv_grh *)request->addr[0];
        int session =
            controller_.findSession(grh->sgid, wc.src_qp, handle.session);
        if (session < 0 && !multicast_addr.empty()) {
            session = controller_.findSession(multicast_addr, handle.session);
        }
        if (session >= 0) {
            switch (handle.cmd) {
                case PKT_CMD_DATA: {
                    assert(active_session_map_.count(session));
                    uint64_t head, tail;
                    auto &receive_queue =
                        *active_session_map_[session].receive_queue;
                    receive_queue.getIndexRange(head, tail);
                    if (head == tail) break;
                    receive_queue.markCompleted(handle);
                    stats_.recv_packets.fetch_add(1, std::memory_order_relaxed);
                    active_session_map_[session].recv_packets++;
                    break;
                }
                case PKT_CMD_ACK: {
                    int index = 0;
                    session = controller_.redirectMulticast(session, index);
                    assert(active_session_map_.count(session));
                    auto &mcast_send_queue =
                        *active_session_map_[session].mcast_send_queue;
                    mcast_send_queue.markCompleted(index, handle.sn);
                    auto rtt = (current_ts - handle.ts) & ((1ull << 48) - 1);
                    updateRTO(rtt);
                    updateWndOnSuccess(session, handle.wnd);
                    break;
                }
                default: {
                    LOG(INFO) << "Unknown packet";
                    break;
                }
            }
        }
        if (!multicast_addr.empty()) {
            return submitMulticastRecvWR(multicast_addr, handle);
        } else {
            return submitNormalRecvWR(handle);
        }
    }
    return 0;
}

void ContextMcast::updateRTO(uint64_t rtt) {
    if (recv_srtt_ == 0) {
        recv_srtt_ = rtt;
        recv_rttval_ = rtt / 2;
    } else {
        long delta = rtt - recv_srtt_;
        if (delta < 0) delta = -delta;
        recv_rttval_ = (3 * recv_rttval_ + delta) / 4;
        recv_srtt_ = (7 * recv_srtt_ + rtt) / 8;
        if (recv_srtt_ < 1) recv_srtt_ = 1;
    }
    uint64_t rto = recv_srtt_ + 4 * recv_rttval_;
    recv_rto_ = std::min(std::max(kMinRTO, rto), kMaxRTO);
}

void ContextMcast::updateWndOnSuccess(int session, uint32_t rwnd) {
    auto &entry = active_session_map_[session];
    if (entry.cwnd < entry.ssthresh) {
        entry.cwnd++;
        entry.incr += mtu_size_;
    } else {
        if (entry.incr < mtu_size_) entry.incr = mtu_size_;
        entry.incr += (mtu_size_ * mtu_size_) / entry.incr + (mtu_size_ / 16);
        if ((entry.cwnd + 1) * mtu_size_ <= entry.incr) {
            entry.cwnd = (entry.incr + mtu_size_ - 1) / mtu_size_;
        }
    }
    auto &queue = *entry.mcast_send_queue;
    entry.rwnd = rwnd;
    entry.cwnd = std::min(entry.rwnd, entry.cwnd);
    if (queue.getWndSize() != entry.cwnd) {
        queue.setWndSize(entry.cwnd);
    }
}

}  // namespace rapid
