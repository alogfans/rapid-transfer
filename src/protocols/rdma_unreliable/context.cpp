// Copyright 2024 Feng Ren

#include "context.h"

#include "protocols/common/rdma_ud_endpoint.h"

namespace rapid {
static inline uint64_t GetCurrentTS() {
    struct timeval tv_now;
    gettimeofday(&tv_now, nullptr);
    return (tv_now.tv_sec * 1000000 + tv_now.tv_usec);
}

Context::Context(size_t mtu_size, size_t max_packets, size_t queue_capacity)
    : mtu_size_(mtu_size),
      packet_manager_(mtu_size, max_packets, queue_capacity),
      next_task_id_(0) {}

Context::~Context() {}

int Context::construct(const std::string &device_name, uint8_t rdma_port,
                       int gid_index) {
    int ret = 0;
    ret = controller_.construct(device_name, rdma_port, gid_index);
    if (ret < 0) return ret;
    ret = packet_manager_.construct(device_name);
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

int Context::deconstruct() {
    active_session_map_.clear();
    controller_.context().unregisterMemoryRegion(
        packet_manager_.getPool().getArena());
    packet_manager_.deconstruct();
    controller_.deconstruct();
    return 0;
}

int Context::runStep() {
    uint64_t current_ts = GetCurrentTS();

    int ret = pollCompletedPackets(RECV_CQ, current_ts);
    if (ret < 0) return ret;

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

TaskID Context::send(const std::string &peer_name,
                     const std::vector<Buffer> &buffer_list) {
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
    uint32_t last_sn = 0;
    auto &queue = *active_session_map_[session].send_queue;
    int ret = queue.push(buffer_list, last_sn);
    if (ret) return ret;
    int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
    task_map_[task_id] = Task{session, last_sn, true, &queue};
    return task_id;
}

TaskID Context::receive(const std::string &peer_name,
                        const std::vector<Buffer> &buffer_list) {
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
    uint32_t last_sn = 0;
    auto &queue = *active_session_map_[session].receive_queue;
    int ret = queue.push(buffer_list, last_sn);
    if (ret) return ret;
    int task_id = next_task_id_.fetch_add(1);
    task_map_[task_id] = Task{session, last_sn, false, &queue};
    return task_id;
}

Status Context::getStatus(TaskID task_id, size_t *transferred_bytes) {
    if (!task_map_.count(task_id)) return Status::UNKNOWN;
    auto &task = task_map_[task_id];
    uint32_t ack_sn, next_sn;
    if (task.is_send) {
        auto &queue = *(SendQueue *)task.queue;
        ack_sn = queue.getAckSN();
        next_sn = queue.getNextSN();
    } else {
        auto &queue = *(ReceiveQueue *)task.queue;
        ack_sn = queue.getAckSN();
        next_sn = queue.getNextSN();
    }
    if (ack_sn <= next_sn) {
        if (task.last_sn <= ack_sn) return Status::SUCCESS;
    } else {
        if (task.last_sn >= next_sn) return Status::SUCCESS;
    }
    return Status::PENDING;
}

int Context::freeTask(TaskID task_id) {
    if (!task_map_.count(task_id)) return -1;
    task_map_.erase(task_id);
    return 0;
}

int Context::prepareConnection(const std::string &peer_addr,
                               Attributes &local) {
    return controller_.prepareConnection(peer_addr, local);
}

int Context::setupConnection(const std::string &peer_addr,
                             const Attributes &peer) {
    return controller_.setupConnection(peer_addr, peer);
}

int Context::registerLocalMemory(void *addr, size_t length) {
    return controller_.context().registerMemoryRegion(addr, length,
                                                      IBV_ACCESS_LOCAL_WRITE);
}

int Context::unregisterLocalMemory(void *addr) {
    return controller_.context().unregisterMemoryRegion(addr);
}

int Context::submitNormalRecvWR(PacketHandle &handle) {
    auto &endpoint_store = controller_.endpointStore();
    int index = recv_handles_qp_index_map_[handle.getRawPacket()];
    Request *request = request_cache_.allocate();
    new (request) Request{.addr = {handle.getRawPacket()},
                          .length = {packet_manager_.mtuSize()},
                          .lkey = {local_arena_lkey_}};
    return endpoint_store.postReceiveRequest({request}, index);
}

int Context::pollCompletedPackets(int cq_index, uint64_t current_ts) {
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

        request_cache_.deallocate(request);
    }

    return nr_poll;
}

thread_local uint64_t tl_resend = 0;
thread_local uint64_t tl_total = 0;

int Context::sendDataPackets(uint64_t current_ts) {
    const static size_t kRequestBatchSize = 4;
    auto &context = controller_.context();
    Buffer slices[2];
    for (auto session : active_session_map_) {
        if (current_ts - session.second.last_send_ts < recv_rto_ * 2 / 3)
            continue;
        std::vector<Request *> request_list;
        std::shared_ptr<RdmaUDEndPoint> endpoint;
        uint64_t head, tail;
        auto &send_queue = *session.second.send_queue;
        send_queue.getIndexRange(head, tail);
        head = std::min(head, tail + session.second.cwnd);
        bool skip_remaining = false;
        for (auto curr = tail; !skip_remaining && curr < head; curr++) {
            auto &handle = send_queue.getMutableEntry(curr);
            if (current_ts - handle.ts < recv_rto_) {
                skip_remaining = true;
                continue;
            }
            if (handle.ts) {
                tl_resend++;
                session.second.ssthresh =
                    std::max(kMinSSThreshValue, session.second.cwnd / 2);
                session.second.cwnd = session.second.ssthresh + kResendValue;
                session.second.incr = mtu_size_;
                send_queue.setWndSize(session.second.cwnd);
            }
            tl_total++;
            // if (tl_total % 100000 == 0)
            //     LOG(INFO) << tl_resend << "/" << tl_total;
            handle.ts = current_ts;
            session.second.last_send_ts = current_ts;
            uint32_t imm_data;
            if (handle.serialize(slices, imm_data)) return -1;
            Request *request = request_cache_.allocate();
            new (request) Request{
                .addr = {slices[0].addr, slices[1].addr},
                .length = {slices[0].length, slices[1].length},
                .lkey = {local_arena_lkey_, context.key(slices[1].addr).first},
                .imm_data = imm_data};

            if (!endpoint) {
                endpoint = controller_.getOrCreateEndpoint(session.first);
                if (!endpoint) return -1;
            }
            request_list.push_back(request);
            if (request_list.size() == kRequestBatchSize) {
                auto request_list_len = request_list.size();
                int ret = endpoint->postSendRequest(request_list);
                if (ret == (int)request_list.size()) {
                    session.second.send_packets += request_list_len;
                    stats_.send_packets.fetch_add(request_list_len,
                                                  std::memory_order_relaxed);
                }
                request_list.clear();
            }
        }
        auto request_list_len = request_list.size();
        if (request_list_len) {
            int ret = endpoint->postSendRequest(request_list);
            if (ret == (int)request_list.size()) {
                session.second.send_packets += request_list_len;
                stats_.send_packets.fetch_add(request_list_len,
                                              std::memory_order_relaxed);
            }
        }
    }
    return 0;
}

int Context::sendAckPackets(uint64_t current_ts) {
    Buffer slices[2];
    for (auto &session : active_session_map_) {
        auto &queue = *session.second.receive_queue;
        PacketHandle &handle = session.second.ack_handle;
        uint64_t recv_packets = session.second.recv_packets;
        if (session.second.ack_packets >= recv_packets || handle.inflight)
            continue;
        handle.session = uint8_t(session.first % 256);
        handle.cmd = PKT_CMD_ACK;
        uint64_t head, tail;
        const static uint64_t kMinWindowSize = 8;
        queue.getIndexRange(head, tail);
        handle.wnd = std::max(head - tail, kMinWindowSize);
        handle.sn = queue.getAckSN();
        handle.ts = queue.getLastTS();
        handle.inflight = true;
        uint32_t imm_data;
        if (handle.serialize(slices, imm_data)) return -1;
        Request *request = request_cache_.allocate();
        new (request) Request{.addr = {slices[0].addr},
                              .length = {slices[0].length},
                              .lkey = {local_arena_lkey_},
                              .imm_data = imm_data,
                              .context = &handle};

        auto endpoint = controller_.getOrCreateEndpoint(session.first);
        if (!endpoint) return -1;
        int ret = endpoint->postSendRequest({request});
        if (ret != 1) return -1;
        session.second.ack_packets = recv_packets;
        stats_.ack_packets.fetch_add(1, std::memory_order_relaxed);
    }
    return 0;
}

int Context::processReceivedPacket(uint64_t current_ts, ibv_wc &wc) {
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
        if (session >= 0) {
            assert(active_session_map_.count(session));
            switch (handle.cmd) {
                case PKT_CMD_DATA: {
                    auto &session_entry = active_session_map_[session];
                    session_entry.receive_queue->markCompleted(handle);
                    stats_.recv_packets.fetch_add(1, std::memory_order_relaxed);
                    session_entry.recv_packets++;
                    break;
                }
                case PKT_CMD_ACK: {
                    auto &session_entry = active_session_map_[session];
                    session_entry.send_queue->markCompleted(handle.sn);
                    auto rtt = (current_ts - handle.ts) & ((1ull << 48) - 1);
                    updateRTO(rtt);
                    updateWndOnSuccess(session, handle.wnd,
                                       *session_entry.send_queue);
                    break;
                }
                default:
                    LOG(INFO) << "Unknown packet";
                    break;
            }
        }
        return submitNormalRecvWR(handle);
    }
    return 0;
}

void Context::updateRTO(uint64_t rtt) {
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

void Context::updateWndOnSuccess(int session, uint32_t rwnd,
                                 SendQueue &send_queue) {
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
    entry.rwnd = rwnd;
    entry.cwnd = std::min(entry.rwnd, entry.cwnd);
    if (send_queue.getWndSize() != entry.cwnd) {
        send_queue.setWndSize(entry.cwnd);
    }
}

}  // namespace rapid
