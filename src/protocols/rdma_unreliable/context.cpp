// Copyright 2024 Feng Ren

#include "context.h"

#include "protocols/common/rdma_ud_endpoint.h"

namespace rapid {
static inline uint64_t GetCurrentTS() {
    struct timeval tv_now;
    gettimeofday(&tv_now, nullptr);
    return (tv_now.tv_sec * 1000000 + tv_now.tv_usec);
}

Context::Context() : next_task_id_(0), send_timeout_(kDefaultSendTimeout) {}

Context::Context(size_t mtu_size, size_t max_packets, size_t queue_capacity)
    : packet_manager_(mtu_size, max_packets, queue_capacity),
      next_task_id_(0),
      send_timeout_(kDefaultSendTimeout) {}

Context::~Context() {}

int Context::construct(std::string local_addr, const std::string &device_name,
                       uint8_t rdma_port, int gid_index) {
    int ret = 0;
    ret = controller_.construct(local_addr, device_name, rdma_port, gid_index);
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

int Context::registerMcastNode(const std::string &multicast_addr) {
    return controller_.registerMcastNode(multicast_addr);
}

int Context::unregisterMcastNode(const std::string &multicast_addr) {
    return controller_.unregisterMcastNode(multicast_addr);
}

int Context::runStep() {
    uint64_t current_ts = GetCurrentTS();

    int ret = pollCompletedPackets(RECV_CQ, current_ts);
    if (ret < 0) return ret;

    thread_local uint64_t snapshot_recv_cnt = 0;  
    uint64_t recv_cnt = stats_.recv_data_packets.load(std::memory_order_relaxed);
    if (snapshot_recv_cnt != recv_cnt) {
        ret = sendAckPackets(current_ts);
        if (ret == 0)
            snapshot_recv_cnt = recv_cnt;
    }

    ret = sendDataPackets(current_ts);
    if (ret) return ret;

    ret = pollCompletedPackets(SEND_CQ, current_ts);
    if (ret < 0) return ret;
    return 0;
}

TaskID Context::send(const std::string &peer_name,
                     const std::vector<Buffer> &buffer_list, bool multicast) {
    int session = controller_.findSession(peer_name, 0);
    if (session < 0) {
        LOG(ERROR) << "cannot assign session id";
        return -1;
    }
    if (!active_session_map_.count(session)) {
        int ret = packet_manager_.getPool().allocatePacket(
            active_session_map_[session].ack_handle);
        if (ret) return ret;
    }
    uint32_t last_sn;
    int ret = packet_manager_.getSendQueue(session).push(buffer_list, last_sn);
    if (ret) return ret;
    int task_id = next_task_id_.fetch_add(1);
    task_map_[task_id] = Task{session, last_sn, true};
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
    }
    uint32_t last_sn;
    int ret =
        packet_manager_.getReceiveQueue(session).push(buffer_list, last_sn);
    if (ret) return ret;
    int task_id = next_task_id_.fetch_add(1);
    task_map_[task_id] = Task{session, last_sn, false};
    return task_id;
}

Status Context::getStatus(TaskID task_id, size_t *transferred_bytes) {
    if (!task_map_.count(task_id)) return Status::UNKNOWN;
    auto &task = task_map_[task_id];
    if (task.is_send) {
        auto &queue = packet_manager_.getSendQueue(task.session);
        if (queue.getAckSN() >= task.last_sn) {
            return Status::SUCCESS;
        }
    } else {
        auto &queue = packet_manager_.getReceiveQueue(task.session);
        if (queue.getAckSN() >= task.last_sn) {
            return Status::SUCCESS;
        }
    }
    return Status::PENDING;
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
    Request *request = new Request{.addr = {handle.getRawPacket()},
                                   .length = {packet_manager_.mtuSize()},
                                   .lkey = {local_arena_lkey_}};
    return endpoint_store.postReceiveRequest({request});
}

int Context::pollCompletedPackets(int cq_index, uint64_t current_ts) {
    const static size_t kPollCount = 64;
    ibv_wc wc[kPollCount];
    int nr_poll = controller_.context().poll(kPollCount, wc, cq_index);
    if (nr_poll < 0) {
        LOG(ERROR) << "Worker: Failed to poll completion queues";
        return -1;
    }

    for (int i = 0; i < nr_poll; ++i) {
        auto request = (Request *)wc[i].wr_id;
        __sync_fetch_and_sub(request->qp_depth, 1);
        if (wc[i].status != IBV_WC_SUCCESS) {
            LOG(ERROR) << "Worker: Process failed for slice (addr: "
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

int Context::sendDataPackets(uint64_t current_ts) {
    auto &context = controller_.context();
    for (auto session : active_session_map_) {
        packet_manager_.getSendQueue(session.first).forEach(
            [&](PacketHandle &handle) -> int {
                if (current_ts - handle.ts < send_timeout_) return 0;
                handle.ts = current_ts;
                std::vector<Buffer> slices;
                uint32_t imm_data;
                if (handle.serialize(slices, imm_data)) return -1;
                Request *request = nullptr;
                if (slices.size() == 1)
                    request = new Request{.addr = {slices[0].addr},
                                          .length = {slices[0].length},
                                          .lkey = {local_arena_lkey_},
                                          .imm_data = imm_data};
                else if (slices.size() == 2)
                    request = new Request{
                        .addr = {slices[0].addr, slices[1].addr},
                        .length = {slices[0].length, slices[1].length},
                        .lkey = {local_arena_lkey_,
                                 context.key(slices[1].addr).first},
                        .imm_data = imm_data};
                else
                    return -1;
                auto endpoint = controller_.getOrCreateEndpoint(session.first);
                if (!endpoint) return -1;
                int rc = endpoint->postSendRequest({request});
                return rc;
            });
    }
    return 0;
}

int Context::sendAckPackets(uint64_t current_ts) {
    for (auto session : active_session_map_) {
        uint32_t ack_sn = packet_manager_.getReceiveQueue(session.first).getAckSN();
        PacketHandle &handle = session.second.ack_handle;
        if (handle.inflight) return -2;
        handle.session = uint8_t(session.first % 256);
        handle.cmd = PKT_CMD_ACK;
        handle.wnd = uint16_t(recv_handles_.size());
        handle.sn = ack_sn;
        handle.ts = current_ts;
        std::vector<Buffer> slices;
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
        endpoint->postSendRequest({request});
    }
    return 0;
}

int Context::processReceivedPacket(uint64_t current_ts, ibv_wc &wc) {
    Request *request = (Request *)wc.wr_id;
    if (wc.opcode == IBV_WC_SEND) {
        auto handle = (PacketHandle *) request->context;
        if (handle && handle->cmd == PKT_CMD_ACK)
            handle->inflight = false;
        return 0;
    }
    if (wc.opcode == IBV_WC_RECV) {
        PacketHandle handle;
        int ret = handle.setRawPacket((char *)request->addr[0], true);
        if (ret) return ret;
        ret = handle.deserialize(wc.imm_data, wc.byte_len);
        if (ret) return ret;
        ibv_grh *grh = (ibv_grh *)request->addr[0];
        int session =
            controller_.findSession(grh->sgid, wc.src_qp, handle.session);
        if (session < 0) {
            LOG(ERROR) << "cannot assign session id";
            return -1;
        }
        switch (handle.cmd) {
            case PKT_CMD_DATA:
                packet_manager_.getReceiveQueue(session).markCompleted(handle);
                stats_.recv_data_packets.fetch_add(1,
                                                   std::memory_order_relaxed);
                break;
            case PKT_CMD_ACK: {
                packet_manager_.getSendQueue(session).markCompleted(handle.sn);
                // updateRTO(current_ts - packet.hdr.ts);
                // timely_.update(current_ts - packet.hdr.ts, current_ts);
                break;
            }
            default:
                LOG(INFO) << "Unknown packet";
                break;
        }
        return submitNormalRecvWR(handle);
    }
    return 0;
}

// void Context::updateRTO(uint64_t rtt) {
//     if (recv_srtt_ == 0) {
//         recv_srtt_ = rtt;
//         recv_rttval_ = rtt / 2;
//     } else {
//         long delta = rtt - recv_srtt_;
//         if (delta < 0) delta = -delta;
//         recv_rttval_ = (3 * recv_rttval_ + delta) / 4;
//         recv_srtt_ = (7 * recv_srtt_ + rtt) / 8;
//         if (recv_srtt_ < 1) recv_srtt_ = 1;
//     }
//     uint64_t rto = recv_srtt_ + 4 * recv_rttval_;
//     recv_rto_ = std::min(std::max(kMinRTO, rto), kMaxRTO);
// }

}  // namespace rapid
