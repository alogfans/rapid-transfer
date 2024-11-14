// Copyright 2024 Feng Ren

#include "event_loop.h"

#include "packet_processor.h"

namespace rapid {
EventLoop::EventLoop(PacketProcessor *processor)
    : processor_(processor), packet_pool_(processor->context()) {}

EventLoop::~EventLoop() {}

int EventLoop::construct() {
    int ret = 0;
    packet_pool_.setupPacketPool();
    for (uint32_t i = 0; i < recv_wnd_; ++i) {
        ret = postReceiveWorkRequest();
        if (ret < 0) return ret;
    }
    return 0;
}

int EventLoop::deconstruct() {
    packet_pool_.destroyPacketPool();
    return 0;
}

int EventLoop::step() {
    uint64_t current_ts = GetCurrentTimeInUsec();
    int ret = submitRequests();
    if (ret) return ret;

    ret = pollCompletedPackets(SEND_CQ, current_ts);
    if (ret) return ret;

    ret = pollCompletedPackets(RECV_CQ, current_ts);
    if (ret) return ret;

    runReceiveCallbacks();

    ret = sendAckPackets(current_ts);
    if (ret) return ret;

    ret = sendDataPackets(current_ts);
    if (ret) return ret;

    return 0;
}

int EventLoop::submitRequests() {
    while (true) {
        bool has_entry = false;
        for (auto &entry : processor_->sessions_) {
            auto &peer_name = entry.first;
            auto &session = entry.second;
            if (send_buffer_.size() >= kWndSend ||
                !session.send_queue.hasRemainingFragment())
                continue;

            has_entry = true;
            auto cid = processor_->sessionIdManager().getSidBySender(peer_name);
            if (cid < 0) {
                LOG(ERROR) << "Not assigned SID for peer " << peer_name;
                return -1;
            }

            auto buffer = session.send_queue.popFragment();
            if (!buffer.addr || buffer.length <= 0 ||
                buffer.length > kMaxPayloadSize) {
                LOG(ERROR) << "Invalid send queue from peer " << peer_name;
                return -1;
            }

            Packet packet;
            packet.hdr.ts = 0;
            packet.hdr.cid = cid;
            packet.hdr.cmd = CMD_DATA;
            packet.hdr.wnd = send_wnd_;
            packet.hdr.sn = session.next_send_sn++;
            packet.hdr.len = buffer.length;
            packet.data = buffer.addr;
            packet.peer_name = peer_name;
            packet.resend_count = 0;
            send_buffer_.push_back(packet);
        }
        if (!has_entry) return 0;
    }
}

int EventLoop::pollCompletedPackets(int cq_index, uint64_t current_ts) {
    const static size_t kPollCount = 64;
    auto &context = processor_->context();
    ibv_wc wc[kPollCount];
    int nr_poll = context.poll(kPollCount, wc, cq_index);
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
                       << ", local_nic: " << context.deviceName()
                       << "): " << ibv_wc_status_str(wc[i].status);
            continue;
        }

        if (wc[i].opcode == IBV_WC_SEND) {
            Request *request = (Request *)wc[i].wr_id;
            packet_pool_.freePacket(request->addr[0]);
        }

        if (wc[i].opcode == IBV_WC_RECV) {
            int rc = processReceivedPacket(current_ts, wc[i]);
            if (rc < 0) {
                LOG(ERROR) << "Process received packet failed";
            }
        }

        delete request;
    }

    return 0;
}

int EventLoop::sendDataPackets(uint64_t current_ts) {
    auto &context = processor_->context();
    for (auto iter = send_buffer_.begin(); iter != send_buffer_.end();) {
        if (processor_->sessions_[iter->peer_name].status ==
            PacketProcessor::SESSION_RESET) {
            send_buffer_.erase(iter);
            iter = send_buffer_.begin();
        } else if (iter->resend_count >= kMaxResendCount) {
            LOG(WARNING) << "Unable to send data packet, sn=" << iter->hdr.sn;
            processor_->sessions_[iter->peer_name].status =
                PacketProcessor::SESSION_RESET;
            send_buffer_.erase(iter);
            iter = send_buffer_.begin();
        } else {
            ++iter;
        }
    }

    for (auto &record : send_buffer_) {
        if (record.resend_count != 0 && current_ts < record.hdr.ts + recv_rto_)
            continue;

        // if (send_credit_ <= 0)
        //     continue;
        // send_credit_--;

        record.resend_count++;
        record.hdr.ts = current_ts;
        PacketHeader *hdr = packet_pool_.allocatePacket();
        if (!hdr) {
            LOG(ERROR) << "Unable to allocate memory for packet";
            return -1;
        }

        EncodePacket(hdr, record.hdr);

        Request *request = new Request{
            .addr = {hdr, record.data},
            .length = {sizeof(PacketHeader), record.hdr.len},
            .lkey = {context.key(hdr).first, context.key(record.data).first}};

        auto endpoint =
            processor_->endpoint_store_.getOrCreateEndpoint(record.peer_name);
        if (!endpoint) {
            packet_pool_.freePacket(hdr);
            return -1;
        }

        int ret = endpoint->postSendRequest({request});
        if (ret <= 0) packet_pool_.freePacket(hdr);

        if (ret < 0) return -1;
    }

    return 0;
}

int EventLoop::sendAckPackets(uint64_t current_ts) {
    // TODO resend on necessary
    auto &endpoint_store = processor_->endpoint_store_;
    for (auto &entry : processor_->sessions_) {
        if (entry.second.next_ack_recv_sn == entry.second.next_recv_sn)
            continue;

        PacketHeader *hdr = packet_pool_.allocatePacket();
        if (!hdr) {
            LOG(ERROR) << "Unable to allocate memory for packet";
            return -1;
        }

        memset(hdr, 0, sizeof(PacketHeader));
        hdr->cid = entry.second.cid;
        hdr->cmd = CMD_ACK;
        hdr->wnd = send_wnd_;
        hdr->sn = entry.second.next_recv_sn - 1;  // TODO
        EncodePacket(hdr, *hdr);

        auto endpoint = endpoint_store.getOrCreateEndpoint(entry.first);
        if (!endpoint) return -1;

        Request *request =
            new Request{.addr = {hdr, nullptr},
                        .length = {sizeof(PacketHeader), 0},
                        .lkey = {endpoint_store.context().key(hdr).first, 0}};

        int ret = endpoint->postSendRequest({request});
        if (ret <= 0) {
            packet_pool_.freePacket(hdr);
            return -1;
        }

        entry.second.next_ack_recv_sn = entry.second.next_recv_sn;
    }
    return 0;
}

void EventLoop::updateRTO(uint64_t rtt) {
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

void EventLoop::runReceiveCallbacks() {
    for (auto &entry : processor_->sessions_) {
        auto &peer_name = entry.first;
        auto &session = entry.second;
        while (session.recv_queue.hasRemainingFragment()) {
            bool progress = false;
            for (auto iter = recv_buffer_.begin(); iter != recv_buffer_.end();
                 iter++) {
                auto &cid = session.cid;
                auto &next_recv_sn = session.next_recv_sn;
                cid =
                    processor_->sessionIdManager().getSidByReceiver(peer_name);
                if (iter->hdr.cid == cid && iter->hdr.sn == next_recv_sn) {
                    auto buffer = session.recv_queue.popFragment();
                    memcpy(buffer.addr, iter->data, buffer.length);
                    packet_pool_.freePacket(iter->data);
                    recv_buffer_.erase(iter);
                    ++next_recv_sn;
                    progress = true;
                    break;
                }
            }
            if (!progress) break;
        }
    }
}

int EventLoop::processReceivedPacket(uint64_t current_ts, ibv_wc &wc) {
    Packet packet;
    Request *request = (Request *)wc.wr_id;
    PacketHeader *hdr = (PacketHeader *)((uint64_t)request->addr[0] + 40);
    DecodePacket(packet.hdr, hdr);
    packet.data = hdr + 1;
    switch (packet.hdr.cmd) {
        case CMD_DATA: {
            auto peer_name =
                processor_->sessionIdManager().getEndPointByReceiver(hdr->cid);
            if (peer_name.empty()) {
                packet_pool_.freePacket(hdr);
                return -1;
            }
            auto &state = processor_->sessions_[peer_name];
            if (packet.hdr.sn < state.next_recv_sn ||
                packet.hdr.sn >= state.next_recv_sn + recv_wnd_) {
                packet_pool_.freePacket(hdr);
                break;
            }
            if (packet.hdr.sn >= state.next_recv_sn) {
                bool dup = false;
                for (auto &item : recv_buffer_) {
                    if (item.hdr.sn == packet.hdr.sn) {
                        dup = true;
                        packet_pool_.freePacket(hdr);
                        break;
                    }
                }
                if (!dup) recv_buffer_.push_back(packet);
            }
            break;
        }

        case CMD_ACK: {
            auto peer_name =
                processor_->sessionIdManager().getEndPointByReceiver(hdr->cid);
            auto &state = processor_->sessions_[peer_name];

            while (true) {
                bool found = false;
                for (auto iter = send_buffer_.begin();
                     iter != send_buffer_.end(); iter++) {
                    if (iter->hdr.sn <= packet.hdr.sn) {
                        send_buffer_.erase(iter);
                        found = true;
                        break;
                    }
                }
                if (!found) break;
            }

            updateRTO(current_ts - packet.hdr.ts);
            state.next_ack_send_sn = hdr->sn + 1;
            // timely_.update(current_ts - packet.hdr.ts, current_ts);
            packet_pool_.freePacket(hdr);
            break;
        }

        default:
            LOG(INFO) << "Unknown cmd: " << packet.hdr.cmd;
            packet_pool_.freePacket(hdr);
            break;
    }

    return postReceiveWorkRequest();
}

int EventLoop::postReceiveWorkRequest() {
    auto &context = processor_->context();
    PacketHeader *hdr = packet_pool_.allocatePacket();
    if (!hdr) {
        LOG(ERROR) << "Unable to allocate memory for packet";
        return -1;
    }

    Request *request = new Request{.addr = {hdr, nullptr},
                                   .length = {kPacketStorageSize, 0},
                                   .lkey = {context.key(hdr).first, 0}};

    int ret = processor_->endpoint_store_.postReceiveRequest({request});
    if (ret <= 0) packet_pool_.freePacket(hdr);
    return 0;
}
}  // namespace rapid
