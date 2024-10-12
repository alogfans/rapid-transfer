// Copyright 2024 Feng Ren

#include "event_loop.h"
#include "impl.h"

namespace rapid
{
    EventLoop::EventLoop(RdmaUnreliableProtocol *protocol)
        : protocol_(protocol),
          endpoint_store_(protocol->endpoint_store_),
          running_(false),
          packet_pool_(protocol->context_)
    {
    }

    EventLoop::~EventLoop() {}

    int EventLoop::start()
    {
        packet_pool_.setupPacketPool();
        running_ = true;
        worker_list_.emplace_back(std::thread(std::bind(&EventLoop::worker, this)));
        return 0;
    }

    int EventLoop::join()
    {
        if (!running_.exchange(false))
            return 0;
        for (auto &entry : worker_list_)
            entry.join();
        packet_pool_.destroyPacketPool();
        return 0;
    }

    void EventLoop::worker()
    {
        for (uint32_t i = 0; i < recv_wnd_; ++i)
            postReceiveWorkRequest();

        while (running_)
        {
            uint64_t current_ts = GetCurrentTimeInUsec();
            if (submitRequests())
                continue;
            if (pollCompletedPackets(SEND_CQ, current_ts))
                continue;
            if (pollCompletedPackets(RECV_CQ, current_ts))
                continue;
            runReceiveCallbacks();
            if (sendAckPackets(current_ts))
                continue;
            if (sendDataPackets(current_ts))
                continue;
        }
    }

    int EventLoop::submitRequests()
    {
        for (auto &entry : protocol_->send_queue_)
        {
            auto peer_name = entry.first;
            auto &state = sessions_[peer_name];
            auto cid = protocol_->session_id_manager_.getSidBySender(peer_name);
            if (cid < 0)
            {
                LOG(WARNING) << "SID is not assigned, cannot send packets";
                continue;
            }

            while (send_buffer_.size() < kWndSend && entry.second.hasRemainingFragment())
            {
                auto buffer = entry.second.popFragment();
                assert(buffer.length <= kMaxPayloadSize);
                Packet packet;
                packet.hdr.ts = 0;
                packet.hdr.cid = cid;
                packet.hdr.cmd = CMD_DATA;
                packet.hdr.wnd = send_wnd_;
                packet.hdr.sn = state.next_send_sn++;
                packet.hdr.len = buffer.length;
                packet.data = buffer.addr;
                packet.peer_name = peer_name;
                packet.resend_count = 0;
                send_buffer_.push_back(packet);
            }
        }
        return 0;
    }

    int EventLoop::pollCompletedPackets(int cq_index, uint64_t current_ts)
    {
        const static size_t kPollCount = 64;
        auto &context = endpoint_store_.context();
        ibv_wc wc[kPollCount];
        int nr_poll = context.poll(kPollCount, wc, cq_index);
        if (nr_poll < 0)
        {
            LOG(ERROR) << "Worker: Failed to poll completion queues";
            return -1;
        }

        for (int i = 0; i < nr_poll; ++i)
        {
            auto request = (Request *)wc[i].wr_id;
            __sync_fetch_and_sub(request->qp_depth, 1);
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                LOG(ERROR) << "Worker: Process failed for slice (addr: " << request->addr
                           << ", length: " << request->length
                           << ", lkey: " << request->lkey
                           << ", local_nic: " << context.deviceName()
                           << "): " << ibv_wc_status_str(wc[i].status);
                continue;
            }

            switch (wc[i].opcode)
            {
            case IBV_WC_RECV:
                ProcessReceivedPacket(current_ts, wc[i]);
                break;
            case IBV_WC_SEND:
                break;
            default:
                LOG(ERROR) << "Unknown RDMA opcode: " << wc[i].opcode;
            }
            delete request;
        }

        return 0;
    }

    int EventLoop::sendDataPackets(uint64_t current_ts)
    {
        auto &context = endpoint_store_.context();
        for (auto iter = send_buffer_.begin(); iter != send_buffer_.end();)
        {
            if (iter->resend_count >= kMaxResendCount)
            {
                LOG(WARNING) << "Unable to send data packet, sn=" << iter->hdr.sn;
                sessions_[iter->peer_name].lost_packet_sn.push_back(iter->hdr.sn);
                send_buffer_.erase(iter);
                iter = send_buffer_.begin();
            }
            else
            {
                ++iter;
            }
        }

        for (auto &record : send_buffer_)
        {
            if (record.resend_count != 0 && current_ts < record.hdr.ts + recv_rto_)
                continue;

            // if (send_credit_ <= 0)
            //     continue;
            // send_credit_--;

            record.resend_count++;
            record.hdr.ts = current_ts;
            PacketHeader *hdr = packet_pool_.allocatePacket();
            EncodePacket(hdr, record.hdr);

            Request *request = new Request{
                .addr = {hdr, record.data},
                .length = {sizeof(PacketHeader), record.hdr.len},
                .lkey = {
                    context.key(hdr).first,
                    context.key(record.data).first}};

            LOG(INFO) << "Send data: " << record.data << ", " << record.hdr.len;
            auto endpoint = endpoint_store_.getOrCreateEndpoint(record.peer_name);
            if (!endpoint)
                return -1;
            int ret = endpoint->postSendRequest({request});
            if (ret < 0)
                return -1;
        }
        return 0;
    }

    int EventLoop::sendAckPackets(uint64_t current_ts)
    {
        for (auto &entry : sessions_)
        {
            // TODO 添加一个重发功能
            if (entry.second.acked_next_recv_sn == entry.second.next_recv_sn && !entry.second.resend_ack)
                continue;
            PacketHeader *hdr = packet_pool_.allocatePacket();
            memset(hdr, 0, sizeof(PacketHeader));
            hdr->cid = entry.second.cid;
            hdr->cmd = CMD_ACK;
            hdr->wnd = send_wnd_;
            hdr->una = entry.second.next_recv_sn;
            EncodePacket(hdr, *hdr);
            LOG(INFO) << "Send ack data";
            auto endpoint = endpoint_store_.getOrCreateEndpoint(entry.first);
            Request *request = new Request{
                .addr = {hdr, nullptr},
                .length = {sizeof(PacketHeader), 0},
                .lkey = {endpoint_store_.context().key(hdr).first, 0}};
            int ret = endpoint->postSendRequest({request});
            if (ret < 0)
                return -1;
            entry.second.acked_next_recv_sn = entry.second.next_recv_sn;
            entry.second.resend_ack = false;
        }
        return 0;
    }

    void EventLoop::updateRTO(uint64_t rtt)
    {
        if (recv_srtt_ == 0)
        {
            recv_srtt_ = rtt;
            recv_rttval_ = rtt / 2;
        }
        else
        {
            long delta = rtt - recv_srtt_;
            if (delta < 0)
                delta = -delta;
            recv_rttval_ = (3 * recv_rttval_ + delta) / 4;
            recv_srtt_ = (7 * recv_srtt_ + rtt) / 8;
            if (recv_srtt_ < 1)
                recv_srtt_ = 1;
        }
        uint64_t rto = recv_srtt_ + 4 * recv_rttval_;
        recv_rto_ = std::min(std::max(kMinRTO, rto), kMaxRTO);
    }

    void EventLoop::runReceiveCallbacks()
    {
        for (auto &entry : protocol_->receive_queue_)
        {
            auto peer_name = entry.first;
            while (entry.second.hasRemainingFragment())
            {
                bool progress = false;
                for (auto iter = recv_buffer_.begin(); iter != recv_buffer_.end(); iter++)
                {
                    auto &cid = sessions_[peer_name].cid;
                    auto &next_recv_sn = sessions_[peer_name].next_recv_sn;
                    cid = protocol_->session_id_manager_.getSidByReceiver(peer_name);
                    if (iter->hdr.cid == cid && iter->hdr.sn == next_recv_sn)
                    {
                        auto buffer = entry.second.popFragment();
                        memcpy(buffer.addr, iter->data, buffer.length);
                        // packet_pool_.freePacket(iter->data);
                        recv_buffer_.erase(iter);
                        ++next_recv_sn;
                        ++completed_packets_;
                        progress = true;
                        break;
                    }
                }
                if (!progress)
                    break;
            }
        }
    }

    int EventLoop::ProcessReceivedPacket(uint64_t current_ts, ibv_wc &wc)
    {
        Packet packet;
        Request *request = (Request *)wc.wr_id;
        PacketHeader *hdr = (PacketHeader *)((uint64_t)request->addr[0] + 40);
        DecodePacket(packet.hdr, hdr);
        packet.data = hdr + 1;
        switch (packet.hdr.cmd)
        {
        case CMD_DATA:
        {
            auto peer_name = protocol_->session_id_manager_.getEndPointByReceiver(hdr->cid);
            auto &state = sessions_[peer_name];
            if (packet.hdr.sn < state.next_recv_sn || packet.hdr.sn >= state.next_recv_sn + recv_wnd_)
                break;
            if (packet.hdr.sn >= state.next_recv_sn)
            {
                bool dup = false;
                for (auto &item : recv_buffer_)
                {
                    if (item.hdr.sn == packet.hdr.sn)
                    {
                        dup = true;
                        break;
                    }
                }
                if (!dup)
                    recv_buffer_.push_back(packet);
                else if (state.next_recv_sn == state.acked_next_recv_sn)
                    state.resend_ack = true;
            }
            received_packets_++;
            break;
        }

        case CMD_ACK:
            while (true)
            {
                bool found = false;
                for (auto iter = send_buffer_.begin(); iter != send_buffer_.end(); iter++)
                {
                    if (iter->hdr.sn < packet.hdr.una)
                    {
                        ++completed_packets_;
                        send_buffer_.erase(iter);
                        found = true;
                        break;
                    }
                }
                if (!found)
                    break;
            }

            LOG(INFO) << "*** Receive ack data";
            updateRTO(current_ts - packet.hdr.ts);
            // timely_.update(current_ts - packet.hdr.ts, current_ts);
            break;

        default:
            LOG(INFO) << "Unknown cmd: " << packet.hdr.cmd;
            break;
        }

        return postReceiveWorkRequest();
    }

    int EventLoop::postReceiveWorkRequest()
    {
        auto &context = endpoint_store_.context();
        PacketHeader *hdr = packet_pool_.allocatePacket();
        if (!hdr)
            return -1;
        Request *request = new Request{
            .addr = {hdr, nullptr},
            .length = {kPacketStorageSize, 0},
            .lkey = {context.key(hdr).first, 0}};
        int ret = endpoint_store_.postReceiveRequest({request});
        assert(ret == 1);
        return 0;
    }
} // namespace rapid
