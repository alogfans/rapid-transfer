// Copyright 2024 Feng Ren

#include "event_loop.h"
#include "impl.h"

namespace rapid
{
    EventLoop::EventLoop(RdmaUnreliableProtocol *protocol)
        : protocol_(protocol),
          endpoint_store_(protocol->endpoint_store_),
          next_task_id_(0),
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
            updateSendUna(current_ts);
        }
    }

    int EventLoop::submitRequests()
    {
        for (auto &entry : protocol_->send_queue_)
        {
            auto peer_name = entry.first;
            while (entry.second.hasRemainingFragment())
            {
                auto buffer = entry.second.popFragment();
                assert(buffer.addr);
                submitSendRequest(peer_name, buffer);
            }
        }

        for (auto &entry : protocol_->receive_queue_)
        {
            auto peer_name = entry.first;
            while (entry.second.hasRemainingFragment())
            {
                auto buffer = entry.second.popFragment();
                assert(buffer.addr);
                submitReceiveRequest(peer_name, buffer);
            }
        }
        return 0;
    }

    int EventLoop::submitSendRequest(const std::string &peer_name, const Buffer &buffer)
    {
        assert(buffer.length <= kMaxPayloadSize);
        total_packets_++;
        Packet packet;
        packet.data = buffer.addr;
        packet.hdr.ts = 0;
        packet.hdr.cid = 0;
        packet.hdr.cmd = CMD_DATA;
        packet.hdr.wnd = send_wnd_;
        packet.hdr.sn = next_send_sn_++;
        packet.hdr.len = buffer.length;
        packet.peer_name = peer_name;
        send_buffer_.push_back(packet);
        return 0;
    }

    TaskID EventLoop::submitReceiveRequest(const std::string &peer_name, const Buffer &buffer)
    {
        char *addr = (char *)buffer.addr;
        assert(buffer.length <= kMaxPayloadSize);
        total_packets_++;
        recv_queue_.push(Buffer{.addr = addr, .length = buffer.length});
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
            }

            switch (wc[i].opcode)
            {
            case IBV_WC_RECV:
                ProcessReceivedPacket(current_ts, wc[i]);
                break;
            case IBV_WC_SEND:
                delete request;
                break;
            default:
                LOG(ERROR) << "Unknown RDMA opcode: " << wc[i].opcode;
            }
        }

        return 0;
    }

    int EventLoop::sendDataPackets(uint64_t current_ts)
    {
        auto &context = endpoint_store_.context();
        for (auto &record : send_buffer_)
        {
            if ((record.hdr.ts != 0 && current_ts < record.hdr.ts + recv_rto_))
                continue;

            // if (send_credit_ <= 0)
            //     continue;
            // send_credit_--;

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

    void EventLoop::updateSendUna(uint64_t current_ts)
    {
        uint32_t next_send_una = next_send_sn_;
        if (!send_buffer_.empty())
            next_send_una = std::min(next_send_una, send_buffer_.front().hdr.sn);
        send_una_ = next_send_una;

        // const static uint64_t kCreditUpdateInterval = 100;
        // if (current_ts - send_credit_ts_ > kCreditUpdateInterval)
        // {
        //     const uint64_t kCreditUpdateValue =
        //         timely_.rate() / (1000000 / kCreditUpdateInterval);
        //     send_credit_ = kCreditUpdateValue * (current_ts - send_credit_ts_) /
        //                 kCreditUpdateInterval;
        //     send_credit_ts_ = current_ts;
        // }
    }

    int EventLoop::sendAckPackets(uint64_t current_ts)
    {
        thread_local uint64_t last_received_packets = 0;
        if (last_received_packets < received_packets_)
        {
            PacketHeader *hdr = packet_pool_.allocatePacket();
            memset(hdr, 0, sizeof(PacketHeader));
            hdr->cid = 0;
            hdr->cmd = CMD_ACK;
            hdr->wnd = send_wnd_;
            hdr->una = next_recv_sn_;
            EncodePacket(hdr, *hdr);
            // TODO from whom?
            LOG(INFO) << "Send ack data";
            auto endpoint = endpoint_store_.getOrCreateEndpoint("optane20");
            Request *request = new Request{
                .addr = {hdr, nullptr},
                .length = {sizeof(PacketHeader), 0},
                .lkey = {endpoint_store_.context().key(hdr).first, 0}};
            int ret = endpoint->postSendRequest({request});
            if (ret < 0)
                return -1;
            last_received_packets = received_packets_;
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
        while (!recv_queue_.empty())
        {
            bool progress = false;
            for (auto iter = recv_buffer_.begin(); iter != recv_buffer_.end(); iter++)
            {
                if (iter->hdr.sn == next_recv_sn_)
                {
                    auto buffer = recv_queue_.front();
                    recv_queue_.pop();
                    memcpy(buffer.addr, iter->data, buffer.length);
                    // packet_pool_.freePacket(iter->data);
                    recv_buffer_.erase(iter);
                    ++next_recv_sn_;
                    ++completed_packets_;
                    progress = true;
                    break;
                }
            }
            if (!progress)
                break;
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
        case CMD_SEND:
            if (packet.hdr.sn < next_recv_sn_ || packet.hdr.sn >= next_recv_sn_ + recv_wnd_)
                break;

            if (packet.hdr.sn >= next_recv_sn_)
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
            }
            received_packets_++;
            break;

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
