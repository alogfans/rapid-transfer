// Copyright 2024 Feng Ren

#include "worker.h"

namespace rapid
{
    RdmaUnreliableWorker::RdmaUnreliableWorker(RdmaUDEndPointStore &endpoint_store)
        : endpoint_store_(endpoint_store),
          next_task_id_(0),
          workers_running_(false)
    {
    }

    RdmaUnreliableWorker::~RdmaUnreliableWorker() {}

    int RdmaUnreliableWorker::start()
    {
        setupPacketPool();
        workers_running_ = true;
        worker_thread_list_.emplace_back(std::thread(std::bind(&RdmaUnreliableWorker::worker, this)));
        return 0;
    }

    int RdmaUnreliableWorker::join()
    {
        if (!workers_running_.exchange(false))
            return 0;
        for (auto &entry : worker_thread_list_)
            entry.join();
        destroyPacketPool();
        return 0;
    }

    int RdmaUnreliableWorker::submitSendRequest(const std::vector<std::string> &peer_name_list,
                                                const std::vector<Buffer> &buffer_list)
    {
        RWSpinlock::WriteGuard lock_guard(worker_lock_);
        int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        auto task = std::make_shared<Task>(SEND, task_id);
        if (!task)
            return -1;
        assert(peer_name_list.size() == 1);
        for (const auto &buffer : buffer_list)
        {
            char *addr = (char *)buffer.addr;
            size_t length = buffer.length;
            task->total_bytes += length;
            total_packets_++;
            while (length > 0)
            {
                size_t current_length = std::min(length, kMaxPayloadSize);
                Packet packet;
                packet.data = addr;
                packet.hdr.ts = 0;
                packet.hdr.cid = 0;
                packet.hdr.cmd = CMD_DATA;
                packet.hdr.wnd = send_wnd_;
                packet.hdr.sn = next_send_sn_++;
                packet.hdr.len = current_length;
                packet.peer_name = peer_name_list[0];
                send_buffer_.push_back(packet);
                addr += current_length;
                length -= current_length;
            }
        }
        task_map_[task_id] = task;
        return task->id;
    }

    int RdmaUnreliableWorker::submitReceiveRequest(const std::string &peer_name,
                                                   const std::vector<Buffer> &buffer_list)
    {
        RWSpinlock::WriteGuard lock_guard(worker_lock_);
        int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        auto task = std::make_shared<Task>(RECEIVE, task_id);
        if (!task)
            return -1;
        for (const auto &buffer : buffer_list)
        {
            char *addr = (char *)buffer.addr;
            size_t length = buffer.length;
            task->total_bytes += length;
            total_packets_++;
            while (length > 0)
            {
                size_t current_length = std::min(length, kMaxPayloadSize);
                recv_queue_.push(Buffer{.addr = addr, .length = current_length});
                addr += current_length;
                length -= current_length;
            }
        }
        task_map_[task_id] = task;
        return task->id;
    }

    Status RdmaUnreliableWorker::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return Status::UNKNOWN;
        if (transferred_bytes)
            *transferred_bytes = task->transferred_bytes;
        if (task->status == FAILED)
            return task->status;
        if (completed_packets_ < total_packets_)
            return PENDING;
        return SUCCESS;
    }

    int RdmaUnreliableWorker::freeTask(TaskID task_id)
    {
        worker_lock_.lock();
        task_map_.erase(task_id);
        worker_lock_.unlock();
        return 0;
    }

    std::shared_ptr<RdmaUnreliableWorker::Task> RdmaUnreliableWorker::getTaskById(TaskID task_id)
    {
        RWSpinlock::ReadGuard guard(worker_lock_);
        if (!task_map_.count(task_id))
            return nullptr;
        return task_map_[task_id];
    }

    void RdmaUnreliableWorker::worker()
    {
        for (uint32_t i = 0; i < recv_wnd_; ++i)
            postReceiveWorkRequest();
        while (workers_running_)
        {
            RWSpinlock::WriteGuard lock_guard(worker_lock_);
            uint64_t current_ts = GetCurrentTimeInUsec();
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

    int RdmaUnreliableWorker::pollCompletedPackets(int cq_index, uint64_t current_ts)
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

    int RdmaUnreliableWorker::sendDataPackets(uint64_t current_ts)
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
            PacketHeader *hdr = allocatePacket();
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

    void RdmaUnreliableWorker::updateSendUna(uint64_t current_ts)
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

    int RdmaUnreliableWorker::sendAckPackets(uint64_t current_ts)
    {
        thread_local uint64_t last_received_packets = 0;
        if (last_received_packets < received_packets_)
        {
            PacketHeader *hdr = allocatePacket();
            memset(hdr, 0, sizeof(PacketHeader));
            hdr->cid = 0;
            hdr->cmd = CMD_ACK;
            hdr->wnd = send_wnd_;
            hdr->una = next_recv_sn_;
            EncodePacket(hdr, *hdr);
            // TODO from whom?
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

    void RdmaUnreliableWorker::updateRTO(uint64_t rtt)
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

    void RdmaUnreliableWorker::runReceiveCallbacks()
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
                    // freePacket(iter->data);
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

    int RdmaUnreliableWorker::ProcessReceivedPacket(uint64_t current_ts, ibv_wc &wc)
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

            updateRTO(current_ts - packet.hdr.ts);
            // timely_.update(current_ts - packet.hdr.ts, current_ts);
            break;

        default:
            LOG(INFO) << "Unknown cmd: " << packet.hdr.cmd;
            break;
        }

        return postReceiveWorkRequest();
    }

    int RdmaUnreliableWorker::postReceiveWorkRequest()
    {
        auto &context = endpoint_store_.context();
        PacketHeader *hdr = allocatePacket();
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
