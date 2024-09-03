// Copyright 2024 Feng Ren

#include "rdma_reliable_protocol.h"

#include <cassert>

namespace rapid
{
    static std::string ToString(const std::vector<uint32_t> &list)
    {
        std::ostringstream oss;
        for (const auto &entry : list)
            oss << " " << entry;
        return oss.str();
    }

    static std::vector<uint32_t> FromString(const std::string &str)
    {
        std::istringstream iss(str);
        std::vector<uint32_t> list;
        uint32_t val;
        while (iss >> val)
            list.push_back(val);
        return list;
    }

    RdmaReliableProtocol::RdmaReliableProtocol()
        : poll_worker_running_(false) {}

    int RdmaReliableProtocol::construct(const std::string &local_hostname,
                                        const std::string &device_name,
                                        uint8_t rdma_port,
                                        int gid_index)
    {
        int ret = context_.construct(local_hostname, device_name, rdma_port, gid_index);
        if (ret)
            return ret;
        poll_worker_running_ = true;
        poll_worker_ = std::thread(&RdmaReliableProtocol::runPollWorker, this);
        return 0;
    }

    int RdmaReliableProtocol::deconstruct()
    {
        if (poll_worker_running_.exchange(false))
            poll_worker_.join();
        context_.deconstruct();
        return 0;
    }

    TaskID RdmaReliableProtocol::allocateTask()
    {
        int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        auto task = std::make_shared<Task>();
        task_map_lock_.lock();
        task_map_[task_id] = task;
        task_map_lock_.unlock();
        return task_id;
    }

    int RdmaReliableProtocol::freeTask(TaskID task_id)
    {
        task_map_lock_.lock();
        task_map_.erase(task_id);
        task_map_lock_.unlock();
        return 0;
    }

    int RdmaReliableProtocol::prepareSend(TaskID task_id,
                                          std::vector<Attributes> &request_list,
                                          const std::vector<std::string> &target_list,
                                          const Attributes &attributes,
                                          const std::vector<Buffer> &buffer_list)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return -1;

        task->target_list = target_list;
        task->attributes = attributes;
        task->buffer_list = buffer_list;
        for (auto &target : task->target_list)
        {
            Attributes request;
            auto endpoint = context_.getOrCreateEndpoint(target);
            request["_name"] = target;
            request["_lid"] = std::to_string(context_.lid());
            request["_gid"] = context_.gid();
            request["_qp"] = ToString(endpoint->qpNum());
            for (auto &entry : attributes)
                request[entry.first] = entry.second;
            request_list.push_back(request);
        }

        return 0;
    }

    int RdmaReliableProtocol::issueSend(TaskID task_id, const std::vector<Attributes> &response_list)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return -1;

        for (size_t index = 0; index < task->target_list.size(); ++index)
        {
            auto &target = task->target_list[index];
            auto response = response_list[index];
            auto lid = (uint16_t)std::atoi(response["_lid"].c_str());
            auto gid = response["_gid"];
            auto qp_num_list = FromString(response["_qp"]);
            auto endpoint = context_.getOrCreateEndpoint(target);
            int ret = endpoint->setupConnection(gid, lid, qp_num_list);
            if (ret)
                return ret;

            std::vector<Request *> request_list;
            for (auto &buffer : task->buffer_list)
            {
                auto lkey = context_.key(buffer.addr).first;
                auto request = new Request{.addr = buffer.addr, .length = buffer.length, .lkey = lkey, .task = task.get()};
                request_list.push_back(request);
                task->total_packets++;
            }
            ret = endpoint->postRequest(RequestType::SEND, request_list);
            if (ret)
                return ret;
        }

        return 0;
    }

    int RdmaReliableProtocol::prepareReceive(TaskID task_id,
                                             const Attributes &request,
                                             Attributes &response,
                                             const RapidTransfer::OnReceiveCallback &on_receive)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return -1;

        Attributes request_clone = request;
        auto target = request_clone["_name"];
        assert(!target.empty());
        task->target_list.push_back(target);
        for (auto &entry : request_clone)
            if (!entry.first.empty() && entry.first[0] != '_')
                task->attributes[entry.first] = entry.second;

        auto lid = (uint16_t)std::atoi(request_clone["_lid"].c_str());
        auto gid = request_clone["_gid"];
        auto qp_num_list = FromString(request_clone["_qp"]);
        auto endpoint = context_.getOrCreateEndpoint(target);
        int ret = endpoint->setupConnection(gid, lid, qp_num_list);
        if (ret)
            return ret;

        response["_name"] = target;
        response["_lid"] = std::to_string(context_.lid());
        response["_gid"] = context_.gid();
        response["_qp"] = ToString(endpoint->qpNum());

        ret = on_receive(task_id, target, task->attributes, task->buffer_list);
        if (ret)
            return ret;

        std::vector<Request *> request_list;
        for (auto &buffer : task->buffer_list)
        {
            auto lkey = context_.key(buffer.addr).first;
            auto request = new Request{.addr = buffer.addr, .length = buffer.length, .lkey = lkey, .task = task.get()};
            request_list.push_back(request);
            task->total_packets++;
        }

        ret = endpoint->postRequest(RequestType::RECEIVE, request_list);
        if (ret)
            return ret;

        return 0;
    }

    int RdmaReliableProtocol::setFailedStatus(TaskID task_id)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return -1;
        task->mark_failed = true;
        return 0;
    }

    Status RdmaReliableProtocol::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return UNKNOWN;

        if (transferred_bytes)
            *transferred_bytes = task->transferred_bytes;

        if (task->success_packets + task->failed_packets < task->total_packets)
            return PENDING;

        if (task->mark_failed || task->failed_packets)
            return FAILED;

        return SUCCESS;
    }

    std::shared_ptr<Task> RdmaReliableProtocol::getTaskById(TaskID task_id)
    {
        RWSpinlock::ReadGuard guard(task_map_lock_);
        if (!task_map_.count(task_id))
            return nullptr;
        return task_map_[task_id];
    }

    int RdmaReliableProtocol::registerBuffer(void *addr, size_t length)
    {
        return context_.registerMemoryRegion(addr, length, IBV_ACCESS_LOCAL_WRITE);
    }

    int RdmaReliableProtocol::unregisterBuffer(void *addr)
    {
        return context_.unregisterMemoryRegion(addr);
    }

    void RdmaReliableProtocol::runPollWorker()
    {
        const static size_t kPollCount = 64;

        while (poll_worker_running_)
        {
            for (int cq_index = 0; cq_index < context_.cqCount(); cq_index++)
            {
                ibv_wc wc[kPollCount];
                int nr_poll = context_.poll(kPollCount, wc, cq_index);
                if (nr_poll < 0)
                {
                    LOG(ERROR) << "Worker: Failed to poll completion queues";
                    continue;
                }

                for (int i = 0; i < nr_poll; ++i)
                {
                    auto request = (Request *)wc[i].wr_id;
                    assert(request);
                    __sync_fetch_and_sub(request->qp_depth, 1);
                    if (wc[i].status != IBV_WC_SUCCESS)
                    {
                        LOG(ERROR) << "Worker: Process failed for slice ("
                                   << ", addr: " << request->addr
                                   << ", length: " << request->length
                                   << ", lkey: " << request->lkey
                                   << ", local_nic: " << context_.deviceName()
                                   << "): " << ibv_wc_status_str(wc[i].status);
                        request->task->failed_packets++;
                    }
                    else
                    {
                        request->task->success_packets++;
                        request->task->transferred_bytes += request->length;
                    }
                    delete request;
                }
            }
        }
    }
}