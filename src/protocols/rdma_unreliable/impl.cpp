// Copyright 2024 Feng Ren

#include "impl.h"

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

    RdmaUnreliableProtocol::RdmaUnreliableProtocol()
        : valid_(false), background_running_(false) {}

    RdmaUnreliableProtocol::~RdmaUnreliableProtocol()
    {
        deconstruct();
    }

    int RdmaUnreliableProtocol::construct(const std::string &local_hostname,
                                          const std::string &device_name,
                                          uint8_t rdma_port,
                                          int gid_index)
    {
        int ret = context_.construct(local_hostname, device_name, rdma_port, gid_index);
        if (ret)
            return ret;
        background_running_ = true;
        background_worker_ = std::thread(&RdmaUnreliableProtocol::runBackgroundWorker, this);
        valid_ = true;
        return 0;
    }

    int RdmaUnreliableProtocol::deconstruct()
    {
        if (!valid_)
            return 0;
        if (background_running_.exchange(false))
            background_worker_.join();
        context_.deconstruct();
        valid_ = false;
        return 0;
    }

    int RdmaUnreliableProtocol::prepareConnection(const std::string &peer_name, Attributes &local)
    {
        auto endpoint = context_.getOrCreateEndpoint(peer_name);
        if (!endpoint)
            return -1;
        local["name"] = context_.localHostname();
        local["lid"] = std::to_string(context_.lid());
        local["gid"] = context_.gid();
        local["qp"] = ToString(endpoint->qpNum());
        return 0;
    }

    int RdmaUnreliableProtocol::setupConnection(const std::string &peer_name, const Attributes &peer)
    {
        auto endpoint = context_.getOrCreateEndpoint(peer_name);
        if (!endpoint)
            return -1;
        if (!peer.count("lid") || !peer.count("gid") || !peer.count("qp"))
            return -1;
        auto lid = (uint16_t)std::stoi(peer.at("lid"));
        auto gid = peer.at("gid");
        auto qp_num_list = FromString(peer.at("qp"));
        int ret = endpoint->setupConnection(gid, lid, qp_num_list);
        if (ret)
            return ret;
        return 0;
    }

    int RdmaUnreliableProtocol::freeTask(TaskID task_id)
    {
        task_map_lock_.lock();
        task_map_.erase(task_id);
        task_map_lock_.unlock();
        return 0;
    }

    TaskID RdmaUnreliableProtocol::send(const std::vector<std::string> &peer_name_list,
                                        const std::vector<Buffer> &buffer_list)
    {
        auto task = allocateTask(SEND);
        if (!task)
            return -1;

        for (auto &peer_name : peer_name_list)
        {
            for (auto &buffer : buffer_list)
            {
                auto lkey = context_.key(buffer.addr).first;
                if (lkey == 0)
                {
                    LOG(ERROR) << "Buffer " << buffer.addr << " not registered";
                    freeTask(task->id);
                    return -1;
                }

                auto request = new Request{
                    .addr = buffer.addr,
                    .length = buffer.length,
                    .lkey = lkey,
                    .status = PENDING,
                    .peer_name = peer_name};
                task->request_list.push_back(request);
            }

            auto endpoint = context_.getOrCreateEndpoint(peer_name);
            if (!endpoint || !endpoint->connected())
                return -1;

            // TODO it should be executed in background!!!
            int ret = endpoint->postSendRequest(task->request_list);
            if (ret != (int)buffer_list.size())
            {
                LOG(INFO) << "Unable to post request";
                return -1;
            }
        }

        return task->id;
    }

    TaskID RdmaUnreliableProtocol::receive(const std::string &peer_name,
                                           const std::vector<Buffer> &buffer_list)
    {
        auto task = allocateTask(RECEIVE);
        if (!task)
            return -1;

        for (auto &buffer : buffer_list)
        {
            auto lkey = context_.key(buffer.addr).first;
            if (lkey == 0)
            {
                LOG(ERROR) << "Buffer " << buffer.addr << " not registered";
                freeTask(task->id);
                return -1;
            }

            auto request = new Request{
                .addr = buffer.addr,
                .length = buffer.length,
                .lkey = lkey,
                .status = PENDING,
                .peer_name = peer_name};
            task->request_list.push_back(request);
        }

        auto endpoint = context_.getOrCreateEndpoint(peer_name);
        if (!endpoint || !endpoint->connected())
            return -1;

        // TODO it should be executed in background!!!
        int ret = endpoint->postReceiveRequest(task->request_list);
        if (ret != (int)buffer_list.size())
        {
            LOG(INFO) << "Unable to post request";
            return -1;
        }

        return task->id;
    }

    Status RdmaUnreliableProtocol::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return UNKNOWN;
        size_t local_transferred_bytes = 0;
        Status summary = SUCCESS;
        for (auto &entry : task->request_list)
        {
            if (entry->status == SUCCESS)
                local_transferred_bytes += entry->length;
            if (entry->status == PENDING && summary == SUCCESS)
                summary = PENDING;
            if (entry->status == FAILED)
                summary = entry->status;
        }
        if (transferred_bytes)
            *transferred_bytes = local_transferred_bytes;
        return summary;
    }

    std::shared_ptr<RdmaUnreliableProtocol::Task> RdmaUnreliableProtocol::allocateTask(RequestType type)
    {
        int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        auto task = std::make_shared<Task>(type, task_id);
        if (!task)
            return nullptr;
        task_map_lock_.lock();
        task_map_[task_id] = task;
        task_map_lock_.unlock();
        return task;
    }

    std::shared_ptr<RdmaUnreliableProtocol::Task> RdmaUnreliableProtocol::getTaskById(TaskID task_id)
    {
        RWSpinlock::ReadGuard guard(task_map_lock_);
        if (!task_map_.count(task_id))
            return nullptr;
        return task_map_[task_id];
    }

    int RdmaUnreliableProtocol::registerLocalMemory(void *addr, size_t length)
    {
        return context_.registerMemoryRegion(addr, length, IBV_ACCESS_LOCAL_WRITE);
    }

    int RdmaUnreliableProtocol::unregisterLocalMemory(void *addr)
    {
        return context_.unregisterMemoryRegion(addr);
    }

    void RdmaUnreliableProtocol::runBackgroundWorker()
    {
        const static size_t kPollCount = 64;
        while (background_running_)
        {
            for (int cq_index = 0; cq_index < 2; cq_index++)
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
                        LOG(ERROR) << "Worker: Process failed for slice (addr: " << request->addr
                                   << ", length: " << request->length
                                   << ", lkey: " << request->lkey
                                   << ", local_nic: " << context_.deviceName()
                                   << "): " << ibv_wc_status_str(wc[i].status);
                        context_.deleteEndpoint(request->peer_name);
                        request->status = FAILED;
                    }
                    else
                        request->status = SUCCESS;
                }
            }
        }
    }
}