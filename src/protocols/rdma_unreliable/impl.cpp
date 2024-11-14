// Copyright 2024 Feng Ren

#include "impl.h"
#include "packet_processor.h"

namespace rapid
{
    RdmaUnreliableProtocol::RdmaUnreliableProtocol()
        : running_(false),
          processors_(nullptr),
          next_task_id_(0)
    {
    }

    RdmaUnreliableProtocol::~RdmaUnreliableProtocol()
    {
        deconstruct();
    }

    int RdmaUnreliableProtocol::construct(const std::string &device_name,
                                          uint8_t rdma_port,
                                          int gid_index)
    {
        if (running_)
        {
            LOG(WARNING) << "RdmaUnreliableProtocol has been constructed";
            return 0;
        }

        int ret = context_.construct(device_name, rdma_port, gid_index);
        if (ret)
            return ret;

        processors_ = new PacketProcessor(*this);
        ret = processors_->construct();
        if (ret)
            return ret;

        running_ = true;
        return 0;
    }

    int RdmaUnreliableProtocol::deconstruct()
    {
        if (!running_)
            return 0;

        processors_->deconstruct();
        delete processors_;
        processors_ = nullptr;

        context_.deconstruct();

        running_ = false;
        return 0;
    }

    int RdmaUnreliableProtocol::prepareConnection(const std::string &peer_name, Attributes &local)
    {
        return processors_->prepareConnection(peer_name, local);
    }

    int RdmaUnreliableProtocol::setupConnection(const std::string &peer_name, const Attributes &peer)
    {
        return processors_->setupConnection(peer_name, peer);
    }

    int RdmaUnreliableProtocol::freeTask(TaskID task_id)
    {
        RWSpinlock::WriteGuard guard(task_lock_);
        task_info_.erase(task_id);
        return 0;
    }

    TaskID RdmaUnreliableProtocol::send(const std::string &peer_name,
                                        const std::vector<Buffer> &buffer_list)
    {
        RWSpinlock::WriteGuard guard(task_lock_);
        auto task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        TaskInfo info;
        info.type = SEND;
        int ret = processors_->issuePackets(peer_name, info.type, buffer_list, info.next_sn[peer_name]);
        if (ret)
        {
            LOG(ERROR) << "Failed to issue send packets";
            return ret;
        }
        task_info_[task_id] = info;
        return task_id;
    }

    TaskID RdmaUnreliableProtocol::receive(const std::string &peer_name,
                                           const std::vector<Buffer> &buffer_list)
    {
        RWSpinlock::WriteGuard guard(task_lock_);
        auto task_id = next_task_id_.fetch_add(1);
        TaskInfo info;
        info.type = RECEIVE;
        int ret = processors_->issuePackets(peer_name, info.type, buffer_list, info.next_sn[peer_name]);
        if (ret)
        {
            LOG(ERROR) << "Failed to issue receive packets";
            return ret;
        }
        task_info_[task_id] = info;
        return task_id;
    }

    Status RdmaUnreliableProtocol::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        RWSpinlock::ReadGuard guard(task_lock_);
        if (!task_info_.count(task_id))
            return UNKNOWN;
        auto &task = task_info_[task_id];
        for (auto entry : task.next_sn)
        {
            auto peer_name = entry.first;
            auto next_sn = processors_->nextPacketSN(peer_name, task.type);
            if (next_sn < entry.second)
                return processors_->connected(peer_name) ? PENDING : FAILED;
        }
        return SUCCESS;
    }

    int RdmaUnreliableProtocol::registerLocalMemory(void *addr, size_t length)
    {
        return context_.registerMemoryRegion(addr, length, IBV_ACCESS_LOCAL_WRITE);
    }

    int RdmaUnreliableProtocol::unregisterLocalMemory(void *addr)
    {
        return context_.unregisterMemoryRegion(addr);
    }
}
