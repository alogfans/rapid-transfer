// Copyright 2024 Feng Ren

#include "impl.h"
#include "event_loop.h"

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
        : valid_(false),
          endpoint_store_(context_),
          next_task_id_(0)
    {
        event_loop_ = new EventLoop(this);
    }

    RdmaUnreliableProtocol::~RdmaUnreliableProtocol()
    {
        deconstruct();
        delete event_loop_;
    }

    int RdmaUnreliableProtocol::construct(const std::string &local_hostname,
                                          const std::string &device_name,
                                          uint8_t rdma_port,
                                          int gid_index)
    {
        int ret = context_.construct(local_hostname, device_name, rdma_port, gid_index);
        if (ret)
            return ret;
        ret = endpoint_store_.construct(context_.cq(SEND_CQ), context_.cq(RECV_CQ));
        if (ret)
            return ret;
        ret = event_loop_->start();
        if (ret)
            return ret;
        valid_ = true;
        return 0;
    }

    int RdmaUnreliableProtocol::deconstruct()
    {
        if (!valid_)
            return 0;
        event_loop_->join();
        endpoint_store_.deconstruct();
        context_.deconstruct();
        valid_ = false;
        return 0;
    }

    int RdmaUnreliableProtocol::prepareConnection(const std::string &peer_name, Attributes &local)
    {
        auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_name);
        if (!endpoint)
            return -1;
        local["name"] = context_.localHostname();
        local["lid"] = std::to_string(context_.lid());
        local["gid"] = context_.gid();
        local["qp"] = ToString(endpoint->qpNum());
        local["session"] = std::to_string(session_id_manager_.allocateSidByReceiver(peer_name));
        return 0;
    }

    int RdmaUnreliableProtocol::setupConnection(const std::string &peer_name, const Attributes &peer)
    {
        auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_name);
        if (!endpoint)
            return -1;
        if (!peer.count("lid") || !peer.count("gid") || !peer.count("qp") || !peer.count("session"))
            return -1;
        auto lid = (uint16_t)std::stoi(peer.at("lid"));
        auto gid = peer.at("gid");
        auto qp_num_list = FromString(peer.at("qp"));
        auto session_id = std::stoi(peer.at("session"));
        int ret = endpoint->setupConnection(gid, lid, qp_num_list);
        if (ret)
            return ret;
        session_id_manager_.setSidBySender(peer_name, session_id);
        return 0;
    }

    int RdmaUnreliableProtocol::freeTask(TaskID task_id)
    {
        task_info_.erase(task_id);
        return 0;
    }

    TaskID RdmaUnreliableProtocol::send(const std::vector<std::string> &peer_name_list,
                                        const std::vector<Buffer> &buffer_list)
    {
        TaskInfo info;
        for (auto &peer_name : peer_name_list)
        {
            auto &queue = send_queue_[peer_name];
            info.fragment_id_map[peer_name] = queue.push(buffer_list);
        }
        auto task_id = next_task_id_.fetch_add(1);
        task_info_[task_id] = info;
        return task_id;
    }

    TaskID RdmaUnreliableProtocol::receive(const std::string &peer_name,
                                           const std::vector<Buffer> &buffer_list)
    {
        TaskInfo info;
        auto &queue = receive_queue_[peer_name];
        info.fragment_id_map[peer_name] = queue.push(buffer_list);
        auto task_id = next_task_id_.fetch_add(1);
        task_info_[task_id] = info;
        return task_id;
    }

    Status RdmaUnreliableProtocol::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        if (!task_info_.count(task_id))
            return UNKNOWN;
        auto &task = task_info_[task_id];
        for (auto entry : task.fragment_id_map)
        {
            auto peer_name = entry.first;
            if (nextAckFragmentId(peer_name) < entry.second.second)
                return PENDING;
            if (hasLostFragment(peer_name, entry.second))
                return FAILED;
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

    uint64_t RdmaUnreliableProtocol::nextAckFragmentId(const std::string &peer_name)
    {
        return event_loop_->completedPackets();
    }

    bool RdmaUnreliableProtocol::hasLostFragment(const std::string &peer_name,
                                                 std::pair<uint64_t, uint64_t> region)
    {
        return false;
    }
}