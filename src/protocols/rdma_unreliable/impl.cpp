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
        : valid_(false), endpoint_store_(context_), background_worker_(endpoint_store_) {}

    RdmaUnreliableProtocol::~RdmaUnreliableProtocol()
    {
        deconstruct();
    }

    int RdmaUnreliableProtocol::construct(const std::string &local_hostname,
                                          const std::string &device_name,
                                          uint8_t rdma_port,
                                          int gid_index)
    {
        LOG(INFO) << local_hostname;
        int ret = context_.construct(local_hostname, device_name, rdma_port, gid_index);
        if (ret)
            return ret;
        ret = endpoint_store_.construct(context_.cq(SEND_CQ), context_.cq(RECV_CQ));
        if (ret)
            return ret;
        ret = background_worker_.start();
        if (ret)
            return ret;
        valid_ = true;
        return 0;
    }

    int RdmaUnreliableProtocol::deconstruct()
    {
        if (!valid_)
            return 0;
        background_worker_.join();
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
        return 0;
    }

    int RdmaUnreliableProtocol::setupConnection(const std::string &peer_name, const Attributes &peer)
    {
        auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_name);
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
        return background_worker_.freeTask(task_id);
    }

    TaskID RdmaUnreliableProtocol::send(const std::vector<std::string> &peer_name_list,
                                        const std::vector<Buffer> &buffer_list)
    {
        return background_worker_.submitSendRequest(peer_name_list, buffer_list);
    }

    TaskID RdmaUnreliableProtocol::receive(const std::string &peer_name,
                                           const std::vector<Buffer> &buffer_list)
    {
        return background_worker_.submitReceiveRequest(peer_name, buffer_list);
    }

    Status RdmaUnreliableProtocol::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        return background_worker_.getStatus(task_id, transferred_bytes);
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