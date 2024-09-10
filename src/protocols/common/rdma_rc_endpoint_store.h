// rdma_rc_endpoint_store.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_RC_ENDPOINT_STORE_H
#define RDMA_RC_ENDPOINT_STORE_H

#include "rapid_transfer.h"
#include "rdma_context.h"
#include "rdma_rc_endpoint.h"

namespace rapid
{
    class RdmaRCEndPointStore
    {
    public:
        RdmaRCEndPointStore(RdmaContext &context) : context_(context) {}

        ~RdmaRCEndPointStore()
        {
            endpoint_map_.clear();
        }

    public:
        std::shared_ptr<RdmaRCEndPoint> getOrCreateEndpoint(const std::string &peer_nic_path)
        {
            if (peer_nic_path.empty())
            {
                LOG(ERROR) << "Invalid peer NIC path";
                return nullptr;
            }

            {
                RWSpinlock::ReadGuard guard(endpoint_map_lock_);
                if (endpoint_map_.count(peer_nic_path))
                    return endpoint_map_[peer_nic_path];
            }

            RWSpinlock::WriteGuard guard(endpoint_map_lock_);
            if (endpoint_map_.count(peer_nic_path))
                return endpoint_map_[peer_nic_path];

            auto endpoint = std::make_shared<RdmaRCEndPoint>(context_);
            int ret = endpoint->construct(context_.cq(SEND_CQ), context_.cq(RECV_CQ));
            if (ret)
                return nullptr;
            endpoint_map_[peer_nic_path] = endpoint;
            return endpoint;
        }

        int deleteEndpoint(const std::string &peer_nic_path)
        {
            RWSpinlock::WriteGuard guard(endpoint_map_lock_);
            endpoint_map_.erase(peer_nic_path);
            return 0;
        }

    private:
        RWSpinlock endpoint_map_lock_;
        std::unordered_map<std::string, std::shared_ptr<RdmaRCEndPoint>> endpoint_map_;
        RdmaContext &context_;
    };

}

#endif // RDMA_RC_ENDPOINT_STORE_H
