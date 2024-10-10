// rdma_endpoint.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_ud_endpoint.h"

#include <cassert>
#include <cstddef>
#include <glog/logging.h>

const static uint8_t MAX_HOP_LIMIT = 16;

namespace rapid
{
    RdmaUDEndPoint::RdmaUDEndPoint(RdmaUDEndPointStore &store)
        : store_(store),
          connected_(false),
          ah_(nullptr) {}

    RdmaUDEndPoint::~RdmaUDEndPoint()
    {
        if (ah_)
        {
            ibv_destroy_ah(ah_);
            ah_ = nullptr;
        }
    }

    int RdmaUDEndPoint::postSendRequest(const std::vector<Request *> &request_list)
    {
        if (!connected_)
        {
            LOG(ERROR) << "Failed to post send request: not connected";
            return -1;
        }
        auto remote_qpn = peer_qp_num_list_[lrand48() % peer_qp_num_list_.size()];
        return store_.postSendRequest(request_list, ah_, remote_qpn);
    }

    int RdmaUDEndPoint::postReceiveRequest(const std::vector<Request *> &request_list)
    {
        if (!connected_)
        {
            LOG(ERROR) << "Failed to post send request: not connected";
            return -1;
        }
        return store_.postReceiveRequest(request_list);
    }

    int RdmaUDEndPoint::setupConnection(const std::string &peer_gid, uint16_t peer_lid, std::vector<uint32_t> peer_qp_num_list)
    {
        if (connected_)
        {
            LOG(ERROR) << "Failed to post send request: already connected";
            return -1;
        }
        struct ibv_ah_attr ah_attr;
        memset(&ah_attr, 0, sizeof(ah_attr));
        ibv_gid peer_gid_raw;
        std::istringstream iss(peer_gid);
        for (int i = 0; i < 16; ++i)
        {
            int value;
            iss >> std::hex >> value;
            peer_gid_raw.raw[i] = static_cast<uint8_t>(value);
            if (i < 15)
                iss.ignore(1, ':');
        }
        auto &context = store_.context();
        ah_attr.grh.dgid = peer_gid_raw;
        ah_attr.grh.sgid_index = context.gidIndex();
        ah_attr.grh.hop_limit = MAX_HOP_LIMIT;
        ah_attr.dlid = peer_lid;
        ah_attr.sl = 0;
        ah_attr.src_path_bits = 0;
        ah_attr.static_rate = 0;
        ah_attr.is_global = 1;
        ah_attr.port_num = context.portNum();
        ah_ = ibv_create_ah(context.pd(), &ah_attr);
        if (!ah_)
        {
            PLOG(ERROR) << "Failed to create AH";
            return -1;
        }
        peer_gid_ = peer_gid;
        peer_lid_ = peer_lid;
        peer_qp_num_list_ = peer_qp_num_list;
        connected_ = true;
        return 0;
    }
}
