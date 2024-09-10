// rdma_ud_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_UD_ENDPOINT_H
#define RDMA_UD_ENDPOINT_H

#include "rdma_endpoint.h"
#include "rdma_ud_endpoint_store.h"

namespace rapid
{
    class RdmaUDEndPoint : public RdmaEndPoint
    {
    public:
        RdmaUDEndPoint(RdmaUDEndPointStore &store);

        virtual ~RdmaUDEndPoint();

    public:
        virtual bool connected() const { return connected_; }

        virtual std::vector<uint32_t> qpNum() const { return store_.qpNum(); }

        virtual int setupConnection(const std::string &peer_gid, uint16_t peer_lid, std::vector<uint32_t> peer_qp_num_list);

        virtual int postSendRequest(const std::vector<Request *> &request_list);

        virtual int postReceiveRequest(const std::vector<Request *> &request_list);

    private:
        RdmaUDEndPointStore &store_;
        bool connected_;
        std::vector<uint32_t> peer_qp_num_list_;
        ibv_ah *ah_;
    };

}

#endif // RDMA_UD_ENDPOINT_H
