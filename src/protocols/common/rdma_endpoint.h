// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "protocol.h"
#include "rapid_transfer.h"
#include "rdma_context.h"

namespace rapid
{
    struct Task;

    struct Request
    {
        void *addr;
        size_t length;
        uint32_t lkey;
        volatile int *qp_depth;
        volatile Status status;
        std::string peer_name;
    };

    struct RdmaEndPoint
    {
        RdmaEndPoint() {}

        virtual ~RdmaEndPoint() {}

        virtual bool connected() const = 0;

        virtual std::vector<uint32_t> qpNum() const = 0;

        virtual int setupConnection(const std::string &peer_gid, uint16_t peer_lid, std::vector<uint32_t> peer_qp_num_list) = 0;

        virtual int postSendRequest(const std::vector<Request *> &request_list) = 0;

        virtual int postReceiveRequest(const std::vector<Request *> &request_list) = 0;
    };
}

#endif // RDMA_ENDPOINT_H
