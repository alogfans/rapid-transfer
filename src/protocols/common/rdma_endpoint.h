// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "protocol.h"
#include "rapid_transfer.h"
#include "rdma_context.h"

#include <queue>

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

    class RdmaEndPoint
    {
    public:
        enum Status
        {
            INITIALIZING,
            UNCONNECTED,
            CONNECTED,
        };

    public:
        RdmaEndPoint(RdmaContext &context);

        ~RdmaEndPoint();

        int construct(ibv_cq *send_cq,
                      ibv_cq *recv_cq,
                      size_t num_qp_list = 2,
                      size_t max_sge = 4,
                      size_t max_wr = 256,
                      size_t max_inline = 64);

        int deconstruct();

    public:
        bool connected() const
        {
            return status_.load(std::memory_order_relaxed) == CONNECTED;
        }

        void disconnect();

        int destroyQP();

        void disconnectUnlocked();

        std::vector<uint32_t> qpNum() const;

        int setupConnection(const std::string &peer_gid, uint16_t peer_lid, std::vector<uint32_t> peer_qp_num_list);

        int setupConnection(int qp_index, const std::string &peer_gid, uint16_t peer_lid, uint32_t peer_qp_num);

        int postSendRequest(const std::vector<Request *> &request_list);

        int postReceiveRequest(const std::vector<Request *> &request_list);

    private:
        RdmaContext &context_;
        std::atomic<Status> status_;

        RWSpinlock lock_;
        std::vector<ibv_qp *> qp_list_;

        std::string peer_nic_path_;

        volatile int *send_wr_depth_list_, *recv_wr_depth_list_;
        int max_wr_depth_;

        volatile bool active_;
    };

}

#endif // RDMA_ENDPOINT_H
