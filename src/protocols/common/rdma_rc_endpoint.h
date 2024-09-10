// rdma_rc_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_RC_ENDPOINT_H
#define RDMA_RC_ENDPOINT_H

#include "rdma_endpoint.h"

namespace rapid
{
    class RdmaRCEndPoint : public RdmaEndPoint
    {
    public:
        RdmaRCEndPoint(RdmaContext &context);

        virtual ~RdmaRCEndPoint();

        int construct(ibv_cq *send_cq,
                      ibv_cq *recv_cq,
                      size_t num_qp_list = 2,
                      size_t max_sge = 4,
                      size_t max_wr = 256,
                      size_t max_inline = 64);

        int deconstruct();

    public:
        virtual bool connected() const
        {
            return status_.load(std::memory_order_relaxed) == CONNECTED;
        }

        virtual std::vector<uint32_t> qpNum() const;

        virtual int setupConnection(const std::string &peer_gid, uint16_t peer_lid, std::vector<uint32_t> peer_qp_num_list);

        virtual int postSendRequest(const std::vector<Request *> &request_list);

        virtual int postReceiveRequest(const std::vector<Request *> &request_list);

    private:
        int setupConnection(int qp_index, const std::string &peer_gid, uint16_t peer_lid, uint32_t peer_qp_num);

        enum Status
        {
            INITIALIZING,
            UNCONNECTED,
            CONNECTED,
        };

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

#endif // RDMA_RC_ENDPOINT_H
