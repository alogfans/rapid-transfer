// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "rapid_transfer.h"
#include "rdma_context.h"

#include <queue>

namespace rapid
{
    enum RequestType
    {
        SEND,
        RECEIVE
    };

    struct Task;

    struct Request
    {
        void *addr;
        size_t length;
        uint32_t lkey;
        Task *task;
        volatile int *qp_depth;
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

        int construct(ibv_cq *cq,
                      size_t num_qp_list = 2,
                      size_t max_sge = 4,
                      size_t max_wr = 256,
                      size_t max_inline = 64);

        int deconstruct();

        bool active() const { return active_; }

        void set_active(bool flag) { active_ = flag; }

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

        int postRequest(RequestType type, const std::vector<Request *> &request_list);

    private:
        RdmaContext &context_;
        std::atomic<Status> status_;

        RWSpinlock lock_;
        std::vector<ibv_qp *> qp_list_;

        std::string peer_nic_path_;

        volatile int *wr_depth_list_;
        int max_wr_depth_;

        volatile bool active_;
    };

}

#endif // RDMA_ENDPOINT_H
