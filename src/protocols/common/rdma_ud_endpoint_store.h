// rdma_ud_endpoint_store.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_UD_ENDPOINT_STORE_H
#define RDMA_UD_ENDPOINT_STORE_H

#include "rapid_transfer.h"
#include "rdma_context.h"
#include "rdma_endpoint.h"

namespace rapid {
class RdmaUDEndPoint;

class RdmaUDEndPointStore {
   public:
    RdmaUDEndPointStore(RdmaContext &context) : context_(context) {}

    ~RdmaUDEndPointStore() { deconstruct(); }

   public:
    int construct(ibv_cq *send_cq, ibv_cq *recv_cq, size_t num_qp_per_endpoint,
                  size_t max_sge_per_wr, size_t max_wr_per_qp,
                  size_t max_inline_bytes);

    int deconstruct();

    std::shared_ptr<RdmaUDEndPoint> getOrCreateEndpoint(
        const std::string &peer_nic_path);

    int deleteEndpoint(const std::string &peer_nic_path);

    int postSendRequest(const std::vector<Request *> &request_list, ibv_ah *ah,
                        uint32_t remote_qpn, int qp_index = 0);

    int postReceiveRequest(const std::vector<Request *> &request_list,
                           int qp_index = 0);

    std::vector<uint32_t> qpNum() const;

    RdmaContext &context() const { return context_; }

   private:
    int setupQueuePair(ibv_qp *qp);

   private:
    RWSpinlock endpoint_map_lock_;
    std::unordered_map<std::string, std::shared_ptr<RdmaUDEndPoint>>
        endpoint_map_;

    std::vector<ibv_qp *> qp_list_;
    volatile int *send_wr_depth_list_, *recv_wr_depth_list_;
    int max_wr_depth_;
    int max_inline_bytes_;

    ibv_cq *send_cq_, *recv_cq_;
    RdmaContext &context_;
};
}  // namespace rapid

#endif  // RDMA_UD_ENDPOINT_STORE_H
