// Copyright 2024 Feng Ren

#ifndef RDMA_MULTICAST_H
#define RDMA_MULTICAST_H

#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <functional>
#include <string>
#include <vector>

#include "rdma_endpoint.h"

namespace rapid {
class RdmaMulticastContext {
   public:
    RdmaMulticastContext();

    ~RdmaMulticastContext();

    int construct(const std::string &local_addr,
                  const std::string &multicast_addr, size_t num_connections);

    int deconstruct();

    int registerMemoryRegion(void *addr, size_t length, int access);

    int unregisterMemoryRegion(void *addr);

    std::pair<uint32_t, uint32_t> key(void *addr, int conn_index = 0);

   public:
    int postSendRequest(const std::vector<Request *> &request_list,
                        int conn_index = 0);

    int postReceiveRequest(const std::vector<Request *> &request_list,
                           int conn_index = 0);

    int poll(int num_entries, ibv_wc *wc, int conn_index = 0);

    void setInitQpHook(std::function<int(ibv_qp *)> on_init_qp_hook) {
        on_init_qp_hook_ = on_init_qp_hook;
    }

   private:
    struct Connection;

    int createQueuePair(Connection *connection);

    int onAddressResolved(void *context);

    int onMulticastJoin(void *context, struct rdma_ud_param *param);

    int processEvents();

   private:
    rdma_event_channel *event_channel_;

    struct Connection {
        Connection()
            : cm_id(nullptr),
              pd(nullptr),
              cq(nullptr),
              ah(nullptr),
              remote_qpn(0),
              remote_qkey(0),
              send_wr_depth(0),
              recv_wr_depth(0) {}
        rdma_cm_id *cm_id;
        ibv_pd *pd;
        ibv_cq *cq;
        ibv_ah *ah;
        uint32_t remote_qpn;
        uint32_t remote_qkey;
        volatile int send_wr_depth;
        volatile int recv_wr_depth;
        std::vector<ibv_mr *> memory_regions;
    };

    std::vector<Connection> connections_;
    struct sockaddr_storage local_addr_storage_, multicast_addr_storage_;
    struct sockaddr *local_addr_, *multicast_addr_;
    std::atomic_bool running_;
    std::atomic_int num_active_connections_;
    std::thread event_thread_;

    std::function<int(ibv_qp *)> on_init_qp_hook_;
};
}  // namespace rapid

#endif  // RDMA_MULTICAST_H
