// Copyright 2024 Feng Ren

#ifndef CONTROLLER_H_
#define CONTROLLER_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <map>
#include <string>

#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_multicast.h"
#include "protocols/common/rdma_ud_endpoint_store.h"

namespace rapid {

struct NodeAddress {
    bool operator==(const NodeAddress &rhs) const {
        return memcmp(&gid, &rhs.gid, sizeof(ibv_gid)) == 0 &&
               qp_num == rhs.qp_num;
    }

    ibv_gid gid;
    uint32_t qp_num;
};

struct NodeAddressHash {
    std::size_t operator()(const NodeAddress &p) const {
        size_t gid_hash = 0;
        const char *gid_bytes = reinterpret_cast<const char *>(&p.gid);
        for (size_t i = 0; i < sizeof(ibv_gid); ++i) {
            gid_hash = gid_hash * 31 + gid_bytes[i];
        }
        size_t qp_num_hash = std::hash<uint32_t>()(p.qp_num);
        return gid_hash ^
               (qp_num_hash + 0x9e3779b9 + (gid_hash << 6) + (gid_hash >> 2));
    }
};

class Controller {
   public:
    Controller();

    ~Controller();

    Controller(const Controller &) = delete;
    Controller &operator=(const Controller &) = delete;

    int construct(const std::string &device_name, uint8_t rdma_port,
                  int gid_index);

    int deconstruct();

    int registerMcastNode(const std::string &multicast_addr);

    int unregisterMcastNode(const std::string &multicast_addr);

    std::shared_ptr<RdmaMulticastContext> queryMcastNode(
        const std::string &multicast_addr);

    int prepareConnection(const std::string &peer_addr, Attributes &local);

    int setupConnection(const std::string &peer_addr, const Attributes &peer);

    int findSession(ibv_gid &gid, uint32_t qp_num, uint8_t session);

    int findSession(const std::string &peer_addr, uint8_t session);

    std::shared_ptr<RdmaUDEndPoint> getOrCreateEndpoint(int session);

    RdmaContext &context() { return context_; }

    RdmaUDEndPointStore &endpointStore() { return endpoint_store_; }

   private:
    void registerNode(const std::string &peer_addr, ibv_gid &gid,
                      const std::vector<uint32_t> &qp_num_list);

   private:
    RdmaContext context_;
    RdmaUDEndPointStore endpoint_store_;

    std::string local_addr_;
    std::map<std::string, std::shared_ptr<RdmaMulticastContext>>
        multicast_context_map_;

    std::unordered_map<NodeAddress, int, NodeAddressHash> node_id_map_;
    std::unordered_map<std::string, int> peer_name_map_;
    std::unordered_map<int, std::string> peer_name_rev_map_;
    std::atomic<int> next_node_id_;

    RWSpinlock session_lock_;
};
}  // namespace rapid

#endif  // CONTROLLER_H_
