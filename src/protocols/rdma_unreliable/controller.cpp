// Copyright 2024 Feng Ren

#include "controller.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#include "protocols/common/rdma_ud_endpoint.h"

namespace rapid {
static std::string ToString(const std::vector<uint32_t> &list) {
    std::ostringstream oss;
    for (const auto &entry : list) oss << " " << entry;
    return oss.str();
}

static std::vector<uint32_t> FromString(const std::string &str) {
    std::istringstream iss(str);
    std::vector<uint32_t> list;
    uint32_t val;
    while (iss >> val) list.push_back(val);
    return list;
}

static std::string getSocketAddress(const std::string &device_name) {
    struct ifreq ifr;
    struct sockaddr_in *sin;
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "socket failed";
        return "";
    }
    memset(&ifr, 0, sizeof(ifr));
    strncpy(ifr.ifr_name, device_name.c_str(), IFNAMSIZ - 1);
    if (ioctl(fd, SIOCGIFADDR, &ifr) < 0) {
        PLOG(ERROR) << "ioctl failed";
        close(fd);
        return "";
    }
    close(fd);
    sin = (struct sockaddr_in *)&ifr.ifr_addr;
    return std::string(inet_ntoa(sin->sin_addr));
}

Controller::Controller() : endpoint_store_(context_), next_node_id_(0) {}

Controller::~Controller() { deconstruct(); }

int Controller::construct(const std::string &device_name, uint8_t rdma_port,
                          int gid_index) {
    local_addr_ = getSocketAddress(device_name);
    int ret = context_.construct(device_name, rdma_port, gid_index);
    if (ret) return ret;
    ret = endpoint_store_.construct(
        context_.cq(SEND_CQ), context_.cq(RECV_CQ),
        context_.config().num_qp_per_endpoint, context_.config().max_sge_per_wr,
        context_.config().max_wr_per_qp, context_.config().max_inline_bytes);
    return ret;
}

int Controller::deconstruct() {
    endpoint_store_.deconstruct();
    context_.deconstruct();
    return 0;
}

int Controller::joinMulticast(const std::string &multicast_addr) {
    // RWSpinlock::WriteGuard guard(session_lock_);
    if (multicast_context_map_.count(multicast_addr)) {
        LOG(ERROR) << "multicast address " << multicast_addr << " registered";
        return -1;
    }
    auto node_id = next_node_id_.fetch_add(1);
    auto context = std::make_shared<RdmaMulticastContext>();
    if (context->construct(local_addr_, multicast_addr)) return -1;
    context->setGroupId(node_id);
    multicast_context_map_[multicast_addr] = context;
    peer_name_map_[multicast_addr] = node_id;
    peer_name_rev_map_[node_id] = multicast_addr;
    return 0;
}

int Controller::leaveMulticast(const std::string &multicast_addr) {
    if (!multicast_context_map_.count(multicast_addr)) {
        LOG(ERROR) << "multicast address " << multicast_addr
                   << " not registered";
        return -1;
    }
    auto context = multicast_context_map_[multicast_addr];
    int ret = context->deconstruct();
    multicast_context_map_.erase(multicast_addr);
    return ret;
}

std::shared_ptr<RdmaMulticastContext> Controller::getMulticastContext(
    const std::string &multicast_addr) {
    if (!multicast_context_map_.count(multicast_addr)) return nullptr;
    auto context = multicast_context_map_[multicast_addr];
    return context;
}

int Controller::prepareConnection(const std::string &peer_addr,
                                  Attributes &local) {
    auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_addr);
    if (!endpoint) return -1;
    local["lid"] = std::to_string(context_.lid());
    local["gid"] = context_.gid();
    local["qp"] = ToString(endpoint->qpNum());
    std::vector<uint32_t> ext_qp_num_list = endpoint->qpNum();
    for (auto &entry : getMulticastContextMap()) {
        entry.second->qpNum(ext_qp_num_list);
    }
    local["ext_qp"] = ToString(ext_qp_num_list);
    return 0;
}

int Controller::setupConnection(const std::string &peer_addr,
                                const Attributes &peer) {
    auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_addr);
    if (!endpoint) return -1;
    if (!peer.count("lid") || !peer.count("gid") || !peer.count("qp") ||
        !peer.count("ext_qp")) {
        LOG(ERROR) << "invalid peer attributes";
        return -1;
    }
    auto lid = (uint16_t)std::stoi(peer.at("lid"));
    auto gid = peer.at("gid");
    auto qp_num_list = FromString(peer.at("qp"));
    auto ext_qp_num_list = FromString(peer.at("ext_qp"));
    int ret = endpoint->setupConnection(gid, lid, qp_num_list);
    if (ret) return ret;

    ibv_gid gid_raw;
    std::istringstream iss(gid);
    for (int i = 0; i < 16; ++i) {
        int value;
        iss >> std::hex >> value;
        gid_raw.raw[i] = static_cast<uint8_t>(value);
        if (i < 15) iss.ignore(1, ':');
    }
    registerNode(peer_addr, gid_raw, ext_qp_num_list);
    return 0;
}

void Controller::registerNode(const std::string &peer_addr, ibv_gid &gid,
                              const std::vector<uint32_t> &qp_num_list) {
    // RWSpinlock::WriteGuard guard(session_lock_);
    auto node_id = next_node_id_.fetch_add(1);
    for (auto qp_num : qp_num_list) {
        NodeAddress p{gid, qp_num};
        node_id_map_[p] = node_id;
    }
    peer_name_map_[peer_addr] = node_id;
    peer_name_rev_map_[node_id] = peer_addr;
}

int Controller::findSession(ibv_gid &gid, uint32_t qp_num, uint8_t session) {
    // RWSpinlock::ReadGuard guard(session_lock_);
    auto device_name = context_.deviceName();
    if (device_name.find("mlx5_bond") != device_name.npos) {
        int ans_cnt = 0;
        int index = -1;
        for (auto &entry : node_id_map_) 
            if (entry.first.qp_num == qp_num) {
                index = entry.second * 256 + session;
                ans_cnt++;
            }
        assert(ans_cnt <= 1);
        return index;
    } else {
        NodeAddress p{gid, qp_num};
        if (node_id_map_.count(p)) return node_id_map_[p] * 256 + session;
    }
    return -1;
}

int Controller::findSession(const std::string &peer_addr, uint8_t session) {
    // RWSpinlock::ReadGuard guard(session_lock_);
    if (!peer_name_map_.count(peer_addr)) return -1;
    return peer_name_map_[peer_addr] * 256 + session;
}

std::shared_ptr<RdmaUDEndPoint> Controller::getOrCreateEndpoint(int session) {
    // RWSpinlock::ReadGuard guard(session_lock_);
    auto node_id = session / 256;
    if (!peer_name_rev_map_.count(node_id)) return nullptr;
    return endpoint_store_.getOrCreateEndpoint(peer_name_rev_map_[node_id]);
}

std::shared_ptr<RdmaMulticastContext> Controller::getMulticastContext(
    int session) {
    // RWSpinlock::ReadGuard guard(session_lock_);
    auto node_id = session / 256;
    if (!peer_name_rev_map_.count(node_id)) return nullptr;
    auto peer_name = peer_name_rev_map_[node_id];
    if (multicast_context_map_.count(peer_name))
        return multicast_context_map_[peer_name];
    else
        return nullptr;
}

int Controller::redirectMulticast(int sid, int &index) {
    // RWSpinlock::ReadGuard guard(session_lock_);
    int node_id = sid / 256;
    int session = sid % 256;
    for (auto &entry : multicast_context_map_) {
        index = 0;
        for (auto &replica : entry.second->replicas()) {
            if (replica == peer_name_rev_map_[node_id]) {
                node_id = entry.second->groupId();
                return node_id * 256 + session;
            }
            index++;
        }
    }
    return sid;
}

}  // namespace rapid
