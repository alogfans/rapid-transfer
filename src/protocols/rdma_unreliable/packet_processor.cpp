// Copyright 2024 Feng Ren

#include "packet_processor.h"

#include "impl.h"

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

PacketProcessor::PacketProcessor(RdmaUnreliableProtocol &protocol)
    : protocol_(protocol),
      endpoint_store_(protocol.context_),
      event_loop_(this),
      running_(false) {}

PacketProcessor::~PacketProcessor() {}

int PacketProcessor::construct() {
    if (running_) return 0;
    int ret = endpoint_store_.construct(protocol_.context_.cq(SEND_CQ),
                                        protocol_.context_.cq(RECV_CQ));
    if (ret) return ret;
    running_ = true;
    event_loop_.construct();
    worker_list_.emplace_back(
        std::thread(std::bind(&PacketProcessor::worker, this)));
    return 0;
}

int PacketProcessor::deconstruct() {
    if (!running_.exchange(false)) return 0;
    for (auto &entry : worker_list_) entry.join();
    event_loop_.deconstruct();
    endpoint_store_.deconstruct();
    return 0;
}

void PacketProcessor::worker() {
    while (running_) event_loop_.step();
}

bool PacketProcessor::connected(const std::string &peer_name) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    if (!sessions_.count(peer_name)) return false;
    auto &session = sessions_[peer_name];
    return session.status == SESSION_OK;
}

int PacketProcessor::issuePackets(const std::string &peer_name,
                                  RequestType type,
                                  const std::vector<Buffer> &buffer_list,
                                  uint32_t &next_sn) {
    RWSpinlock::WriteGuard guard(sessions_lock_);
    auto &session = sessions_[peer_name];
    if (type == SEND) {
        auto fragment = session.send_queue.push(buffer_list);
        next_sn = fragment.second;
    } else {
        auto fragment = session.recv_queue.push(buffer_list);
        next_sn = fragment.second;
    }
    return 0;
}

uint32_t PacketProcessor::nextPacketSN(const std::string &peer_name,
                                       RequestType type) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    if (!sessions_.count(peer_name)) return UINT32_MAX;
    auto &session = sessions_[peer_name];
    if (session.status != SESSION_OK) return UINT32_MAX;
    return type == SEND ? session.next_ack_send_sn : session.next_ack_recv_sn;
}

int PacketProcessor::prepareConnection(const std::string &peer_name,
                                       Attributes &local) {
    auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_name);
    if (!endpoint) return -1;
    auto &context = protocol_.context_;
    local["lid"] = std::to_string(context.lid());
    local["gid"] = context.gid();
    local["qp"] = ToString(endpoint->qpNum());
    local["session"] = std::to_string(
        protocol_.session_id_manager_.allocateSidByReceiver(peer_name));
    return 0;
}

int PacketProcessor::setupConnection(const std::string &peer_name,
                                     const Attributes &peer) {
    auto endpoint = endpoint_store_.getOrCreateEndpoint(peer_name);
    if (!endpoint) return -1;
    if (!peer.count("lid") || !peer.count("gid") || !peer.count("qp") ||
        !peer.count("session"))
        return -1;
    auto lid = (uint16_t)std::stoi(peer.at("lid"));
    auto gid = peer.at("gid");
    auto qp_num_list = FromString(peer.at("qp"));
    auto session_id = std::stoi(peer.at("session"));
    int ret = endpoint->setupConnection(gid, lid, qp_num_list);
    if (ret) return ret;
    protocol_.session_id_manager_.setSidBySender(peer_name, session_id);
    return 0;
}
}  // namespace rapid
