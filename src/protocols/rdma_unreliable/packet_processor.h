// Copyright 2024 Feng Ren

#ifndef PACKET_PROCESSOR_H_
#define PACKET_PROCESSOR_H_

#include "event_loop.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"

namespace rapid {
class EventLoop;
class PacketProcessor {
    friend class EventLoop;

   public:
    PacketProcessor(RdmaUnreliableProtocol &protocol);

    ~PacketProcessor();

    int construct();

    int deconstruct();

    RdmaContext &context() { return protocol_.context_; }

    SessionIdManager &sessionIdManager() {
        return protocol_.session_id_manager_;
    }

   public:
    bool connected(const std::string &peer_name);

    int issuePackets(const std::string &peer_name, RequestType type,
                     const std::vector<Buffer> &buffer_list, uint32_t &next_sn);

    uint32_t nextPacketSN(const std::string &peer_name, RequestType type);

    int prepareConnection(const std::string &peer_name, Attributes &local);

    int setupConnection(const std::string &peer_name, const Attributes &peer);

   private:
    void worker();

   private:
    enum SessionStatus { SESSION_OK, SESSION_RESET };

    struct Session {
        SessionStatus status = SESSION_OK;
        uint32_t cid = 0;
        uint32_t next_send_sn = 0;
        uint32_t next_recv_sn = 0;
        uint32_t next_ack_send_sn = 0;
        uint32_t next_ack_recv_sn = 0;
        QueueEntry send_queue, recv_queue;
    };

   private:
    RdmaUnreliableProtocol &protocol_;
    RdmaUDEndPointStore endpoint_store_;
    EventLoop event_loop_;

    std::atomic<bool> running_;
    std::vector<std::thread> worker_list_;

    RWSpinlock sessions_lock_;
    std::unordered_map<std::string, Session> sessions_;
};
}  // namespace rapid

#endif  // PACKET_PROCESSOR_H_
