// Copyright 2024 Feng Ren

#ifndef EVENT_LOOP_H_
#define EVENT_LOOP_H_

#include "concurrency.h"
#include "impl.h"
#include "packet.h"
#include "packet_pool.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"

#include <cassert>

namespace rapid
{
    class EventLoop
    {
    public:
        EventLoop(RdmaUnreliableProtocol *protocol);

        ~EventLoop();

        int start();

        int join();

        uint64_t completedPackets() { return completed_packets_; }

    private:
        void worker();

    private:
        struct Packet
        {
            PacketHeader hdr;
            void *data;
            std::string peer_name;
            int resend_count;
        };

        int submitRequests();

        int pollCompletedPackets(int cq_index, uint64_t current_ts);

        int sendDataPackets(uint64_t current_ts);

        int sendAckPackets(uint64_t current_ts);

        void updateRTO(uint64_t rtt);

        void runReceiveCallbacks();

        int ProcessReceivedPacket(uint64_t current_ts, ibv_wc &wc);

        int postReceiveWorkRequest();

        static inline uint64_t GetCurrentTimeInUsec()
        {
            struct timeval tv_now;
            gettimeofday(&tv_now, nullptr);
            return (tv_now.tv_sec * 1000000 + tv_now.tv_usec);
        }

    private:
        RdmaUnreliableProtocol *protocol_;
        RdmaUDEndPointStore &endpoint_store_;

        std::vector<std::thread> worker_list_;
        std::atomic<bool> running_;

        const static size_t kPacketStorageSize = 4096 + 40;
        const static size_t kMaxPayloadSize = 4096 - sizeof(PacketHeader);

        const static size_t kWndSend = 128;
        const static size_t kWndRecv = 128;
        const static size_t kDefaultRTO = 1000 * 10;
        const static size_t kMinRTO = 50;
        const static size_t kMaxRTO = 1000 * 10;
        const static uint64_t kMaxPacketBpsRate = 25 * 1000 * 1000;

        const static int kMaxResendCount = 16;
        uint32_t send_wnd_ = kWndSend, recv_wnd_ = kWndRecv;

        uint64_t recv_srtt_ = 0, recv_rttval_ = 0, recv_rto_ = kDefaultRTO;
        int64_t send_credit_ = 1;
        uint64_t send_credit_ts_ = GetCurrentTimeInUsec();

        std::vector<Packet> send_buffer_, recv_buffer_;
        std::queue<Buffer> recv_queue_;

        std::atomic<uint64_t> completed_packets_ = 0;
        std::atomic<uint64_t> received_packets_ = 0;

        PacketPool packet_pool_;

        struct Session
        {
            uint32_t cid = 0;
            uint32_t next_send_sn = 0;
            uint32_t next_recv_sn = 0;

            // TODO remove
            uint64_t acked_ts = 0;
            uint32_t acked_next_recv_sn = 0;
            bool resend_ack = false;
            std::vector<uint32_t> lost_packet_sn;
        };

        std::unordered_map<std::string, Session> sessions_;
    };
}

#endif // EVENT_LOOP_H_
