// Copyright 2024 Feng Ren

#ifndef EVENT_LOOP_H_
#define EVENT_LOOP_H_

#include "impl.h"
#include "packet.h"
#include "packet_pool.h"

#include <sys/time.h>

namespace rapid
{
    class PacketProcessor;
    class EventLoop
    {
    public:
        EventLoop(PacketProcessor *processor);

        ~EventLoop();

        int construct();

        int deconstruct();

        int step();

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

        PacketProcessor *processor_;
        PacketPool packet_pool_;
        std::vector<Packet> send_buffer_, recv_buffer_;
    };
}

#endif // EVENT_LOOP_H_
