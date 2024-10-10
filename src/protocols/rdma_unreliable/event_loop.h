// Copyright 2024 Feng Ren

#ifndef EVENT_LOOP_H_
#define EVENT_LOOP_H_

#include "concurrency.h"
#include "impl.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"

#include <cassert>

namespace rapid
{
    const static uint32_t CMD_DATA = 81;
    const static uint32_t CMD_ACK = 82;
    struct PacketHeader
    {
        __le32 cid;
        __u8 cmd;
        __u8 resv1;
        __le16 wnd;
        __le64 ts;
        __le32 sn;
        __le32 una;
        __le32 len;
        __le32 resv2;
    };

    static inline void EncodePacket(PacketHeader *dst, const PacketHeader &src)
    {
        dst->cid = htole32(src.cid);
        dst->cmd = src.cmd;
        dst->resv1 = src.resv1;
        dst->wnd = htole16(src.wnd);
        dst->ts = htole64(src.ts);
        dst->sn = htole32(src.sn);
        dst->una = htole32(src.una);
        dst->len = htole32(src.len);
        dst->resv2 = htole32(src.resv2);
    }

    static inline void DecodePacket(PacketHeader &dst, const PacketHeader *src)
    {
        dst.cid = le32toh(src->cid);
        dst.cmd = src->cmd;
        dst.resv1 = src->resv1;
        dst.wnd = le16toh(src->wnd);
        dst.ts = le64toh(src->ts);
        dst.sn = le32toh(src->sn);
        dst.una = le32toh(src->una);
        dst.len = le32toh(src->len);
        dst.resv2 = le32toh(src->resv2);
    }

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
        };

        int submitRequests();

        int submitSendRequest(const std::string &peer_name, const Buffer &buffer);

        int submitReceiveRequest(const std::string &peer_name, const Buffer &buffer);

        int pollCompletedPackets(int cq_index, uint64_t current_ts);

        int sendDataPackets(uint64_t current_ts);

        int sendAckPackets(uint64_t current_ts);

        void updateSendUna(uint64_t current_ts);

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

        std::atomic<int> next_task_id_;
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

        const static uint32_t CMD_SEND = 81;
        const static uint32_t CMD_ACK = 82;

        uint32_t next_send_sn_ = 0, next_recv_sn_ = 0;
        uint32_t send_wnd_ = kWndSend, recv_wnd_ = kWndRecv;
        uint32_t send_una_ = 0;
        uint64_t recv_srtt_ = 0, recv_rttval_ = 0, recv_rto_ = kDefaultRTO;
        int64_t send_credit_ = 1;
        uint64_t send_credit_ts_ = GetCurrentTimeInUsec();

        std::vector<Packet> send_buffer_, recv_buffer_;
        std::queue<Buffer> recv_queue_;
        std::atomic<uint64_t> completed_packets_ = 0, total_packets_ = 0;
        std::atomic<uint64_t> received_packets_ = 0;

    private:
        void *packet_buffer_;
        void *next_free_packet_buffer_;
        size_t packet_buffer_count_ = 512;

        void setupPacketPool()
        {
            packet_buffer_ = malloc(kPacketStorageSize * packet_buffer_count_);
            assert(packet_buffer_);
            auto &context = endpoint_store_.context();
            int ret = context.registerMemoryRegion(packet_buffer_,
                                                   kPacketStorageSize * packet_buffer_count_,
                                                   IBV_ACCESS_LOCAL_WRITE);
            assert(!ret);
            next_free_packet_buffer_ = nullptr;
            for (size_t index = 0; index < packet_buffer_count_; ++index)
            {
                void *ptr = (char *)packet_buffer_ + kPacketStorageSize * index;
                *(uintptr_t *)ptr = (uintptr_t)next_free_packet_buffer_;
                next_free_packet_buffer_ = ptr;
            }
        }

        void destroyPacketPool()
        {
            auto &context = endpoint_store_.context();
            context.unregisterMemoryRegion(packet_buffer_);
            free(packet_buffer_);
        }

        PacketHeader *allocatePacket()
        {
            void *ptr = next_free_packet_buffer_;
            assert(ptr);
            uintptr_t next = *(uintptr_t *)ptr;
            next_free_packet_buffer_ = (void *)next;
            return (PacketHeader *)ptr;
        }

        void freePacket(PacketHeader *header)
        {
            *(uintptr_t *)header = (uintptr_t)next_free_packet_buffer_;
            next_free_packet_buffer_ = header;
        };
    };
}

#endif // EVENT_LOOP_H_
