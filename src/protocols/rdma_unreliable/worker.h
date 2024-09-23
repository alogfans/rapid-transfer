// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_WORKER_H
#define RDMA_UNRELIABLE_WORKER_H

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"

#include <atomic>
#include <cassert>
#include <infiniband/verbs.h>
#include <map>
#include <mutex>
#include <queue>
#include <sys/time.h>
#include <unordered_set>

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

    class RdmaUnreliableWorker
    {
    public:
        RdmaUnreliableWorker(RdmaUDEndPointStore &endpoint_store);

        ~RdmaUnreliableWorker();

        int start();

        int join();

        int submitSendRequest(const std::vector<std::string> &peer_name_list,
                              const std::vector<Buffer> &buffer_list);

        int submitReceiveRequest(const std::string &peer_name,
                                 const std::vector<Buffer> &buffer_list);

        Status getStatus(TaskID task_id, size_t *transferred_bytes);

        int freeTask(TaskID task_id);

    private:
        void worker();

    private:
        struct Task;

        struct Packet
        {
            PacketHeader hdr;
            void *data;
        };

        struct Task
        {
            Task(RequestType type, int id)
                : type(type),
                  id(id),
                  status(PENDING),
                  transferred_bytes(0),
                  total_bytes(0) {}
            ~Task() { /* TBD */ }

            const RequestType type;
            const TaskID id;

            std::atomic<Status> status;
            std::atomic<uint64_t> transferred_bytes;
            std::atomic<uint64_t> total_bytes;
        };

        std::shared_ptr<Task> getTaskById(TaskID task_id);

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
        RdmaUDEndPointStore &endpoint_store_;
        RWSpinlock worker_lock_;

        std::atomic<int> next_task_id_;
        std::unordered_map<TaskID, std::shared_ptr<Task>> task_map_;

        std::vector<std::thread> worker_thread_list_;
        std::atomic<bool> workers_running_;

        const static size_t kPacketStorageSize = 4096 + 40;
        const static size_t kMaxPayloadSize = kPacketStorageSize - sizeof(PacketHeader);

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

#endif // RDMA_UNRELIABLE_WORKER_H
