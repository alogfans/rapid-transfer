// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_WORKER_H
#define RDMA_UNRELIABLE_WORKER_H

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"

#include <atomic>
#include <mutex>
#include <queue>

namespace rapid
{
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

    static inline void EncodePacket(PacketHeader &packet)
    {
        packet.cid = htole32(packet.cid);
        packet.wnd = htole16(packet.wnd);
        packet.ts = htole64(packet.ts);
        packet.sn = htole32(packet.sn);
        packet.una = htole32(packet.una);
        packet.len = htole32(packet.len);
    }

    static inline void DecodePacket(PacketHeader &packet)
    {
        packet.cid = le32toh(packet.cid);
        packet.wnd = le16toh(packet.wnd);
        packet.ts = le64toh(packet.ts);
        packet.sn = le32toh(packet.sn);
        packet.una = le32toh(packet.una);
        packet.len = le32toh(packet.len);
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
        void sendWorker();

        void receiveWorker();

        int poll(int cq_index);

    private:
        struct Task
        {
            Task(RequestType type, int id) : type(type), id(id), status(PENDING), transferred_bytes(0) {}
            ~Task() { /* TBD */ }

            const RequestType type;
            const TaskID id;
            std::vector<std::string> peer_name_list;
            std::vector<Buffer> buffer_list;
            std::atomic<Status> status;
            std::atomic<uint64_t> transferred_bytes;

            std::queue<Request *> request_buffer;
        };

        std::shared_ptr<Task> getTaskById(TaskID task_id);

    private:
        RdmaUDEndPointStore &endpoint_store_;
        RWSpinlock worker_lock_;

        std::atomic<int> next_task_id_;
        std::unordered_map<TaskID, std::shared_ptr<Task>> task_map_;

        std::queue<std::shared_ptr<Task>> send_task_queue_, receive_task_queue_;

        std::vector<std::thread> worker_thread_list_;
        std::atomic<bool> workers_running_;
    };
}

#endif // RDMA_UNRELIABLE_WORKER_H
