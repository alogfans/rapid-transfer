// Copyright 2024 Feng Ren

#ifndef RDMA_RELIABLE_PROTOCOL_H
#define RDMA_RELIABLE_PROTOCOL_H

#include "../../concurrency.h"
#include "../../protocol.h"
#include "../common/rdma_context.h"
#include "../common/rdma_endpoint.h"

#include <atomic>
#include <mutex>

namespace rapid
{
    struct Task
    {
        Task()
            : mark_failed(false),
              transferred_bytes(0),
              total_packets(0),
              success_packets(0),
              failed_packets(0) {}

        bool mark_failed;
        std::atomic<size_t> transferred_bytes;

        Attributes attributes;
        std::vector<std::string> target_list;
        std::vector<Buffer> buffer_list;

        int total_packets;
        std::atomic<int> success_packets, failed_packets;
    };

    struct RdmaReliableProtocol : public Protocol
    {
        RdmaReliableProtocol();
        virtual ~RdmaReliableProtocol() {}
        RdmaReliableProtocol(const RdmaReliableProtocol &) = delete;
        RdmaReliableProtocol &operator=(const RdmaReliableProtocol &) = delete;

        virtual int construct(const std::string &local_hostname,
                              const std::string &device_name,
                              uint8_t rdma_port,
                              int gid_index);

        virtual int deconstruct();

        virtual TaskID allocateTask();

        virtual int freeTask(TaskID task_id);

        virtual int prepareSend(TaskID task_id,
                                std::vector<Attributes> &request_list,
                                const std::vector<std::string> &target_list,
                                const Attributes &attributes,
                                const std::vector<Buffer> &buffers);

        virtual int issueSend(TaskID task_id, const std::vector<Attributes> &response_list);

        virtual int prepareReceive(TaskID task_id,
                                   const Attributes &request,
                                   Attributes &response,
                                   const RapidTransfer::OnReceiveCallback &on_receive);

        virtual int setFailedStatus(TaskID task_id);

        virtual Status getStatus(TaskID task_id, size_t *transferred_bytes);

        virtual int registerBuffer(void *addr, size_t length);

        virtual int unregisterBuffer(void *addr);

    public:
        std::shared_ptr<Task> getTaskById(TaskID task_id);

        void runPollWorker();

    public:
        std::atomic<int> next_task_id_;
        RWSpinlock task_map_lock_;
        std::unordered_map<TaskID, std::shared_ptr<Task>> task_map_;

        RdmaContext context_;

        std::atomic<bool> poll_worker_running_;
        std::thread poll_worker_;
    };
}

#endif // RDMA_RELIABLE_PROTOCOL_H
