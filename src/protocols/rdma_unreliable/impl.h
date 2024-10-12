// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_PROTOCOL_H
#define RDMA_UNRELIABLE_PROTOCOL_H

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"
#include "queue_entry.h"
#include "session_id_manager.h"

#include <atomic>
#include <map>
#include <mutex>
#include <queue>
#include <sys/time.h>

namespace rapid
{
    class EventLoop;

    struct RdmaUnreliableProtocol : public Protocol
    {
        friend class EventLoop;

        RdmaUnreliableProtocol();

        virtual ~RdmaUnreliableProtocol();
        RdmaUnreliableProtocol(const RdmaUnreliableProtocol &) = delete;
        RdmaUnreliableProtocol &operator=(const RdmaUnreliableProtocol &) = delete;

        virtual int construct(const std::string &local_hostname,
                              const std::string &device_name,
                              uint8_t rdma_port,
                              int gid_index);

        virtual int deconstruct();

        virtual int prepareConnection(const std::string &peer_name, Attributes &local);

        virtual int setupConnection(const std::string &peer_name, const Attributes &peer);

        virtual TaskID send(const std::vector<std::string> &peer_name_list,
                            const std::vector<Buffer> &buffer_list);

        virtual TaskID receive(const std::string &peer_name,
                               const std::vector<Buffer> &buffer_list);

        virtual Status getStatus(TaskID task_id, size_t *transferred_bytes);

        virtual int freeTask(TaskID task_id);

        virtual int registerLocalMemory(void *addr, size_t length);

        virtual int unregisterLocalMemory(void *addr);

    private:
        struct TaskInfo
        {
            std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> fragment_id_map;
        };

        uint64_t nextAckFragmentId(const std::string &peer_name);

    private:
        bool valid_;
        RdmaContext context_;
        RdmaUDEndPointStore endpoint_store_;
        SessionIdManager session_id_manager_;

        RWSpinlock lock_;
        std::unordered_map<std::string, QueueEntry> send_queue_, receive_queue_;
        std::unordered_map<TaskID, TaskInfo> task_info_;

        std::atomic<TaskID> next_task_id_;
        EventLoop *event_loop_;
    };
}

#endif // RDMA_UNRELIABLE_PROTOCOL_H
