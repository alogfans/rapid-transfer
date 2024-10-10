// Copyright 2024 Feng Ren

#ifndef RDMA_UNRELIABLE_PROTOCOL_H
#define RDMA_UNRELIABLE_PROTOCOL_H

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_ud_endpoint.h"
#include "protocols/common/rdma_ud_endpoint_store.h"
#include "queue_entry.h"

#include <atomic>
#include <map>
#include <mutex>
#include <queue>
#include <sys/time.h>

namespace rapid
{
    class EventLoop;

    struct SessionIdManager
    {
    public:
        SessionIdManager() : next_session_id_(0) {}

        int allocateLocalSessionId(const std::string &peer_name)
        {
            RWSpinlock::WriteGuard guard(session_lock_);
            for (auto &entry : local_session_id_list_)
                if (entry.second == peer_name)
                    return entry.first;
            int session_id = next_session_id_++;
            local_session_id_list_[session_id] = peer_name;
            return session_id;
        }

        void setRemoteSessionId(const std::string &peer_name, int session_id)
        {
            RWSpinlock::WriteGuard guard(session_lock_);
            if (remote_session_id_list_.count(peer_name) && remote_session_id_list_[peer_name] != session_id)
                LOG(ERROR) << "Session id has been assigned to different peers";
            remote_session_id_list_[peer_name] = session_id;
        }

        // 接收方调用，以分辨不同来源的消息，并回传到不同位置
        std::string getPeerName(int session_id)
        {
            RWSpinlock::ReadGuard guard(session_lock_);
            if (local_session_id_list_.count(session_id))
                return local_session_id_list_[session_id];
            return "";
        }

        // 发送方调用，根据传递的目标地址决定要填充什么 session_id
        int getSessionId(const std::string &peer_name)
        {
            RWSpinlock::ReadGuard guard(session_lock_);
            if (remote_session_id_list_.count(peer_name))
                return remote_session_id_list_[peer_name];
            return -1;
        }

    private:
        RWSpinlock session_lock_;
        int next_session_id_;
        std::map<int, std::string> local_session_id_list_;
        std::map<std::string, int> remote_session_id_list_;
    };

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

        bool hasLostFragment(const std::string &peer_name, std::pair<uint64_t, uint64_t> region);

    private:
        bool valid_;
        RdmaContext context_;
        RdmaUDEndPointStore endpoint_store_;
        SessionIdManager session_id_manager_;
        std::unordered_map<std::string, QueueEntry> send_queue_, receive_queue_;
        std::unordered_map<TaskID, TaskInfo> task_info_;
        std::atomic<TaskID> next_task_id_;
        // TODO Undelivered packets [FAILED]
        EventLoop *event_loop_;
    };
}

#endif // RDMA_UNRELIABLE_PROTOCOL_H
