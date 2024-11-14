// Copyright 2024 Feng Ren

#ifndef RDMA_RELIABLE_PROTOCOL_H
#define RDMA_RELIABLE_PROTOCOL_H

#include <atomic>
#include <mutex>

#include "concurrency.h"
#include "protocol.h"
#include "protocols/common/rdma_context.h"
#include "protocols/common/rdma_rc_endpoint.h"
#include "protocols/common/rdma_rc_endpoint_store.h"

namespace rapid {
struct RdmaReliableProtocol : public Protocol {
    RdmaReliableProtocol();

    virtual ~RdmaReliableProtocol();
    RdmaReliableProtocol(const RdmaReliableProtocol &) = delete;
    RdmaReliableProtocol &operator=(const RdmaReliableProtocol &) = delete;

    virtual int construct(const std::string &device_name, uint8_t rdma_port,
                          int gid_index);

    virtual int deconstruct();

    virtual int prepareConnection(const std::string &peer_name,
                                  Attributes &local);

    virtual int setupConnection(const std::string &peer_name,
                                const Attributes &peer);

    virtual TaskID send(const std::string &peer_name,
                        const std::vector<Buffer> &buffer_list);

    virtual TaskID receive(const std::string &peer_name,
                           const std::vector<Buffer> &buffer_list);

    virtual Status getStatus(TaskID task_id, size_t *transferred_bytes);

    virtual int freeTask(TaskID task_id);

    virtual int registerLocalMemory(void *addr, size_t length);

    virtual int unregisterLocalMemory(void *addr);

   public:
    struct Task {
        Task(RequestType type, int id) : type(type), id(id) {}
        ~Task() {
            for (auto &request : request_list) delete request;
            request_list.clear();
        }

        const RequestType type;
        const TaskID id;

        std::vector<Request *> request_list;
    };

    std::shared_ptr<Task> allocateTask(RequestType type);

    std::shared_ptr<Task> getTaskById(TaskID task_id);

    void runBackgroundWorker();

    int poll(int cq_index);

   public:
    bool valid_;

    std::atomic<int> next_task_id_;
    RWSpinlock task_map_lock_;
    std::unordered_map<TaskID, std::shared_ptr<Task>> task_map_;

    RdmaContext context_;
    RdmaRCEndPointStore endpoint_store_;

    std::atomic<bool> background_running_;
    std::thread background_worker_;
};
}  // namespace rapid

#endif  // RDMA_RELIABLE_PROTOCOL_H
