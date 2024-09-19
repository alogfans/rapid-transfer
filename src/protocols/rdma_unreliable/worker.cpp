// Copyright 2024 Feng Ren

#include "worker.h"

namespace rapid
{
    RdmaUnreliableWorker::RdmaUnreliableWorker(RdmaUDEndPointStore &endpoint_store)
        : endpoint_store_(endpoint_store),
          next_task_id_(0),
          workers_running_(false) {}

    RdmaUnreliableWorker::~RdmaUnreliableWorker() {}

    int RdmaUnreliableWorker::start()
    {
        workers_running_ = true;
        worker_thread_list_.emplace_back(std::thread(std::bind(&RdmaUnreliableWorker::sendWorker, this)));
        worker_thread_list_.emplace_back(std::thread(std::bind(&RdmaUnreliableWorker::receiveWorker, this)));
        return 0;
    }

    int RdmaUnreliableWorker::join()
    {
        if (!workers_running_.exchange(false))
            return 0;
        for (auto &entry : worker_thread_list_)
            entry.join();
        return 0;
    }

    int RdmaUnreliableWorker::submitSendRequest(const std::vector<std::string> &peer_name_list,
                                                const std::vector<Buffer> &buffer_list)
    {
        int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        auto task = std::make_shared<Task>(SEND, task_id);
        if (!task)
            return -1;
        task->peer_name_list = peer_name_list;
        task->buffer_list = buffer_list;

        worker_lock_.lock();
        task_map_[task_id] = task;
        send_task_queue_.push(task);
        worker_lock_.unlock();

        return task->id;
    }

    int RdmaUnreliableWorker::submitReceiveRequest(const std::string &peer_name,
                                                   const std::vector<Buffer> &buffer_list)
    {
        int task_id = next_task_id_.fetch_add(1, std::memory_order_relaxed);
        auto task = std::make_shared<Task>(RECEIVE, task_id);
        if (!task)
            return -1;
        task->peer_name_list.push_back(peer_name);
        task->buffer_list = buffer_list;

        worker_lock_.lock();
        task_map_[task_id] = task;
        receive_task_queue_.push(task);
        worker_lock_.unlock();

        return task->id;
    }

    Status RdmaUnreliableWorker::getStatus(TaskID task_id, size_t *transferred_bytes)
    {
        auto task = getTaskById(task_id);
        if (!task)
            return Status::UNKNOWN;
        if (transferred_bytes)
            *transferred_bytes = task->transferred_bytes;
        return task->status;
    }

    int RdmaUnreliableWorker::freeTask(TaskID task_id)
    {
        worker_lock_.lock();
        task_map_.erase(task_id);
        worker_lock_.unlock();
        return 0;
    }

    std::shared_ptr<RdmaUnreliableWorker::Task> RdmaUnreliableWorker::getTaskById(TaskID task_id)
    {
        RWSpinlock::ReadGuard guard(worker_lock_);
        if (!task_map_.count(task_id))
            return nullptr;
        return task_map_[task_id];
    }

    void RdmaUnreliableWorker::sendWorker()
    {
        std::shared_ptr<Task> current_task = nullptr;
        while (workers_running_)
        {
            if (!current_task)
            {
                worker_lock_.lockShared();
                if (!send_task_queue_.empty())
                {
                    current_task = send_task_queue_.front();
                    send_task_queue_.pop();
                }
                worker_lock_.unlockShared();
            }
            if (!current_task)
                continue;
            // ...
            poll(SEND);
            // ...
        }
    }

    void RdmaUnreliableWorker::receiveWorker()
    {
        std::shared_ptr<Task> current_task = nullptr;
        while (workers_running_)
        {
            if (!current_task)
            {
                worker_lock_.lockShared();
                if (!receive_task_queue_.empty())
                {
                    current_task = receive_task_queue_.front();
                    receive_task_queue_.pop();
                }
                worker_lock_.unlockShared();
            }
            if (!current_task)
                continue;
            poll(RECEIVE);
        }
    }

    int RdmaUnreliableWorker::poll(int cq_index)
    {
        const static size_t kPollCount = 64;
        auto &context = endpoint_store_.context();
        ibv_wc wc[kPollCount];
        int nr_poll = context.poll(kPollCount, wc, cq_index);
        if (nr_poll < 0)
        {
            LOG(ERROR) << "Worker: Failed to poll completion queues";
            return -1;
        }

        for (int i = 0; i < nr_poll; ++i)
        {
            auto request = (Request *)wc[i].wr_id;
            __sync_fetch_and_sub(request->qp_depth, 1);
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                LOG(ERROR) << "Worker: Process failed for slice (addr: " << request->addr
                           << ", length: " << request->length
                           << ", lkey: " << request->lkey
                           << ", local_nic: " << context.deviceName()
                           << "): " << ibv_wc_status_str(wc[i].status);
                endpoint_store_.deleteEndpoint(request->peer_name);
                request->status = FAILED;
            }
            else
                request->status = SUCCESS;
        }

        return 0;
    }

} // namespace rapid
