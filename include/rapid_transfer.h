// rapid_transfer.h
//
// C++ Interface of RapidTransfer
//
// Copyright (C) 2024 Feng Ren

#ifndef RAPID_TRANSFER_H
#define RAPID_TRANSFER_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace rapid
{
    using Attributes = std::unordered_map<std::string, std::string>;

    struct Buffer
    {
        void *addr;
        size_t length;
    };

    enum Status
    {
        UNKNOWN,
        PENDING,
        SUCCESS,
        FAILED
    };

    using TaskID = int;

    class SessionManager;
    class Protocol;

    using OnConnectionStateChange = std::function<void(const std::string & /* peer name */, bool /* join or leave */)>;

    class RapidTransfer
    {
    public:
        // Create an instance
        // Parameters:
        // - protocol: Transfer protocol name, can be either `rdma-reliable` or `rdma-unreliable`
        // - device_name: RDMA NIC name for transfer, e.g. `mlx5_0`
        // - rdma_port: RDMA NIC port for communication
        // - gid_index: RDMA Local GID index for communication
        //
        // Return Value: RapidTransfer pointer if success, nullptr if failed
        static std::shared_ptr<RapidTransfer> Create(const std::string &protocol,
                                                     const std::string &device_name,
                                                     uint8_t rdma_port = 1,
                                                     int gid_index = 0);

        RapidTransfer(const std::string &device_name);

        virtual ~RapidTransfer();

        // Start an asynchronous file transfer task
        // - peer_name_list: Hostnames (or IP ports) of target servers to transfer file
        // - buffer_list: List of memory buffers, representing the content of transferred data
        // Return Value: Task ID if success, negative values if failed
        TaskID send(const std::vector<std::string> &peer_name_list, const std::vector<Buffer> &buffer_list);

        TaskID receive(const std::string &peer_name, const std::vector<Buffer> &buffer_list);

        // Get send/receive progress
        Status getStatus(TaskID task_id, size_t *transferred_bytes);

        // Free internal resource for specified task, i.e., call getStatus() is then undefined
        int freeTask(TaskID task_id);

        // Register local memory region, address regions of Buffer objects must have been registered
        int registerLocalMemory(void *addr, size_t length);

        // Unregister local memory region
        int unregisterLocalMemory(void *addr);

        // Start listen thread, required if this instance will receive data from remote
        int startListener(const std::string &listen_address, const OnConnectionStateChange &callback);

        // Stop listen thread
        int shutdownListener();

    private:
        int makeConnectionIfNeeded(const std::string &peer_name);

    private:
        SessionManager *session_manager_;
        Protocol *protocol_;
    };
} // namespace rapid

#endif // RAPID_TRANSFER_H
