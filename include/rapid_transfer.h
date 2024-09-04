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

    using Attributes = std::unordered_map<std::string, std::string>;
    using TaskID = int;

    class SessionManager;
    class Protocol;

    class RapidTransfer
    {
    public:
        // Create an instance
        // Parameters:
        // - protocol: Transfer protocol name, can be either `rdma-reliable` or `rdma-unreliable`
        // - device_name: RDMA NIC name for transfer, e.g. `mlx5_0`
        // - local_hostname: Local server identification, `gethostname(2)` by default
        // - rdma_port: RDMA NIC port for communication
        // - gid_index: RDMA Local GID index for communication
        //
        // Return Value: RapidTransfer pointer if success, nullptr if failed
        static std::shared_ptr<RapidTransfer> Create(const std::string &protocol,
                                                     const std::string &device_name,
                                                     const std::string &local_hostname = "",
                                                     uint8_t rdma_port = 1,
                                                     int gid_index = 3);

        RapidTransfer(const std::string &device_name);

        virtual ~RapidTransfer();

        // Start an asynchronous file transfer task
        // - target_list: Hostnames (or IP ports) of target servers to transfer file
        // - attributes: User-defined attributes (key-value style)
        // - buffer_list: List of memory buffers, representing the content of transferred data
        //
        // Return Value: Task ID if success, negative values if failed
        TaskID send(const std::vector<std::string> &target_list,
                    const Attributes &attributes,
                    const std::vector<Buffer> &buffer_list);

        // Optional callback: be called if a receive task is completed (success or failed)
        using OnReceiveEndCallback = std::function<int(TaskID, const std::vector<Buffer> &)>;

        // Callback: be called if new send task from remote server arrived
        // User should allocate buffer_list for storing data
        // If needed, set on_success and/or on_failure callbacks after transfer completed
        using OnReceiveBeginCallback = std::function<int(TaskID,
                                                         const std::string & /* peer hostname */,
                                                         const Attributes & /* attributes from peer */,
                                                         std::vector<Buffer> & /* to fill: buffer_list */,
                                                         OnReceiveEndCallback & /* to fill: on_success */,
                                                         OnReceiveEndCallback & /* to fill: on_failure */)>;

        // Get send/receive progress
        Status getStatus(TaskID task_id, size_t *transferred_bytes);

        // Free internal resource for specified task, i.e., call getStatus() is then undefined
        int freeTask(TaskID task_id);

        // Register local memory region, address regions of Buffer objects must have been registered
        int registerLocalMemory(void *addr, size_t length);

        // Unregister local memory region
        int unregisterLocalMemory(void *addr);

        // Start listen thread, required if this instance will receive data from remote
        int startListener(const std::string &listen_address, const OnReceiveBeginCallback &on_receive_begin);

        // Stop listen thread
        int shutdownListener();

    private:
        SessionManager *session_manager_;
        Protocol *protocol_;
    };
} // namespace rapid

#endif // RAPID_TRANSFER_H
