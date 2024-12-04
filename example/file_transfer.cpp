// file_transfer.cpp
//
// Samples code for RapidTransfer, providing option to transfer data from disk to disk
//
// Copyright (C) 2024 Feng Ren

#include "rapid_transfer.h"

#include <atomic>
#include <cassert>
#include <csignal>
#include <fcntl.h>
#include <future>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iomanip>
#include <numa.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>

DEFINE_string(role, "sender", "Execution role: sender, receiver");
DEFINE_string(protocol, "rdma_reliable", "Transport protocol: rdma_reliable, rdma_unreliable");
DEFINE_string(path, "", "Path of file to transfer");
DEFINE_string(device, "mlx5_0", "RDMA device name to use");
DEFINE_string(target, "optane21:12348", "Target hostname (and port, if needed)");
DEFINE_string(listen, ":12348", "TCP listen address");
DEFINE_uint32(rdma_port, 1, "RDMA port");
DEFINE_uint32(gid_index, 0, "GID Index");

using namespace rapid;

static void *allocateMemoryPool(size_t size, int socket_id)
{
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size)
{
    numa_free(addr, size);
}

static inline ssize_t writeFully(int fd, const void *buf, size_t len)
{
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes)
    {
        ssize_t rc = write(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0)
        {
            PLOG(ERROR) << "Socket write failed";
            return rc;
        }
        else if (rc == 0)
        {
            LOG(WARNING) << "Socket write incompleted: expected " << len
                         << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline ssize_t readFully(int fd, void *buf, size_t len)
{
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes)
    {
        ssize_t rc = read(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0)
        {
            PLOG(ERROR) << "Socket read failed";
            return rc;
        }
        else if (rc == 0)
        {
            LOG(WARNING) << "Socket read incompleted: expected " << len
                         << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

int receiver()
{
    auto engine = rapid::RapidTransfer::Create(FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);

    const size_t dram_buffer_size = 1ull << 30;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    if (!addr)
    {
        LOG(ERROR) << "Failed to allocate memory pool";
        return -1;
    }

    int ret = engine->registerLocalMemory(addr, dram_buffer_size);
    if (ret)
    {
        LOG(ERROR) << "Failed to register memory";
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    std::mutex mutex;
    std::string peer_name;

    TaskID task_id;
    uint64_t length = UINT64_MAX, packet_length = 0;
    auto on_new_connection = [&](const std::string &peer_name_, bool is_join)
    {
        if (!is_join || !peer_name.empty())
        {
            LOG(INFO) << "The receiver can accept only one connection";
            return;
        }
        LOG(INFO) << "Arriving connection: " << peer_name_;
        mutex.lock();
        peer_name = peer_name_;
        task_id = engine->receive(peer_name, {{addr, sizeof(uint64_t)}});
        mutex.unlock();
    };

    ret = engine->startListener(FLAGS_listen, on_new_connection);
    if (ret)
    {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    auto cleanup = [&]()
    {
        engine->shutdownListener();
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
    };

    int fd = -1;
    if (!FLAGS_path.empty())
    {
        fd = open(FLAGS_path.c_str(), O_RDWR | O_TRUNC | O_CREAT, 0644);
        if (fd < 0)
        {
            LOG(ERROR) << "Failed to open file " << FLAGS_path;
            cleanup();
            return -1;
        }
    }

    while (true)
    {
        mutex.lock();
        if (peer_name.empty())
        {
            mutex.unlock();
            continue;
        }
        mutex.unlock();
        if (task_id < 0)
        {
            LOG(INFO) << "Illegal task ID";
            cleanup();
            return -1;
        }

        auto status = engine->getStatus(task_id, nullptr);
        if (status == SUCCESS)
        {
            if (length == UINT64_MAX)
            {
                length = *(uint64_t *)addr;
                assert(length > 0 && length < (32ull << 30));
            }
            else
            {
                if ((ssize_t)packet_length != writeFully(fd, addr, packet_length))
                {
                    cleanup();
                    return -1;
                }
            }

            packet_length = std::min(length, dram_buffer_size);
            length -= packet_length;
            engine->freeTask(task_id);
            if (!packet_length)
                break;
            task_id = engine->receive(peer_name, {{addr, packet_length}});
        }
        else if (status == FAILED)
        {
            cleanup();
            return -1;
        }
    }

    cleanup();
    return 0;
}

int sender()
{
    auto engine = rapid::RapidTransfer::Create(FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);

    const size_t dram_buffer_size = 1ull << 30;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    if (!addr)
    {
        LOG(ERROR) << "Failed to allocate memory pool";
        return -1;
    }

    int ret = engine->registerLocalMemory(addr, dram_buffer_size);
    if (ret)
    {
        LOG(ERROR) << "Failed to register memory";
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    auto cleanup = [&]()
    {
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
    };

    std::string path = FLAGS_path;
    size_t file_size = dram_buffer_size;
    int fd = -1;
    if (!path.empty())
    {
        struct stat st;
        ret = stat(path.c_str(), &st);
        if (ret)
            PLOG(WARNING) << "Failed to stat file: " << path << ", fallback to send dummy data";
        else if (!(st.st_mode & S_IFREG))
            LOG(WARNING) << "Not a regular file: " << path << ", fallback to send dummy data";
        else
        {
            file_size = st.st_size;
            fd = open(path.c_str(), O_RDONLY);
            if (fd < 0)
            {
                LOG(ERROR) << "Failed to open file";
                cleanup();
                return -1;
            }
        }
    }

    auto wait_for_completion = [&](TaskID task_id)
    {
        while (true)
        {
            auto status = engine->getStatus(task_id, nullptr);
            if (status == rapid::FAILED)
            {
                LOG(ERROR) << "Failed to send data to remote";
                cleanup();
                return -1;
            }

            if (status == rapid::SUCCESS)
                return 0;
        }
    };

    *(uint64_t *)addr = file_size;
    TaskID task_id = engine->send(FLAGS_target, {{addr, sizeof(uint64_t)}});
    if (wait_for_completion(task_id))
    {
        cleanup();
        return -1;
    }

    for (size_t offset = 0; offset < file_size; offset += dram_buffer_size)
    {
        size_t chunk_size = std::min(dram_buffer_size, file_size - offset);
        if (fd >= 0)
        {
            if ((ssize_t)chunk_size != readFully(fd, addr, chunk_size))
            {
                LOG(ERROR) << "Failed to read file fully";
                cleanup();
                return -1;
            }
        }

        TaskID task_id = engine->send({FLAGS_target}, {{addr, chunk_size}});
        if (wait_for_completion(task_id))
        {
            cleanup();
            return -1;
        }
    }

    LOG(INFO) << "Sending completed";
    cleanup();
    return 0;
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_role == "sender")
        return sender();
    else if (FLAGS_role == "receiver")
        return receiver();

    LOG(ERROR) << "Wrong execution role: should be either 'sender' or 'receiver'";
    exit(EXIT_FAILURE);
}
