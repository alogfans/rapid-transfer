// rapid_transfer.cpp
//
// Samples code for RapidTransfer, providing option to transfer data from disk to disk
//
// Copyright (C) 2024 Feng Ren

#include "rapid_transfer.h"

#include <atomic>
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
DEFINE_string(path, "", "Path of file to transfer");
DEFINE_string(device, "mlx5_2", "RDMA device name to use");
DEFINE_string(target, "optane21:12348", "Target hostname (and port, if needed)");
DEFINE_string(listen, ":12348", "TCP listen address");

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

std::function<void()> cleanup_func;

void signalHandler(int signum)
{
    LOG(INFO) << "Interrupt signal (" << signum << ") received";
    if (cleanup_func)
        cleanup_func();
    exit(signum);
}

int receiver()
{
    auto engine = rapid::RapidTransfer::Create("rdma_reliable", FLAGS_device);
    LOG_ASSERT(engine);

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

    auto on_success_callback = [&](TaskID task_id, const std::vector<rapid::Buffer> &buffer_list) -> int
    {
        int fd = open(FLAGS_path.c_str(), O_WRONLY, 0644);
        if (fd < 0)
        {
            LOG(ERROR) << "Failed to open file";
            return -1;
        }

        for (auto &buffer : buffer_list)
        {
            if ((ssize_t)buffer.length != writeFully(fd, buffer.addr, buffer.length))
            {
                LOG(ERROR) << "Failed to write file";
                return -1;
            }
        }

        close(fd);
        return 0;
    };

    auto on_receive = [&](TaskID task,
                          const std::string &source_hostname,
                          const Attributes &attributes,
                          std::vector<rapid::Buffer> &buffer_list,
                          rapid::RapidTransfer::OnReceiveEndCallback &on_success,
                          rapid::RapidTransfer::OnReceiveEndCallback &on_failure) -> int
    {
        size_t chunk_size = std::stoi(attributes.at("size"));
        buffer_list.push_back({.addr = addr, .length = chunk_size});
        if (!FLAGS_path.empty())
            on_success = on_success_callback;
        return 0;
    };

    ret = engine->startListener(FLAGS_listen, on_receive);
    if (ret)
    {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    cleanup_func = [&]()
    {
        engine->shutdownListener();
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
    };

    std::signal(SIGINT, signalHandler);
    while (true)
        std::this_thread::yield();

    return 0;
}

int sender()
{
    auto engine = rapid::RapidTransfer::Create("rdma_reliable", FLAGS_device);
    LOG_ASSERT(engine);

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

    std::string path = FLAGS_path;
    size_t file_size = dram_buffer_size;
    int fd = -1;
    if (!path.empty())
    {
        struct stat st;
        ret = stat(path.c_str(), &st);
        if (ret)
            PLOG(WARNING) << "Failed to stat file: " << path << ", fallback to send dummy data";
        else if (st.st_mode != S_IFREG)
            LOG(WARNING) << "Not a regular file: " << path << ", fallback to send dummy data";
        else
        {
            file_size = st.st_size;
            fd = open(path.c_str(), O_RDONLY);
            if (fd < 0)
            {
                LOG(ERROR) << "Failed to open file";
                engine->unregisterLocalMemory(addr);
                freeMemoryPool(addr, dram_buffer_size);
                return -1;
            }
        }
    }

    size_t chunk_size = std::min(file_size, dram_buffer_size);
    rapid::Buffer buf = {.addr = addr, .length = chunk_size};
    Attributes attributes;
    attributes["size"] = std::to_string(chunk_size);
    if (fd >= 0)
    {
        if ((ssize_t)chunk_size != readFully(fd, addr, chunk_size))
        {
            LOG(ERROR) << "Failed to read file fully";
            return -1;
        }
    }

    TaskID task = engine->send({FLAGS_target}, attributes, {buf});
    while (true)
    {
        auto status = engine->getStatus(task, nullptr);
        if (status == rapid::FAILED)
        {
            LOG(ERROR) << "Failed to send data to remote";
            return -1;
        }

        if (status == rapid::SUCCESS)
            break;
    }

    LOG(INFO) << "Sending completed";
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
