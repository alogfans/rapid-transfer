// rapid_transfer_perf.cpp
//
// Performance benchmark tool for RapidTransfer
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
DEFINE_string(device, "mlx5_3", "RDMA device name to use");
DEFINE_string(target_hostname, "optane21", "Target hostname (and port, if needed)");
DEFINE_uint32(first_port, 12345, "First TCP port for connecting");
DEFINE_uint32(threads, 8, "Number of concurrent threads");
DEFINE_uint32(block_size, 65536, "Access granularity");

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

int receiveThread(int thread_id)
{
    uint16_t port = FLAGS_first_port + thread_id;
    std::string local_hostname = "client-" + std::to_string(thread_id);
    auto engine = rapid::RapidTransfer::Create("rdma_reliable", FLAGS_device, local_hostname);
    LOG_ASSERT(engine);

    const size_t dram_buffer_size = 64 * 1024 * 1024;
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

    auto on_receive = [&](TaskID task,
                          const std::string &source_hostname,
                          const Attributes &attributes,
                          std::vector<rapid::Buffer> &buffer_list,
                          rapid::RapidTransfer::OnReceiveEndCallback &on_success,
                          rapid::RapidTransfer::OnReceiveEndCallback &on_failure) -> int
    {
        size_t chunk_size = std::stoi(attributes.at("size"));
        buffer_list.push_back({.addr = addr, .length = chunk_size});
        return 0;
    };

    ret = engine->startListener(":" + std::to_string(port), on_receive);
    if (ret)
    {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    while (true)
        std::this_thread::yield();

    return 0;
}

std::atomic<bool> g_running = true;
std::atomic<uint64_t> g_transferred_bytes = 0;

int sendThread(pthread_barrier_t *barrier, int thread_id)
{
    std::string local_hostname = "client-" + std::to_string(thread_id);
    auto engine = rapid::RapidTransfer::Create("rdma_reliable", FLAGS_device, local_hostname);
    LOG_ASSERT(engine);
    uint64_t transferred_bytes = 0;

    const size_t dram_buffer_size = 64 * 1024 * 1024;
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

    pthread_barrier_wait(barrier);
    size_t chunk_size = FLAGS_block_size;
    while (g_running)
    {
        rapid::Buffer buf = {.addr = addr, .length = chunk_size};
        Attributes attributes;
        attributes["size"] = std::to_string(chunk_size);
        uint16_t port = FLAGS_first_port + lrand48() % FLAGS_threads;
        auto target = FLAGS_target_hostname + ":" + std::to_string(port);
        TaskID task = engine->send({target}, attributes, {buf});
        while (true)
        {
            auto status = engine->getStatus(task, nullptr);
            if (status == rapid::FAILED)
            {
                LOG(ERROR) << "Failed to send data to remote";
                break;
            }

            if (status == rapid::SUCCESS)
                break;
        }
        engine->freeTask(task);
        transferred_bytes += chunk_size;
    }
    pthread_barrier_wait(barrier);
    g_transferred_bytes += transferred_bytes;
    return 0;
}

int receiver()
{
    std::thread workers[FLAGS_threads];
    for (uint32_t i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(receiveThread, (int)i);
    for (uint32_t i = 0; i < FLAGS_threads; ++i)
        workers[i].join();
    return 0;
}

int sender()
{
    std::thread workers[FLAGS_threads];
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, FLAGS_threads + 1);
    timeval tv_begin, tv_end;

    for (uint32_t i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(sendThread, &barrier, (int)i);

    pthread_barrier_wait(&barrier);
    gettimeofday(&tv_begin, nullptr);

    sleep(5);
    g_running = false;

    pthread_barrier_wait(&barrier);
    gettimeofday(&tv_end, nullptr);

    for (uint32_t i = 0; i < FLAGS_threads; ++i)
        workers[i].join();

    pthread_barrier_destroy(&barrier);
    double duration = (tv_end.tv_sec - tv_begin.tv_sec) + (tv_end.tv_usec - tv_begin.tv_usec) / 1000000.0;
    LOG(INFO) << g_transferred_bytes.load() / duration / 1024.0 / 1024.0 / 1024.0;
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
