// rapid_transfer_perf.cpp
//
// Performance benchmark tool for RapidTransfer
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
DEFINE_string(device, "mlx5_0", "RDMA device name to use");
DEFINE_string(target_hostname, "optane21", "Target hostname (and port, if needed)");
DEFINE_uint32(first_port, 12345, "First TCP port for connecting");
DEFINE_uint32(threads, 8, "Number of concurrent threads");
DEFINE_uint32(block_size, 65536, "Access granularity");
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

int receiveThread(int thread_id)
{
    uint16_t port = FLAGS_first_port + thread_id;
    auto engine = rapid::RapidTransfer::Create(FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);

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

    std::mutex mutex;
    std::unordered_map<std::string, TaskID> task_id_map;
    auto on_new_connection = [&](const std::string &peer_name, bool is_join)
    {
        LOG(INFO) << "Arriving connection: " << peer_name;
        mutex.lock();
        task_id_map[peer_name] = engine->receive(peer_name, {{addr, FLAGS_block_size}});
        mutex.unlock();
    };

    ret = engine->startListener(":" + std::to_string(port), on_new_connection);
    if (ret)
    {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    while (true)
    {
        mutex.lock();
        auto task_id_map_clone = task_id_map;
        mutex.unlock();
        for (auto &entry : task_id_map)
        {
            auto status = engine->getStatus(entry.second, nullptr);
            if (status == rapid::FAILED)
            {
                LOG(ERROR) << "Failed to send data to remote";
                break;
            }

            if (status == rapid::SUCCESS)
            {
                engine->freeTask(entry.second);
                // auto base = *((char *)addr);
                // for (uint64_t i = 0; i < FLAGS_block_size; ++i)
                //     assert(*((char *)addr + i) == char(base + i % 256));
                entry.second = engine->receive(entry.first, {{addr, FLAGS_block_size}});
            }
        }
        std::this_thread::yield();
    }

    return 0;
}

std::atomic<bool> g_running = true;
std::atomic<uint64_t> g_transferred_bytes = 0;

int sendThread(pthread_barrier_t *barrier, int thread_id)
{
    auto engine = rapid::RapidTransfer::Create(FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);
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
        uint16_t port = FLAGS_first_port + lrand48() % FLAGS_threads;
        auto target = FLAGS_target_hostname + ":" + std::to_string(port);
        // auto base = lrand48();
        // for (uint64_t i = 0; i < chunk_size; ++i)
        //     *((char *)addr + i) = (i + base) % 256;
        TaskID task_id = engine->send(target, {{addr, chunk_size}});
        if (task_id < 0)
        {
            LOG(ERROR) << "Cannot post send request";
            break;
        }

        while (true)
        {
            auto status = engine->getStatus(task_id, nullptr);
            if (status == rapid::FAILED)
            {
                LOG(ERROR) << "Failed to send data to remote";
                break;
            }

            if (status == rapid::SUCCESS)
                break;
        }

        engine->freeTask(task_id);
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

    sleep(20);
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
