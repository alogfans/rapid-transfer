// rapid_transfer_perf.cpp
//
// Performance benchmark tool for RapidTransfer
//
// Copyright (C) 2024 Feng Ren

// Test program
// ./benchmark --role=receiver --protocol=rdma_unreliable --threads=16
// --block_size=4096
// ./benchmark --role=sender --target_hostname=vm-5-3-3 --threads=16
// --protocol=rdma_unreliable --block_size=4096

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include <atomic>
#include <cassert>
#include <csignal>
#include <future>
#include <iomanip>
#include <thread>

#include "rapid_transfer.h"

DEFINE_string(role, "sender", "Execution role: sender, receiver");
DEFINE_string(protocol, "rdma_unreliable",
              "Transport protocol: rdma_reliable, rdma_unreliable, "
              "rdma_unreliable_mcast");
DEFINE_string(device, "ibp6s0", "RDMA device name to use");
DEFINE_string(target_hostname, "optane21", "Target hostname");
DEFINE_uint32(first_port, 12345, "First TCP port for connecting");
DEFINE_uint32(threads, 8, "Number of concurrent threads");
DEFINE_uint32(block_size, 65536, "Access granularity");
DEFINE_uint32(rdma_port, 1, "RDMA port");
DEFINE_uint32(gid_index, 0, "GID Index");
DEFINE_uint32(depth, 16, "Outstanding work requests per thread");

using namespace rapid;

static void *allocateMemoryPool(size_t size, int socket_id) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

int receiveThread(int thread_id) {
    uint16_t port = FLAGS_first_port + thread_id;
    auto engine = rapid::RapidTransfer::Create(
        FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);

    const size_t dram_buffer_size = 64 * 1024 * 1024;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    if (!addr) {
        LOG(ERROR) << "Failed to allocate memory pool";
        return -1;
    }

    int ret = engine->registerLocalMemory(addr, dram_buffer_size);
    if (ret) {
        LOG(ERROR) << "Failed to register memory";
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    std::mutex mutex;
    std::unordered_multimap<std::string, TaskID> task_id_map;
    auto on_new_connection = [&](const std::string &peer_name, bool is_join) {
        LOG(INFO) << "Arriving connection: " << peer_name;
        mutex.lock();
        for (size_t depth = 0; depth < FLAGS_depth; ++depth) {
            auto task_id =
                engine->receive(peer_name, {{addr, FLAGS_block_size}});
            task_id_map.emplace(std::make_pair(peer_name, task_id));
        }
        mutex.unlock();
    };

    ret = engine->startListener(":" + std::to_string(port), on_new_connection);
    if (ret) {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    while (true) {
        mutex.lock();
        for (auto &entry : task_id_map) {
            auto status = engine->getStatus(entry.second, nullptr);
            if (status == rapid::FAILED) {
                LOG(ERROR) << "Failed to send data to remote";
                break;
            } else if (status == rapid::SUCCESS) {
                engine->freeTask(entry.second);
                // auto base = *((char *)addr);
                // for (uint64_t i = 0; i < FLAGS_block_size; ++i)
                //     assert(*((char *)addr + i) == char(base + i % 256));
                entry.second =
                    engine->receive(entry.first, {{addr, FLAGS_block_size}});
            }
        }
        mutex.unlock();
    }

    return 0;
}

std::atomic<bool> g_running = true;
std::atomic<uint64_t> g_transferred_bytes = 0;

int sendThread(pthread_barrier_t *barrier, int thread_id) {
    auto engine = rapid::RapidTransfer::Create(
        FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);
    uint64_t transferred_bytes = 0;

    const size_t dram_buffer_size = 64 * 1024 * 1024;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    if (!addr) {
        LOG(ERROR) << "Failed to allocate memory pool";
        return -1;
    }

    int ret = engine->registerLocalMemory(addr, dram_buffer_size);
    if (ret) {
        LOG(ERROR) << "Failed to register memory";
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    pthread_barrier_wait(barrier);
    size_t chunk_size = FLAGS_block_size;
    while (g_running) {
        uint16_t port = FLAGS_first_port + lrand48() % FLAGS_threads;
        auto target = FLAGS_target_hostname + ":" + std::to_string(port);
        TaskID task_id_list[FLAGS_depth];
        for (size_t depth = 0; depth < FLAGS_depth; depth++) {
            task_id_list[depth] = engine->send(target, {{addr, chunk_size}});
            if (task_id_list[depth] < 0) {
                LOG(ERROR) << "Cannot post send request";
                break;
            }
        }

        for (size_t depth = 0; depth < FLAGS_depth; depth++) {
            while (g_running) {
                auto status = engine->getStatus(task_id_list[depth], nullptr);
                if (status == rapid::FAILED) {
                    LOG(ERROR) << "Failed to send data to remote";
                    break;
                }
                if (status == rapid::SUCCESS) break;
            }
            engine->freeTask(task_id_list[depth]);
            transferred_bytes += chunk_size;
        }
    }
    pthread_barrier_wait(barrier);
    g_transferred_bytes += transferred_bytes;
    return 0;
}

int receiver() {
    std::thread workers[FLAGS_threads];
    for (uint32_t i = 0; i < FLAGS_threads; ++i)
        workers[i] = std::thread(receiveThread, (int)i);
    for (uint32_t i = 0; i < FLAGS_threads; ++i) workers[i].join();
    return 0;
}

int sender() {
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

    for (uint32_t i = 0; i < FLAGS_threads; ++i) workers[i].join();

    pthread_barrier_destroy(&barrier);
    double duration = (tv_end.tv_sec - tv_begin.tv_sec) +
                      (tv_end.tv_usec - tv_begin.tv_usec) / 1000000.0;
    LOG(INFO) << g_transferred_bytes.load() / duration / 1024.0 / 1024.0 /
                     1024.0;
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_role == "sender")
        return sender();
    else if (FLAGS_role == "receiver")
        return receiver();

    LOG(ERROR)
        << "Wrong execution role: should be either 'sender' or 'receiver'";
    exit(EXIT_FAILURE);
}
