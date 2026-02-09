// file_transfer.cpp
//
// Samples code for RapidTransfer, providing option to transfer data from disk
// to disk
//
// Copyright (C) 2024 Feng Ren

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
#include <iostream>
#include <thread>

#include "rapid_transfer.h"

DEFINE_string(role, "sender", "Execution role: sender, receiver");
DEFINE_string(protocol, "rdma_unreliable",
              "Transport protocol: rdma_reliable, rdma_unreliable, "
              "rdma_unreliable_mcast");
DEFINE_string(path, "", "Path of file to transfer");
DEFINE_string(device, "ibp6s0", "RDMA device name to use");
DEFINE_string(target, "optane21:12348",
              "Target hostname with port, seperated using commas");
DEFINE_string(multicast_addr, "239.0.0.1", "Multicast address");
DEFINE_string(listen, ":12348", "TCP listen address");
DEFINE_uint32(num_recv_files, 1, "Number of receiving files");
DEFINE_uint32(rdma_port, 1, "RDMA port");
DEFINE_uint32(gid_index, 0, "GID Index");
DEFINE_bool(use_write_read, false, "Use write/read instead of send/receive");
DEFINE_string(remote_addr, "", "Remote buffer address (hex format, e.g., 0x7f1234000000)");
DEFINE_uint32(remote_rkey, 0, "Remote rkey for write/read operations");

using namespace rapid;

static inline int64_t getCurrentTimeInNano() {
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts)) {
        return -1;
    }
    return (int64_t{ts.tv_sec} * kNanosPerSecond + int64_t{ts.tv_nsec});
}

static void *allocateMemoryPool(size_t size, int socket_id) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { numa_free(addr, size); }

static inline ssize_t writeFully(int fd, const void *buf, size_t len) {
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = write(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0) {
            PLOG(ERROR) << "Socket write failed";
            return rc;
        } else if (rc == 0) {
            LOG(WARNING) << "Socket write incompleted: expected " << len
                         << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline ssize_t readFully(int fd, void *buf, size_t len) {
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = read(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0) {
            PLOG(ERROR) << "Socket read failed";
            return rc;
        } else if (rc == 0) {
            LOG(WARNING) << "Socket read incompleted: expected " << len
                         << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline std::vector<std::string> split(const std::string &s,
                                             char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

int receiver() {
    auto engine = rapid::RapidTransfer::Create(
        FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);
    int ret = 0;

    if (FLAGS_protocol == "rdma_unreliable_mcast") {
        ret = engine->joinMulticast(FLAGS_multicast_addr);
        if (ret) {
            LOG(ERROR) << "Failed to join multicast group";
            return -1;
        }
    }

    const size_t dram_buffer_size = (1ull << 30) * FLAGS_num_recv_files;
    std::atomic<int> start_recv_count(0);
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    if (!addr) {
        LOG(ERROR) << "Failed to allocate memory pool";
        return -1;
    }

    ret = engine->registerLocalMemory(addr, dram_buffer_size);
    if (ret) {
        LOG(ERROR) << "Failed to register memory";
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    auto cleanup = [&]() {
        engine->shutdownListener();
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
    };

    // Print buffer addresses for write/read mode
    if (FLAGS_use_write_read) {
        std::cout << "=== Write/Read Mode Info ===" << std::endl;
        std::cout << "Receiver buffer address: 0x" << std::hex << (uintptr_t)addr << std::dec << std::endl;
        std::cout << "Note: Sender needs to use --remote_addr=0x" << std::hex << (uintptr_t)addr << std::dec << std::endl;
        std::cout << "=========================" << std::endl;
        std::cout.flush();
    }

    auto wait_for_completion = [&](TaskID task_id) {
        while (true) {
            auto status = engine->getStatus(task_id, nullptr);
            if (status == rapid::FAILED) {
                LOG(ERROR) << "Failed to send data to remote";
                return -1;
            }

            if (status == rapid::SUCCESS) return 0;
        }
    };

    auto receiver = [&](const std::string source) -> int {
        std::string path = "/tmp/received";
        if (!FLAGS_path.empty()) path = FLAGS_path;

        auto receiver_index = start_recv_count.fetch_add(1);
        if (receiver_index) path += "." + std::to_string(receiver_index);

        int fd = open(path.c_str(), O_RDWR | O_TRUNC | O_CREAT, 0644);
        if (fd < 0) {
            LOG(ERROR) << "Failed to open file " << FLAGS_path;
            return -1;
        }

        const auto transferred_size = dram_buffer_size / FLAGS_num_recv_files;
        auto start_addr = (char *)addr + transferred_size * receiver_index;

        if (FLAGS_use_write_read) {
            // In write/read mode, the RPC handler will automatically call receive
            // We just need to wait for data to arrive and poll for completion
            LOG(INFO) << "Waiting for write/read data from " << source << " to buffer at 0x"
                      << std::hex << (uintptr_t)start_addr << std::dec;

            // Wait a bit for sender to initiate
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // Poll for completion by checking if data has arrived
            // For now, we'll use a simple timeout mechanism
            auto start_time = std::chrono::steady_clock::now();
            const auto timeout = std::chrono::seconds(30);

            // Check for file size (first 8 bytes)
            while (std::chrono::steady_clock::now() - start_time < timeout) {
                uint64_t file_size = *(uint64_t *)start_addr;
                if (file_size != 0 && file_size < (32ull << 30)) {
                    LOG(INFO) << "Received file size: " << file_size << " bytes";

                    // Wait for all data to arrive
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));

                    // Write to file
                    if ((ssize_t)file_size != writeFully(fd, start_addr, file_size)) {
                        return -1;
                    }
                    close(fd);
                    LOG(INFO) << "File saved to " << path;
                    return 0;
                }
                engine->runStep();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            LOG(ERROR) << "Timeout waiting for data from sender";
            close(fd);
            return -1;
        } else {
            // Original send/receive mode
            TaskID task_id =
                engine->receive(source, {{start_addr, sizeof(uint64_t)}});
            if (wait_for_completion(task_id)) {
                return -1;
            }

            uint64_t file_size = *(uint64_t *)start_addr;
            assert(file_size > 0 && file_size < (32ull << 30));
            for (size_t offset = 0; offset < file_size;
                 offset += transferred_size) {
                size_t chunk_size = std::min(transferred_size, file_size - offset);
                task_id = engine->receive(source, {{start_addr, chunk_size}});
                if (wait_for_completion(task_id)) {
                    return -1;
                }
                if ((ssize_t)chunk_size != writeFully(fd, start_addr, chunk_size)) {
                    return -1;
                }
            }

            close(fd);
            return 0;
        }
    };

    std::vector<std::thread> receiver_list;
    auto on_new_connection = [&](const std::string &peer_name, bool is_join) {
        if (!is_join) return;
        LOG(INFO) << "Arriving connection: " << peer_name;
        if (!FLAGS_use_write_read) {
            receiver_list.emplace_back(std::bind(receiver, peer_name));
        }
    };

    std::cout << "About to start listener..." << std::endl;
    std::cout.flush();

    ret = engine->startListener(FLAGS_listen, on_new_connection);
    if (ret) {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    // In write/read mode, just wait indefinitely for RPC requests
    // In normal mode, wait for all receivers to finish
    if (FLAGS_use_write_read) {
        std::cout << "Write/Read mode: waiting for incoming data..." << std::endl;
        std::cout.flush();
        while (true) {
            engine->runStep();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    } else {
        while (start_recv_count.load() < (int)FLAGS_num_recv_files)
            std::this_thread::yield();
        for (auto &receiver : receiver_list) receiver.join();
    }

    // Extra delay for resend ACK packets
    uint64_t begin_ts = getCurrentTimeInNano();
    while (true) {
        engine->runStep();
        uint64_t current_ts = getCurrentTimeInNano();
        if (current_ts - begin_ts > 100*1000*1000ull) break;
    }

    cleanup();
    return 0;
}

int sender() {
    auto engine = rapid::RapidTransfer::Create(
        FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);
    int ret;

    if (FLAGS_protocol == "rdma_unreliable_mcast") {
        ret = engine->joinMulticast(FLAGS_multicast_addr);
        if (ret) {
            LOG(ERROR) << "Failed to join multicast group";
            return -1;
        }
        ret = engine->setMulticastReplicas(FLAGS_multicast_addr,
                                           split(FLAGS_target, ','));
        if (ret) {
            LOG(ERROR) << "Failed to join multicast group";
            return -1;
        }
    }

    const size_t dram_buffer_size = 1ull << 30;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    if (!addr) {
        LOG(ERROR) << "Failed to allocate memory pool";
        return -1;
    }

    ret = engine->registerLocalMemory(addr, dram_buffer_size);
    if (ret) {
        LOG(ERROR) << "Failed to register memory";
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }

    auto cleanup = [&]() {
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
    };

    std::string path = FLAGS_path;
    size_t file_size = dram_buffer_size;
    int fd = -1;
    if (!path.empty()) {
        struct stat st;
        ret = stat(path.c_str(), &st);
        if (ret)
            PLOG(WARNING) << "Failed to stat file: " << path
                          << ", fallback to send dummy data";
        else if (!(st.st_mode & S_IFREG))
            LOG(WARNING) << "Not a regular file: " << path
                         << ", fallback to send dummy data";
        else {
            file_size = st.st_size;
            fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                LOG(ERROR) << "Failed to open file";
                cleanup();
                return -1;
            }
        }
    }

    auto wait_for_completion = [&](TaskID task_id) {
        while (true) {
            auto status = engine->getStatus(task_id, nullptr);
            if (status == rapid::FAILED) {
                LOG(ERROR) << "Failed to send data to remote";
                return -1;
            }

            if (status == rapid::SUCCESS) return 0;
        }
    };

    std::vector<std::string> target_list;
    std::vector<TaskID> task_id_list;
    if (FLAGS_protocol == "rdma_unreliable_mcast") {
        target_list.push_back(FLAGS_multicast_addr);
    } else {
        target_list = split(FLAGS_target, ',');
    }

    *(uint64_t *)addr = file_size;
    for (auto target : target_list) {
        TaskID task_id;
        if (FLAGS_use_write_read) {
            // Parse remote address from command line
            if (FLAGS_remote_addr.empty()) {
                LOG(ERROR) << "Remote address must be specified for write/read mode";
                cleanup();
                return -1;
            }
            void *remote_addr = reinterpret_cast<void*>(std::stoull(FLAGS_remote_addr, nullptr, 16));
            std::vector<RemoteBuffer> remote_buffers = {{remote_addr, sizeof(uint64_t), FLAGS_remote_rkey}};
            std::vector<Buffer> local_buffers = {{addr, sizeof(uint64_t)}};
            task_id = engine->write(target, local_buffers, remote_buffers);
        } else {
            task_id = engine->send(target, {{addr, sizeof(uint64_t)}});
        }
        task_id_list.push_back(task_id);
    }

    for (auto task_id : task_id_list) {
        if (wait_for_completion(task_id)) {
            cleanup();
            return -1;
        }
    }

    for (size_t offset = 0; offset < file_size; offset += dram_buffer_size) {
        size_t chunk_size = std::min(dram_buffer_size, file_size - offset);
        if (fd >= 0) {
            if ((ssize_t)chunk_size != readFully(fd, addr, chunk_size)) {
                LOG(ERROR) << "Failed to read file fully";
                cleanup();
                return -1;
            }
        }

        task_id_list.clear();
        for (auto target : target_list) {
            TaskID task_id;
            if (FLAGS_use_write_read) {
                void *remote_addr = reinterpret_cast<void*>(std::stoull(FLAGS_remote_addr, nullptr, 16));
                std::vector<RemoteBuffer> remote_buffers = {{remote_addr, chunk_size, FLAGS_remote_rkey}};
                std::vector<Buffer> local_buffers = {{addr, chunk_size}};
                task_id = engine->write(target, local_buffers, remote_buffers);
            } else {
                task_id = engine->send(target, {{addr, chunk_size}});
            }
            task_id_list.push_back(task_id);
        }
        for (auto task_id : task_id_list) {
            if (wait_for_completion(task_id)) {
                cleanup();
                return -1;
            }
        }
    }
    cleanup();
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    FLAGS_minloglevel = google::GLOG_WARNING;

    if (FLAGS_role == "sender")
        return sender();
    else if (FLAGS_role == "receiver")
        return receiver();

    LOG(ERROR)
        << "Wrong execution role: should be either 'sender' or 'receiver'";
    exit(EXIT_FAILURE);
}
