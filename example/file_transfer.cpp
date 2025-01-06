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
#include <thread>

#include "rapid_transfer.h"

DEFINE_string(role, "sender", "Execution role: sender, receiver");
DEFINE_string(protocol, "rdma_unreliable",
              "Transport protocol: rdma_reliable, rdma_unreliable");
DEFINE_string(path, "", "Path of file to transfer");
DEFINE_string(device, "ibp6s0", "RDMA device name to use");
DEFINE_string(target, "optane21:12348",
              "Target hostname with port, seperated using commas");
DEFINE_string(listen, ":12348", "TCP listen address");
DEFINE_uint32(num_recv_files, 1, "Number of receiving files");
DEFINE_uint32(rdma_port, 1, "RDMA port");
DEFINE_uint32(gid_index, 0, "GID Index");

const static std::string kMulticastAddress = "239.0.0.1";

using namespace rapid;

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
        ret = engine->joinMulticast(kMulticastAddress);
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
    };

    std::vector<std::thread> receiver_list;
    auto on_new_connection = [&](const std::string &peer_name, bool is_join) {
        if (!is_join) return;
        LOG(INFO) << "Arriving connection: " << peer_name;
        receiver_list.emplace_back(std::bind(receiver, peer_name));
    };

    ret = engine->startListener(FLAGS_listen, on_new_connection);
    if (ret) {
        LOG(ERROR) << "Failed to start transfer engine";
        engine->unregisterLocalMemory(addr);
        freeMemoryPool(addr, dram_buffer_size);
        return -1;
    }
    while (start_recv_count.load() < (int)FLAGS_num_recv_files)
        std::this_thread::yield();
    for (auto &receiver : receiver_list) receiver.join();
    cleanup();
    return 0;
}

int sender() {
    auto engine = rapid::RapidTransfer::Create(
        FLAGS_protocol, FLAGS_device, FLAGS_rdma_port, FLAGS_gid_index);
    assert(engine);
    int ret;

    if (FLAGS_protocol == "rdma_unreliable_mcast") {
        ret = engine->joinMulticast(kMulticastAddress);
        if (ret) {
            LOG(ERROR) << "Failed to join multicast group";
            return -1;
        }
        ret = engine->setMulticastReplicas(kMulticastAddress,
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
        target_list.push_back(kMulticastAddress);
    } else {
        target_list = split(FLAGS_target, ',');
    }

    *(uint64_t *)addr = file_size;
    for (auto target : target_list) {
        TaskID task_id = engine->send(target, {{addr, sizeof(uint64_t)}});
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
            TaskID task_id = engine->send(target, {{addr, chunk_size}});
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

    if (FLAGS_role == "sender")
        return sender();
    else if (FLAGS_role == "receiver")
        return receiver();

    LOG(ERROR)
        << "Wrong execution role: should be either 'sender' or 'receiver'";
    exit(EXIT_FAILURE);
}
