// perf_test.cpp
//
// Performance test for RapidTransfer v1
// Measures throughput and latency for various transfer sizes
//
// Copyright (C) 2024 Feng Ren

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <sstream>
#include <thread>
#include <vector>

#include "rapid_transfer.h"

using namespace rapid::v1;
using rapid::Buffer;
using rapid::RemoteBuffer;
using rapid::Status;
using rapid::TaskID;

// Statistics structure
struct TransferStats {
    std::string test_name;
    size_t size_bytes;
    double avg_latency_ms;
    double min_latency_ms;
    double max_latency_ms;
    double throughput_gbps;
    double stddev_ms;
    int iterations;
    int success_count;
    int timeout_count;
};

// Calculate statistics
TransferStats calculateStats(const std::string& name, size_t size_bytes,
                             const std::vector<double>& latencies_ms,
                             int timeouts) {
    TransferStats stats;
    stats.test_name = name;
    stats.size_bytes = size_bytes;
    stats.iterations = latencies_ms.size() + timeouts;
    stats.success_count = latencies_ms.size();
    stats.timeout_count = timeouts;

    if (!latencies_ms.empty()) {
        stats.avg_latency_ms =
            std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0) /
            latencies_ms.size();
        stats.min_latency_ms =
            *std::min_element(latencies_ms.begin(), latencies_ms.end());
        stats.max_latency_ms =
            *std::max_element(latencies_ms.begin(), latencies_ms.end());

        // Calculate standard deviation
        double variance = 0.0;
        for (double lat : latencies_ms) {
            variance += (lat - stats.avg_latency_ms) *
                       (lat - stats.avg_latency_ms);
        }
        variance /= latencies_ms.size();
        stats.stddev_ms = std::sqrt(variance);

        // Calculate throughput (GB/s)
        // 1 GB/s = 8 Gbps, throughput = size / time
        double avg_latency_sec = stats.avg_latency_ms / 1000.0;
        stats.throughput_gbps = (size_bytes / avg_latency_sec) / (1024.0 * 1024.0 * 1024.0) * 8.0;
    }

    return stats;
}

// Print statistics table
void printStats(const std::vector<TransferStats>& all_stats) {
    std::cout << "\n"
              << std::string(120, '=') << "\n";
    std::cout << "Performance Test Results:\n";
    std::cout << std::string(120, '=') << "\n";

    std::cout << std::left << std::setw(25) << "Test Name"
              << std::right << std::setw(12) << "Size"
              << std::setw(10) << "Iter"
              << std::setw(10) << "Success"
              << std::setw(12) << "Avg Lat"
              << std::setw(12) << "Min Lat"
              << std::setw(12) << "Max Lat"
              << std::setw(12) << "StdDev"
              << std::setw(12) << "Throughput"
              << "\n";
    std::cout << std::left << std::setw(25) << ""
              << std::right << std::setw(12) << "(bytes)"
              << std::setw(10) << ""
              << std::setw(10) << ""
              << std::setw(12) << "(ms)"
              << std::setw(12) << "(ms)"
              << std::setw(12) << "(ms)"
              << std::setw(12) << "(ms)"
              << std::setw(12) << "(Gbps)"
              << "\n";
    std::cout << std::string(120, '-') << "\n";

    for (const auto& stats : all_stats) {
        std::string size_str;
        if (stats.size_bytes < 1024) {
            size_str = std::to_string(stats.size_bytes) + " B";
        } else if (stats.size_bytes < 1024 * 1024) {
            size_str = std::to_string(stats.size_bytes / 1024) + " KB";
        } else if (stats.size_bytes < 1024 * 1024 * 1024) {
            size_str = std::to_string(stats.size_bytes / (1024 * 1024)) + " MB";
        } else {
            size_str =
                std::to_string(stats.size_bytes / (1024 * 1024 * 1024)) + " GB";
        }

        std::cout << std::left << std::setw(25) << stats.test_name
                  << std::right << std::setw(12) << size_str
                  << std::setw(10) << stats.iterations
                  << std::setw(10) << stats.success_count
                  << std::setw(12) << std::fixed << std::setprecision(3)
                  << stats.avg_latency_ms
                  << std::setw(12) << std::fixed << std::setprecision(3)
                  << stats.min_latency_ms
                  << std::setw(12) << std::fixed << std::setprecision(3)
                  << stats.max_latency_ms
                  << std::setw(12) << std::fixed << std::setprecision(3)
                  << stats.stddev_ms
                  << std::setw(12) << std::fixed << std::setprecision(3)
                  << stats.throughput_gbps << "\n";
    }
    std::cout << std::string(120, '=') << "\n";
}

// Run sender for performance test
void runPerfSender(const std::string& peer_address,
                   const std::string& device_name,
                   size_t test_size,
                   int iterations, int timeout_sec) {
    // Configure rail
    RailConfig rail_config;
    rail_config.device_name = device_name;

    // Create sender engine
    auto engine = RapidTransfer::Create(rail_config, "");
    if (!engine) {
        std::cerr << "[Sender] Failed to create engine\n";
        return;
    }

    // Wait for receiver to be ready
    std::cout << "[Sender] Waiting for receiver to be ready...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto recv_buffer_info_opt = engine->getRemoteBufferInfo(peer_address);
    if (!recv_buffer_info_opt) {
        std::cerr << "[Sender] Failed to get receiver buffer info\n";
        return;
    }

    auto recv_buffer_info = *recv_buffer_info_opt;

    std::cout << "\n[Sender] Testing size: " << test_size << " bytes\n";

    // Allocate send buffer
    char* send_buffer = new char[test_size];

    // Fill with test pattern (for validation)
    const char test_pattern[] = "RapidTransfer v1 Perf Test - Data ";
    for (size_t i = 0; i < test_size; i += sizeof(test_pattern) - 1) {
        size_t copy_len =
            std::min(sizeof(test_pattern) - 1, test_size - i);
        std::memcpy(send_buffer + i, test_pattern, copy_len);
    }

    // Register memory
    int ret = engine->registerLocalMemory(send_buffer, test_size);
    if (ret) {
        std::cerr << "[Sender] Failed to register memory\n";
        delete[] send_buffer;
        return;
    }

    // Prepare local and remote buffers
    std::vector<Buffer> local_buffers;
    local_buffers.push_back({send_buffer, test_size});

    std::vector<RemoteBuffer> remote_buffers;
    remote_buffers.push_back({reinterpret_cast<void*>(recv_buffer_info.addr),
                              recv_buffer_info.length, recv_buffer_info.rkey});

    std::vector<double> latencies_ms;
    int timeouts = 0;

    // Run iterations
    for (int iter = 0; iter < iterations; iter++) {
        // Measure time
        auto start = std::chrono::high_resolution_clock::now();

        TaskID task_id =
            engine->write(peer_address, local_buffers, remote_buffers);
        if (task_id < 0) {
            std::cerr << "[Sender] Failed to post write\n";
            continue;
        }

        // Wait for completion
        Status status = Status::PENDING;
        size_t transferred = 0;
        int wait_count = 0;
        const int max_wait = timeout_sec * 100;

        while (wait_count < max_wait && status == Status::PENDING) {
            engine->runStep();
            status = engine->getStatus(task_id, &transferred);
            if (status == Status::PENDING) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            wait_count++;
        }

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed_ms =
            std::chrono::duration<double, std::milli>(end - start).count();

        if (status == Status::SUCCESS) {
            latencies_ms.push_back(elapsed_ms);
            if ((iter + 1) % 10 == 0) {
                std::cout << "[Sender] Iteration " << (iter + 1) << "/"
                          << iterations
                          << ": latency=" << std::fixed << std::setprecision(3)
                          << elapsed_ms << " ms, throughput="
                          << std::fixed << std::setprecision(3)
                          << ((test_size / (elapsed_ms / 1000.0)) /
                              (1024.0 * 1024.0 * 1024.0) * 8.0)
                          << " Gbps\n";
            }
        } else {
            timeouts++;
            std::cerr << "[Sender] Iteration " << iter << " timed out or "
                      << (status == Status::FAILED ? "failed" : "pending")
                      << "\n";
        }

        engine->freeTask(task_id);

        // Small delay between iterations
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Calculate and print statistics
    std::string test_name = "write_" + std::to_string(test_size);
    TransferStats stats =
        calculateStats(test_name, test_size, latencies_ms, timeouts);

    std::vector<TransferStats> all_stats;
    all_stats.push_back(stats);
    printStats(all_stats);

    // Cleanup
    engine->unregisterLocalMemory(send_buffer);
    delete[] send_buffer;
    engine->shutdown();

    std::cout << "[Sender] Performance test complete\n";
}

// Run receiver for performance test
void runPerfReceiver(const std::string& listen_address,
                     const std::string& device_name, size_t test_size) {
    // Configure rail
    RailConfig rail_config;
    rail_config.device_name = device_name;

    // Create receiver engine
    auto engine = RapidTransfer::Create(rail_config, listen_address);
    if (!engine) {
        std::cerr << "[Receiver] Failed to create engine\n";
        return;
    }

    // Allocate receive buffer
    char* recv_buffer = new char[test_size];
    std::memset(recv_buffer, 0, test_size);

    int ret = engine->registerLocalMemory(recv_buffer, test_size);
    if (ret) {
        std::cerr << "[Receiver] Failed to register memory\n";
        delete[] recv_buffer;
        return;
    }

    // Set buffer info for RPC sharing
    RapidTransfer::BufferInfo buffer_info;
    buffer_info.addr = reinterpret_cast<uint64_t>(recv_buffer);
    buffer_info.length = test_size;
    buffer_info.rkey = 0;
    engine->setBufferInfo(buffer_info);

    std::cout << "[Receiver] Registered receive buffer at "
              << static_cast<void*>(recv_buffer)
              << ", size=" << test_size << " bytes\n";
    std::cout << "[Receiver] Ready for performance testing (running for 300 "
                 "seconds)...\n";

    // Run progress engine
    const int max_wait_seconds = 300;
    int wait_count = 0;

    while (wait_count < max_wait_seconds * 100) {
        engine->runStep();
        wait_count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::cout << "[Receiver] Test complete\n";

    // Cleanup
    engine->unregisterLocalMemory(recv_buffer);
    delete[] recv_buffer;
    engine->shutdown();

    std::cout << "[Receiver] Shut down complete\n";
}

int main(int argc, char* argv[]) {
    // Initialize Google Logging
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    std::cout << "=== RapidTransfer v1 Performance Test ===\n\n";

    // Default values
    std::string device_name = "mlx5_0";
    std::string mode = "sender";
    std::string peer_address = "localhost:12349";
    int iterations = 100;
    int timeout_sec = 10;
    size_t test_size = 1024 * 1024;  // Default 1 MB

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--device" && i + 1 < argc) {
            device_name = argv[++i];
        } else if (arg == "--mode" && i + 1 < argc) {
            mode = argv[++i];
        } else if (arg == "--peer" && i + 1 < argc) {
            peer_address = argv[++i];
        } else if (arg == "--iter" && i + 1 < argc) {
            iterations = std::stoi(argv[++i]);
        } else if (arg == "--timeout" && i + 1 < argc) {
            timeout_sec = std::stoi(argv[++i]);
        } else if (arg == "--size" && i + 1 < argc) {
            test_size = std::stoull(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]\n"
                      << "Options:\n"
                      << "  --device <name>    RDMA device name (default: mlx5_0)\n"
                      << "  --mode <mode>      Role: sender or receiver (default: "
                         "sender)\n"
                      << "  --peer <addr>      Peer address (default: "
                         "localhost:12349)\n"
                      << "  --iter <n>         Iterations per test size (default: "
                         "100)\n"
                      << "  --timeout <sec>    Timeout per transfer in seconds "
                         "(default: 10)\n"
                      << "  --sizes <s1,s2,..> Custom test sizes in bytes "
                         "(default: 4K-16M)\n"
                      << "  --help             Show this help message\n\n"
                      << "Examples:\n"
                      << "  Terminal 1 (Receiver): " << argv[0]
                      << " --mode receiver\n"
                      << "  Terminal 2 (Sender):   " << argv[0]
                      << " --mode sender --peer <receiver_ip>:12349\n"
                      << "  Custom size: " << argv[0]
                      << " --mode sender --size 1048576 --iter 1000\n";
            return 0;
        }
    }

    std::cout << "Configuration:\n"
              << "  Device: " << device_name << "\n"
              << "  Mode: " << mode << "\n"
              << "  Peer: " << peer_address << "\n"
              << "  Iterations: " << iterations << "\n"
              << "  Timeout: " << timeout_sec << " seconds\n"
              << "  Test size: " << test_size << " bytes\n\n";

    if (mode == "receiver") {
        std::string listen_addr = "0.0.0.0:12349";
        runPerfReceiver(listen_addr, device_name, test_size);
    } else if (mode == "sender") {
        runPerfSender(peer_address, device_name, test_size, iterations,
                     timeout_sec);
    } else {
        std::cerr << "Invalid mode: " << mode
                  << ". Use 'sender' or 'receiver'.\n";
        return 1;
    }

    return 0;
}
