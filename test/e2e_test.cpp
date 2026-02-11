// v1_e2e_test.cpp
//
// End-to-end test for RapidTransfer v1
// Tests actual data transfer between sender and receiver
//
// Copyright (C) 2024 Feng Ren

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <thread>

#include "rapid_transfer.h"

using namespace rapid::v1;
using rapid::Buffer;
using rapid::Status;
using rapid::TaskID;

void runSender(const std::string& peer_address,
               const std::string& device_name) {
    // Configure rail
    RailConfig rail_config;
    rail_config.device_name = device_name;

    // Create sender engine
    auto engine = RapidTransfer::Create(rail_config, "");
    if (!engine) {
        std::cerr << "[Sender] Failed to create engine\n";
        return;
    }

    // Allocate and prepare send buffer
    const size_t buffer_size = 4096;
    char* send_buffer = new char[buffer_size];

    // Fill with test data pattern
    const char test_pattern[] = "RapidTransfer v1 E2E Test - Message #";
    for (size_t i = 0; i < buffer_size; i += sizeof(test_pattern) - 1) {
        size_t copy_len = std::min(sizeof(test_pattern) - 1, buffer_size - i);
        std::memcpy(send_buffer + i, test_pattern, copy_len);
    }

    // Register memory
    int ret = engine->registerLocalMemory(send_buffer, buffer_size);
    if (ret) {
        std::cerr << "[Sender] Failed to register memory\n";
        delete[] send_buffer;
        return;
    }

    // Prepare local buffer
    std::vector<Buffer> local_buffers;
    local_buffers.push_back({send_buffer, buffer_size});

    // Wait for receiver to be ready and get buffer info via RPC
    std::cout << "[Sender] Getting receiver buffer info via RPC...\n";
    std::this_thread::sleep_for(
        std::chrono::seconds(2));  // Wait for receiver startup

    auto recv_buffer_info_opt = engine->getRemoteBufferInfo(peer_address);
    if (!recv_buffer_info_opt) {
        std::cerr << "[Sender] Failed to get receiver buffer info via RPC\n";
        engine->unregisterLocalMemory(send_buffer);
        delete[] send_buffer;
        return;
    }

    auto recv_buffer_info = *recv_buffer_info_opt;
    std::cout << "[Sender] Receiver buffer info: addr=0x" << std::hex
              << recv_buffer_info.addr << std::dec
              << ", size=" << recv_buffer_info.length << "\n";

    // Prepare remote_buffers with actual receiver buffer address
    std::vector<Buffer> remote_buffers;
    remote_buffers.push_back({reinterpret_cast<void*>(recv_buffer_info.addr),
                              recv_buffer_info.length});

    std::cout << "[Sender] Initiating write to " << peer_address << "\n";

    // Send data (write internally uses send)
    TaskID task_id = engine->write(peer_address, local_buffers, remote_buffers,
                                   "e2e_test_complete");
    if (task_id < 0) {
        std::cerr << "[Sender] Failed to post write\n";
        engine->unregisterLocalMemory(send_buffer);
        delete[] send_buffer;
        return;
    }

    std::cout << "[Sender] Write posted, task_id: " << task_id << "\n";
    std::cout << "[Sender] Running progress engine...\n";

    // Wait for completion
    const int max_wait_seconds = 10;
    int wait_count = 0;
    Status status = Status::PENDING;
    size_t transferred = 0;

    while (wait_count < max_wait_seconds * 100) {
        engine->runStep();

        status = engine->getStatus(task_id, &transferred);
        if (status == Status::SUCCESS) {
            std::cout << "[Sender] Transfer completed successfully!\n";
            std::cout << "[Sender] Transferred: " << transferred << " bytes\n";
            break;
        } else if (status == Status::FAILED) {
            std::cout << "[Sender] Transfer failed!\n";
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        wait_count++;
    }

    if (status == Status::PENDING) {
        std::cout << "[Sender] Transfer timed out after " << max_wait_seconds
                  << " seconds\n";
    }

    engine->freeTask(task_id);
    engine->unregisterLocalMemory(send_buffer);
    delete[] send_buffer;
    engine->shutdown();

    std::cout << "[Sender] Shut down complete\n";
}

void runReceiver(const std::string& listen_address,
                 const std::string& device_name) {
    // Configure rail
    RailConfig rail_config;
    rail_config.device_name = device_name;

    // Create receiver engine
    auto engine = RapidTransfer::Create(rail_config, listen_address);
    if (!engine) {
        std::cerr << "[Receiver] Failed to create engine\n";
        return;
    }

    // Allocate and register receive buffer
    const size_t buffer_size = 4096;
    char* recv_buffer = new char[buffer_size];
    std::memset(recv_buffer, 0, buffer_size);

    int ret = engine->registerLocalMemory(recv_buffer, buffer_size);
    if (ret) {
        std::cerr << "[Receiver] Failed to register memory\n";
        delete[] recv_buffer;
        return;
    }

    // Prepare buffer info to share with sender via RPC
    RapidTransfer::BufferInfo buffer_info;
    buffer_info.addr = reinterpret_cast<uint64_t>(recv_buffer);
    buffer_info.length = buffer_size;
    buffer_info.rkey = 0;  // Not used for UD send/receive

    // Set buffer info for RPC sharing
    engine->setBufferInfo(buffer_info);

    std::cout << "[Receiver] Registered receive buffer at "
              << static_cast<void*>(recv_buffer) << ", size=" << buffer_size
              << " bytes\n";
    std::cout << "[Receiver] Buffer info available via RPC\n";

    // Flag to track if data was received
    std::atomic<bool> data_received{false};
    std::atomic<bool> notification_received{false};

    // Set notification callback
    engine->setNotificationCallback([&](const std::string& peer_name,
                                        TaskID task_id,
                                        const std::string& message) {
        std::cout << "[Receiver] Notification from " << peer_name
                  << ", task_id=" << task_id << ", message=" << message << "\n";
        notification_received = true;
    });

    std::cout << "[Receiver] Waiting for incoming data (will run for 30 "
                 "seconds)...\n";

    // Run progress engine and wait for data
    const int max_wait_seconds = 30;
    int wait_count = 0;

    while (wait_count < max_wait_seconds * 100) {
        engine->runStep();

        // Check if we received data by verifying the buffer content
        // The test pattern should be present if data was received
        if (!data_received &&
            std::strstr(recv_buffer, "RapidTransfer v1 E2E Test") != nullptr) {
            data_received = true;
            std::cout << "[Receiver] Data detected in receive buffer!\n";

            // Verify the received data
            const char expected_pattern[] =
                "RapidTransfer v1 E2E Test - Message #";
            bool data_valid = true;
            size_t validated_bytes = 0;

            for (size_t i = 0; i < buffer_size && data_valid;
                 i += sizeof(expected_pattern) - 1) {
                size_t compare_len =
                    std::min(sizeof(expected_pattern) - 1, buffer_size - i);
                if (std::memcmp(recv_buffer + i, expected_pattern,
                                compare_len) != 0) {
                    data_valid = false;
                } else {
                    validated_bytes += compare_len;
                }
            }

            if (data_valid) {
                std::cout << "[Receiver] Data validation PASSED! Verified "
                          << validated_bytes << " bytes\n";
            } else {
                std::cout << "[Receiver] Data validation FAILED!\n";
                // Print first 128 bytes for debugging
                std::cout << "[Receiver] First 128 bytes of receive buffer:\n";
                for (size_t i = 0; i < std::min(size_t(128), buffer_size);
                     i++) {
                    if (i % 16 == 0) std::cout << "[Receiver] ";
                    std::cout << std::hex << std::setw(2) << std::setfill('0')
                              << static_cast<int>(
                                     static_cast<unsigned char>(recv_buffer[i]))
                              << " ";
                    if ((i + 1) % 16 == 0) std::cout << "\n";
                }
                std::cout << std::dec << std::endl;
            }
        }

        // Exit early if we received both data and notification
        if (data_received && notification_received) {
            std::cout << "[Receiver] Transfer complete and verified!\n";
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        wait_count++;
    }

    if (!data_received) {
        std::cout << "[Receiver] WARNING: No data received after "
                  << max_wait_seconds << " seconds\n";
    }

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

    std::cout << "=== RapidTransfer v1 End-to-End Test ===\n\n";

    // Default values
    std::string device_name = "mlx5_0";
    std::string mode = "sender";
    std::string peer_address = "localhost:12348";

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--device" && i + 1 < argc) {
            device_name = argv[++i];
        } else if (arg == "--mode" && i + 1 < argc) {
            mode = argv[++i];
        } else if (arg == "--peer" && i + 1 < argc) {
            peer_address = argv[++i];
        } else if (arg == "--help") {
            std::cout
                << "Usage: " << argv[0] << " [options]\n"
                << "Options:\n"
                << "  --device <name>    RDMA device name (default: mlx5_0)\n"
                << "  --mode <mode>      Role: sender or receiver (default: "
                   "sender)\n"
                << "  --peer <addr>      Peer address (default: "
                   "localhost:12348)\n"
                << "  --help             Show this help message\n\n"
                << "Examples:\n"
                << "  Terminal 1 (Receiver): " << argv[0]
                << " --mode receiver\n"
                << "  Terminal 2 (Sender):   " << argv[0]
                << " --mode sender --peer <receiver_ip>:12348\n";
            return 0;
        }
    }

    std::cout << "Configuration:\n"
              << "  Device: " << device_name << "\n"
              << "  Mode: " << mode << "\n"
              << "  Peer: " << peer_address << "\n\n";

    if (mode == "receiver") {
        std::string listen_addr = "0.0.0.0:12348";
        runReceiver(listen_addr, device_name);
    } else if (mode == "sender") {
        runSender(peer_address, device_name);
    } else {
        std::cerr << "Invalid mode: " << mode
                  << ". Use 'sender' or 'receiver'.\n";
        return 1;
    }

    return 0;
}
