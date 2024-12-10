// Copyright 2024 Feng Ren

#ifndef CONTEXT_H_
#define CONTEXT_H_

#include <sys/time.h>

#include <unordered_set>

#include "controller.h"
#include "packet_manager.h"

namespace rapid {
class Context {
   public:
    Context();

    Context(size_t mtu_size, size_t max_packets, size_t queue_capacity);

    ~Context();

    int construct(const std::string &device_name, uint8_t rdma_port,
                  int gid_index);

    int deconstruct();

    int registerMcastNode(const std::string &multicast_addr);

    int unregisterMcastNode(const std::string &multicast_addr);

    TaskID send(const std::string &peer_name,
                const std::vector<Buffer> &buffer_list, bool multicast = false);

    TaskID receive(const std::string &peer_name,
                   const std::vector<Buffer> &buffer_list);

    Status getStatus(TaskID task_id, size_t *transferred_bytes);

    int freeTask(TaskID task_id);

    int prepareConnection(const std::string &peer_addr, Attributes &local);

    int setupConnection(const std::string &peer_addr, const Attributes &peer);

    int registerLocalMemory(void *addr, size_t length);

    int unregisterLocalMemory(void *addr);

    int runStep();

   private:
    int pollCompletedPackets(int cq_index, uint64_t current_ts);

    int sendDataPackets(uint64_t current_ts);

    int sendAckPackets(uint64_t current_ts);

    void updateRTO(uint64_t rtt);

    int processReceivedPacket(uint64_t current_ts, ibv_wc &wc);

    int submitNormalRecvWR(PacketHandle &handle);

   private:
    struct Task {
        int session;
        uint32_t last_sn;
        bool is_send;
    };

   private:
    Controller controller_;
    PacketManager packet_manager_;

    std::unordered_map<TaskID, Task> task_map_;
    std::atomic<TaskID> next_task_id_;

    struct SessionInfo {
        PacketHandle ack_handle;
    };
    std::unordered_map<int, SessionInfo> active_session_map_;

    const static size_t kDefaultSendTimeout = 8000;  // 8us
    uint64_t send_timeout_;
    uint32_t local_arena_lkey_;

    const static size_t kNumReceiveHandles = 128;
    std::vector<PacketHandle> recv_handles_;

    struct Stats {
        Stats()
            : request_data_packets(0),
              send_data_packets(0),
              recv_data_packets(0) {}
        std::atomic<uint64_t> request_data_packets;
        std::atomic<uint64_t> send_data_packets;
        std::atomic<uint64_t> recv_data_packets;
    };

    Stats stats_;
};
}  // namespace rapid

#endif  // CONTEXT_H_
