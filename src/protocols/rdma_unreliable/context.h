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

    int construct(std::string local_addr, const std::string &device_name,
                  uint8_t rdma_port, int gid_index);

    int deconstruct();

    int registerMcastNode(const std::string &multicast_addr);

    int unregisterMcastNode(const std::string &multicast_addr);

    TaskID send(const std::string &peer_name,
                const std::vector<Buffer> &buffer_list, bool multicast = false);

    TaskID receive(const std::string &peer_name,
                   const std::vector<Buffer> &buffer_list);

    Status getStatus(TaskID task_id, size_t *transferred_bytes);

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

    std::unordered_set<int> active_session_set_;

    const static size_t kWindowSize = 128;
    size_t send_wnd_, recv_wnd_;

    const static size_t kDefaultSendTimeout = 8000;  // 8us
    uint64_t send_timeout_;

    uint16_t local_arena_lkey_;
    std::vector<PacketHandle> recv_handles_;
};
}  // namespace rapid

#endif  // CONTEXT_H_
