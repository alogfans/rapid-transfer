// Copyright 2024 Feng Ren

#ifndef CONTEXT_H_
#define CONTEXT_H_

#include <sys/time.h>

#include <unordered_set>

#include "concurrency.h"
#include "controller.h"
#include "packet_manager.h"
#include "rapid_transfer.h"

namespace rapid {
class Context {
   public:
    // Control packet handler callback type
    using ControlPacketHandler = std::function<int(const std::string& peer_name,
                                                    const uint8_t* data, size_t length)>;

    Context(size_t mtu_size, size_t max_packets, size_t queue_capacity);

    ~Context();

    int construct(const std::string& device_name, uint8_t rdma_port,
                  int gid_index);

    int deconstruct();

    TaskID send(const std::string& peer_name,
                const std::vector<Buffer>& local_buffers,
                const std::vector<Buffer>& remote_buffers = {});

    Status getStatus(TaskID task_id, size_t* transferred_bytes);

    int freeTask(TaskID task_id);

    int prepareConnection(const std::string& peer_addr, Attributes& local);

    int setupConnection(const std::string& peer_addr, const Attributes& peer);

    int registerLocalMemory(void* addr, size_t length);

    int unregisterLocalMemory(void* addr);

    int runStep();

    size_t mtuSize() const { return mtu_size_; }

    // Send control message to peer
    int sendControl(const std::string& peer_name,
                   const std::vector<uint8_t>& message);

    // Set control packet handler (public for UDControlManager)
    void setControlPacketHandler(ControlPacketHandler handler) {
        control_packet_handler_ = std::move(handler);
    }

   private:
    int pollCompletedPackets(int cq_index, uint64_t current_ts);

    int sendDataPackets(uint64_t current_ts);

    int sendAckPackets(uint64_t current_ts);

    void updateRTO(uint64_t rtt);

    void updateWndOnSuccess(int session, uint32_t rwnd, SendQueue& send_queue);

    int processReceivedPacket(uint64_t current_ts, ibv_wc& wc);

    int submitNormalRecvWR(PacketHandle& handle);

    int ensureSessionInitialized(int session);

   private:
    ControlPacketHandler control_packet_handler_;

   private:
    struct Task {
        int session;
        uint32_t last_sn;
        bool is_send;
        void* queue;
    };

   private:
    const uint32_t mtu_size_;
    Controller controller_;
    PacketManager packet_manager_;

    std::unordered_map<TaskID, Task> task_map_;
    std::atomic<TaskID> next_task_id_;

    const static uint32_t kMinSSThreshValue = 2;
    const static uint32_t kResendValue = 2;

    struct SessionInfo {
        SessionInfo()
            : send_packets(0),
              recv_packets(0),
              ack_packets(0),
              cwnd(1),
              rwnd(PacketManager::kWndSize),
              ssthresh(kMinSSThreshValue),
              incr(0),
              send_queue(nullptr),
              last_send_ts(0) {}

        PacketHandle ack_handle;
        uint64_t send_packets;
        uint64_t recv_packets;
        uint64_t ack_packets;

        uint32_t cwnd, rwnd, ssthresh, incr;

        void setup(PacketManager& mgr, int sid) {
            send_queue = &mgr.getSendQueue(sid);
        }

        SendQueue* send_queue;
        AckQueue ack_queue;
        uint64_t last_send_ts;
    };
    std::unordered_map<int, SessionInfo> active_session_map_;
    RWSpinlock active_session_lock_;

    const static size_t kDefaultRTO = 4096;  // 8us
    const static size_t kMinRTO = 128;
    const static size_t kMaxRTO = 8196;
    uint64_t recv_srtt_ = 0, recv_rttval_ = 0, recv_rto_ = kDefaultRTO;
    uint32_t local_arena_lkey_;

    const static size_t kNumReceiveHandles = 128;
    std::vector<PacketHandle> recv_handles_;
    std::unordered_map<void*, int> recv_handles_qp_index_map_;

    struct Stats {
        Stats() : send_packets(0), recv_packets(0), ack_packets(0) {}
        std::atomic<uint64_t> send_packets;
        std::atomic<uint64_t> recv_packets;
        std::atomic<uint64_t> ack_packets;
    };

    Stats stats_;
    RequestCache request_cache_;
};
}  // namespace rapid

#endif  // CONTEXT_H_
