// rdma_context.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_CONTEXT_H
#define RDMA_CONTEXT_H

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "concurrency.h"

namespace rapid {
class RdmaRCEndPoint;
class RdmaRCEndPointStore;

enum { SEND_CQ, RECV_CQ };

class RdmaContext {
   public:
    struct Config {
        size_t num_qp_per_endpoint;
        size_t max_sge_per_wr;
        size_t max_wr_per_qp;
        size_t max_inline_bytes;
    };

   public:
    RdmaContext();

    ~RdmaContext();

    int construct(const std::string &device_name, uint8_t rdma_port,
                  int gid_index);

    int deconstruct();

   public:
    int registerMemoryRegion(void *addr, size_t length, int access);

    int unregisterMemoryRegion(void *addr);

    std::pair<uint32_t, uint32_t> key(void *addr);

    bool active() const { return active_; }

    void set_active(bool flag) { active_ = flag; }

    const Config &config() const { return config_; }

   public:
    std::string deviceName() const { return device_name_; }

   public:
    uint16_t lid() const { return lid_; }

    std::string gid() const;

    int gidIndex() const { return gid_index_; }

    ibv_context *context() const { return context_; }

    ibv_pd *pd() const { return pd_; }

    uint8_t portNum() const { return port_; }

    int activeSpeed() const { return active_speed_; }

    ibv_mtu activeMTU() const { return active_mtu_; }

    ibv_comp_channel *compChannel();

    int compVector();

    int eventFd() const { return event_fd_; }

    int poll(int num_entries, ibv_wc *wc, int cq_index = 0);

    int socketId();

    ibv_cq *cq(int type) { return cq_list_[type]; }

   private:
    int openRdmaDevice(const std::string &device_name, uint8_t port,
                       int gid_index);

    int joinNonblockingPollList(int event_fd, int data_fd);

   private:
    std::string device_name_;

    ibv_context *context_ = nullptr;
    ibv_pd *pd_ = nullptr;
    int event_fd_ = -1;

    size_t num_comp_channel_ = 0;
    ibv_comp_channel **comp_channel_ = nullptr;

    uint8_t port_ = 0;
    uint16_t lid_ = 0;
    int gid_index_ = -1;
    int active_speed_ = -1;
    ibv_mtu active_mtu_;
    ibv_gid gid_;

    RWSpinlock memory_regions_lock_;
    std::vector<ibv_mr *> memory_region_list_;
    std::vector<ibv_cq *> cq_list_;

    std::vector<std::thread> background_thread_;
    std::atomic<bool> threads_running_;

    std::atomic<int> next_comp_channel_index_;
    std::atomic<int> next_comp_vector_index_;
    std::atomic<int> next_cq_list_index_;

    volatile bool active_;
    Config config_;
};

}  // namespace rapid

#endif  // RDMA_CONTEXT_H
