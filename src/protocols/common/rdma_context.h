// rdma_context.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_CONTEXT_H
#define RDMA_CONTEXT_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "../../concurrency.h"

namespace rapid
{
    class RdmaEndPoint;

    const static std::string NIC_PATH_DELIM = "@";

    static inline const std::string getServerNameFromNicPath(const std::string &nic_path)
    {
        size_t pos = nic_path.find(NIC_PATH_DELIM);
        if (pos == nic_path.npos)
            return "";
        return nic_path.substr(0, pos);
    }

    static inline const std::string getNicNameFromNicPath(const std::string &nic_path)
    {
        size_t pos = nic_path.find(NIC_PATH_DELIM);
        if (pos == nic_path.npos)
            return "";
        return nic_path.substr(pos + 1);
    }

    static inline const std::string MakeNicPath(const std::string &server_name, const std::string &nic_name)
    {
        return server_name + NIC_PATH_DELIM + nic_name;
    }

    class RdmaContext
    {
    public:
        RdmaContext();

        ~RdmaContext();

        int construct(const std::string &local_hostname,
                      const std::string &device_name,
                      uint8_t rdma_port,
                      int gid_index);

        int deconstruct();

    public:
        int registerMemoryRegion(void *addr, size_t length, int access);

        int unregisterMemoryRegion(void *addr);

        std::pair<uint32_t, uint32_t> key(void *addr);

        bool active() const { return active_; }

        void set_active(bool flag) { active_ = flag; }

    public:
        std::shared_ptr<RdmaEndPoint> getOrCreateEndpoint(const std::string &peer_nic_path);

        int deleteEndpoint(const std::string &peer_nic_path);

    public:
        std::string nicPath() const { return MakeNicPath(local_hostname_, device_name_); }

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

        ibv_cq *cq();

        int cqCount() const { return cq_list_.size(); }

        int poll(int num_entries, ibv_wc *wc, int cq_index = 0);

        int socketId();

    private:
        int openRdmaDevice(const std::string &device_name, uint8_t port, int gid_index);

        int joinNonblockingPollList(int event_fd, int data_fd);

    private:
        std::string local_hostname_;
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

        RWSpinlock endpoint_map_lock_;
        std::unordered_map<std::string, std::shared_ptr<RdmaEndPoint>> endpoint_map_;

        RWSpinlock memory_regions_lock_;
        std::vector<ibv_mr *> memory_region_list_;
        std::vector<ibv_cq *> cq_list_;

        std::vector<std::thread> background_thread_;
        std::atomic<bool> threads_running_;

        std::atomic<int> next_comp_channel_index_;
        std::atomic<int> next_comp_vector_index_;
        std::atomic<int> next_cq_list_index_;

        volatile bool active_;
    };

}

#endif // RDMA_CONTEXT_H
