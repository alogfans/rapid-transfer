// rdma_endpoint.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_endpoint.h"
#include "protocols/rdma_reliable/rdma_reliable_protocol.h"

#include <cassert>
#include <cstddef>
#include <glog/logging.h>

namespace rapid
{
    const static uint8_t MAX_HOP_LIMIT = 16;
    const static uint8_t TIMEOUT = 14;
    const static uint8_t RETRY_CNT = 7;

    RdmaEndPoint::RdmaEndPoint(RdmaContext &context)
        : context_(context),
          status_(INITIALIZING) {}

    RdmaEndPoint::~RdmaEndPoint()
    {
        if (!qp_list_.empty())
            deconstruct();
    }

    int RdmaEndPoint::construct(ibv_cq *send_cq,
                                ibv_cq *recv_cq,
                                size_t num_qp_list,
                                size_t max_sge_per_wr,
                                size_t max_wr_depth,
                                size_t max_inline_bytes)
    {
        if (status_.load(std::memory_order_relaxed) != INITIALIZING)
        {
            PLOG(ERROR) << "Endpoint has already been constructed";
            return -1;
        }

        qp_list_.resize(num_qp_list);

        max_wr_depth_ = (int)max_wr_depth;
        send_wr_depth_list_ = new volatile int[num_qp_list];
        recv_wr_depth_list_ = new volatile int[num_qp_list];
        if (!send_wr_depth_list_ || !recv_wr_depth_list_)
        {
            PLOG(ERROR) << "Failed to allocate memory for work request depth list";
            return -1;
        }
        for (size_t i = 0; i < num_qp_list; ++i)
        {
            send_wr_depth_list_[i] = 0;
            recv_wr_depth_list_[i] = 0;
            ibv_qp_init_attr attr;
            memset(&attr, 0, sizeof(attr));
            attr.send_cq = send_cq;
            attr.recv_cq = recv_cq;
            attr.sq_sig_all = false;
            attr.qp_type = IBV_QPT_RC;
            attr.cap.max_send_wr = attr.cap.max_recv_wr = max_wr_depth;
            attr.cap.max_send_sge = attr.cap.max_recv_sge = max_sge_per_wr;
            attr.cap.max_inline_data = max_inline_bytes;
            qp_list_[i] = ibv_create_qp(context_.pd(), &attr);
            if (!qp_list_[i])
            {
                PLOG(ERROR) << "Failed to create QP";
                return -1;
            }
        }

        status_.store(UNCONNECTED, std::memory_order_relaxed);
        return 0;
    }

    int RdmaEndPoint::deconstruct()
    {
        for (size_t i = 0; i < qp_list_.size(); ++i)
        {
            if (send_wr_depth_list_[i] || recv_wr_depth_list_[i])
                PLOG(WARNING) << "Outstanding work requests found, CQ will not be generated";

            if (ibv_destroy_qp(qp_list_[i]))
            {
                PLOG(ERROR) << "Failed to destroy QP";
                return -1;
            }
        }
        qp_list_.clear();
        delete[] send_wr_depth_list_;
        delete[] recv_wr_depth_list_;
        return 0;
    }

    int RdmaEndPoint::destroyQP()
    {
        return deconstruct();
    }

    void RdmaEndPoint::disconnect()
    {
        RWSpinlock::WriteGuard guard(lock_);
        disconnectUnlocked();
    }

    void RdmaEndPoint::disconnectUnlocked()
    {
        for (size_t i = 0; i < qp_list_.size(); ++i)
        {
            if (send_wr_depth_list_[i] || recv_wr_depth_list_[i])
                PLOG(WARNING) << "Outstanding work requests found, CQ will not be generated";
        }
        ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RESET;
        for (size_t i = 0; i < qp_list_.size(); ++i)
        {
            if (ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE))
                PLOG(ERROR) << "Failed to modity QP to RESET";
        }
        peer_nic_path_.clear();
        for (size_t i = 0; i < qp_list_.size(); ++i)
        {
            send_wr_depth_list_[i] = 0;
            recv_wr_depth_list_[i] = 0;
        }
        status_.store(UNCONNECTED, std::memory_order_release);
    }

    int RdmaEndPoint::postSendRequest(const std::vector<Request *> &request_list)
    {
        int qp_index = 0;
        int wr_count = std::min(max_wr_depth_ - send_wr_depth_list_[qp_index], (int)request_list.size());
        if (wr_count == 0)
            return 0;

        ibv_sge sge_list[wr_count];
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            auto &sge = sge_list[i];
            sge.addr = (uint64_t)request->addr;
            sge.length = request->length;
            sge.lkey = request->lkey;
        }

        ibv_send_wr wr_list[wr_count], *bad_wr = nullptr;
        memset(wr_list, 0, sizeof(ibv_send_wr) * wr_count);
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            auto &wr = wr_list[i];
            wr.wr_id = (uint64_t)request;
            wr.opcode = IBV_WR_SEND;
            wr.num_sge = 1;
            wr.sg_list = &sge_list[i];
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
            request->qp_depth = &send_wr_depth_list_[qp_index];
        }
        __sync_fetch_and_add(&send_wr_depth_list_[qp_index], wr_count);
        int rc = ibv_post_send(qp_list_[qp_index], wr_list, &bad_wr);
        if (rc)
        {
            PLOG(ERROR) << "ibv_post_send failed";
            while (bad_wr)
            {
                int i = bad_wr - wr_list;
                request_list[i]->status = FAILED;
                __sync_fetch_and_sub(&send_wr_depth_list_[qp_index], 1);
                bad_wr = bad_wr->next;
            }
        }
        return wr_count;
    }

    int RdmaEndPoint::postReceiveRequest(const std::vector<Request *> &request_list)
    {
        int qp_index = 0;
        int wr_count = std::min(max_wr_depth_ - recv_wr_depth_list_[qp_index], (int)request_list.size());
        if (wr_count == 0)
            return 0;

        ibv_sge sge_list[wr_count];
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            auto &sge = sge_list[i];
            sge.addr = (uint64_t)request->addr;
            sge.length = request->length;
            sge.lkey = request->lkey;
        }

        ibv_recv_wr wr_list[wr_count], *bad_wr = nullptr;
        memset(wr_list, 0, sizeof(ibv_recv_wr) * wr_count);
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            auto &wr = wr_list[i];
            wr.wr_id = (uint64_t)request;
            wr.num_sge = 1;
            wr.sg_list = &sge_list[i];
            wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
            request->qp_depth = &recv_wr_depth_list_[qp_index];
        }
        __sync_fetch_and_add(&recv_wr_depth_list_[qp_index], wr_count);
        int rc = ibv_post_recv(qp_list_[qp_index], wr_list, &bad_wr);
        if (rc)
        {
            PLOG(ERROR) << "ibv_post_send failed";
            while (bad_wr)
            {
                int i = bad_wr - wr_list;
                request_list[i]->status = FAILED;
                __sync_fetch_and_sub(&recv_wr_depth_list_[qp_index], 1);
                bad_wr = bad_wr->next;
            }
        }
        return wr_count;
    }

    std::vector<uint32_t> RdmaEndPoint::qpNum() const
    {
        std::vector<uint32_t> ret;
        for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
            ret.push_back(qp_list_[qp_index]->qp_num);
        return ret;
    }

    int RdmaEndPoint::setupConnection(const std::string &peer_gid, uint16_t peer_lid, std::vector<uint32_t> peer_qp_num_list)
    {
        if (qp_list_.size() != peer_qp_num_list.size())
        {
            std::string message = "QP count mismatch in peer and local endpoints, check MC_MAX_EP_PER_CTX";
            LOG(ERROR) << message;
            return -1;
        }

        for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
        {
            int ret = setupConnection(qp_index, peer_gid, peer_lid, peer_qp_num_list[qp_index]);
            if (ret)
                return ret;
        }

        status_.store(CONNECTED, std::memory_order_relaxed);
        return 0;
    }

    int RdmaEndPoint::setupConnection(int qp_index, const std::string &peer_gid, uint16_t peer_lid, uint32_t peer_qp_num)
    {
        if (qp_index < 0 || qp_index > (int)qp_list_.size())
            return -1;
        auto &qp = qp_list_[qp_index];

        // Any state -> RESET
        ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RESET;
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE))
        {
            std::string message = "Failed to modity QP to RESET";
            PLOG(ERROR) << message;
            return -1;
        }

        // RESET -> INIT
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = context_.portNum();
        attr.pkey_index = 0;
        attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        {
            std::string message = "Failed to modity QP to INIT, check local context port num";
            PLOG(ERROR) << message;
            return -1;
        }

        // INIT -> RTR
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = context_.activeMTU();
        ibv_gid peer_gid_raw;
        std::istringstream iss(peer_gid);
        for (int i = 0; i < 16; ++i)
        {
            int value;
            iss >> std::hex >> value;
            peer_gid_raw.raw[i] = static_cast<uint8_t>(value);
            if (i < 15)
                iss.ignore(1, ':');
        }
        attr.ah_attr.grh.dgid = peer_gid_raw;
        // TODO gidIndex and portNum must fetch from REMOTE
        attr.ah_attr.grh.sgid_index = context_.gidIndex();
        attr.ah_attr.grh.hop_limit = MAX_HOP_LIMIT;
        attr.ah_attr.dlid = peer_lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.static_rate = 0;
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = context_.portNum();
        attr.dest_qp_num = peer_qp_num;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 16;
        attr.min_rnr_timer = 12; // 12 in previous implementation
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN))
        {
            std::string message = "Failed to modity QP to RTR, check mtu, gid, peer lid, peer qp num";
            PLOG(ERROR) << message;
            return -1;
        }

        // RTR -> RTS
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.timeout = TIMEOUT;
        attr.retry_cnt = RETRY_CNT;
        attr.rnr_retry = 7; // or 7,RNR error
        attr.sq_psn = 0;
        attr.max_rd_atomic = 16;

        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
        {
            std::string message = "Failed to modity QP to RTS";
            PLOG(ERROR) << message;
            return -1;
        }

        return 0;
    }
}
