// rdma_endpoint.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_ud_endpoint_store.h"
#include "rdma_ud_endpoint.h"

namespace rapid
{
    int RdmaUDEndPointStore::construct(ibv_cq *send_cq,
                                       ibv_cq *recv_cq,
                                       size_t num_qp_list,
                                       size_t max_sge_per_wr,
                                       size_t max_wr_depth,
                                       size_t max_inline_bytes)
    {
        send_cq_ = send_cq;
        recv_cq_ = recv_cq;
        max_wr_depth_ = (int)max_wr_depth;
        qp_list_.resize(num_qp_list);
        send_wr_depth_list_ = new volatile int[num_qp_list];
        recv_wr_depth_list_ = new volatile int[num_qp_list];

        for (size_t i = 0; i < num_qp_list; ++i)
        {
            send_wr_depth_list_[i] = 0;
            recv_wr_depth_list_[i] = 0;
            ibv_qp_init_attr attr;
            memset(&attr, 0, sizeof(attr));
            attr.send_cq = send_cq;
            attr.recv_cq = recv_cq;
            attr.sq_sig_all = false;
            attr.qp_type = IBV_QPT_UD;
            attr.cap.max_send_wr = attr.cap.max_recv_wr = max_wr_depth;
            attr.cap.max_send_sge = attr.cap.max_recv_sge = max_sge_per_wr;
            attr.cap.max_inline_data = max_inline_bytes;
            qp_list_[i] = ibv_create_qp(context_.pd(), &attr);
            if (!qp_list_[i])
            {
                PLOG(ERROR) << "Failed to create QP";
                return -1;
            }
            if (setupQueuePair(qp_list_[i]))
            {
                PLOG(ERROR) << "Failed to configure QP";
                return -1;
            }
        }
        return 0;
    }

    int RdmaUDEndPointStore::deconstruct()
    {
        endpoint_map_.clear();
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
        send_wr_depth_list_ = nullptr;
        recv_wr_depth_list_ = nullptr;
        return 0;
    }

    std::shared_ptr<RdmaUDEndPoint> RdmaUDEndPointStore::getOrCreateEndpoint(const std::string &peer_nic_path)
    {
        if (peer_nic_path.empty())
        {
            LOG(ERROR) << "Invalid peer NIC path";
            return nullptr;
        }

        {
            RWSpinlock::ReadGuard guard(endpoint_map_lock_);
            if (endpoint_map_.count(peer_nic_path))
                return endpoint_map_[peer_nic_path];
        }

        RWSpinlock::WriteGuard guard(endpoint_map_lock_);
        if (endpoint_map_.count(peer_nic_path))
            return endpoint_map_[peer_nic_path];

        auto endpoint = std::make_shared<RdmaUDEndPoint>(*this);
        endpoint_map_[peer_nic_path] = endpoint;
        return endpoint;
    }

    int RdmaUDEndPointStore::deleteEndpoint(const std::string &peer_nic_path)
    {
        RWSpinlock::WriteGuard guard(endpoint_map_lock_);
        endpoint_map_.erase(peer_nic_path);
        return 0;
    }

    int RdmaUDEndPointStore::setupQueuePair(ibv_qp *qp)
    {
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
        attr.qkey = 0xdeadbeef;
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY))
        {
            std::string message = "Failed to modity QP to INIT, check local context port num";
            PLOG(ERROR) << message;
            return -1;
        }

        // INIT -> RTR
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE))
        {
            std::string message = "Failed to modity QP to RTR";
            PLOG(ERROR) << message;
            return -1;
        }

        // RTR -> RTS
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.sq_psn = 0;
        if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN))
        {
            std::string message = "Failed to modity QP to RTS";
            PLOG(ERROR) << message;
            return -1;
        }

        return 0;
    }

    std::vector<uint32_t> RdmaUDEndPointStore::qpNum() const
    {
        std::vector<uint32_t> ret;
        for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
            ret.push_back(qp_list_[qp_index]->qp_num);
        return ret;
    }

    int RdmaUDEndPointStore::postSendRequest(const std::vector<Request *> &request_list, ibv_ah *ah, uint32_t remote_qpn, int qp_index)
    {
        int wr_count = std::min(max_wr_depth_ - send_wr_depth_list_[qp_index], (int)request_list.size());
        if (wr_count == 0)
            return 0;

        ibv_sge sge_list[kMaxSgeCount * wr_count];
        int actual_sge_count = 0;
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            for (int j = 0; j < kMaxSgeCount; j++)
            {
                if (!request->addr[j])
                    break;
                auto &sge = sge_list[i * kMaxSgeCount + j];
                sge.addr = (uint64_t)request->addr[j];
                sge.length = request->length[j];
                sge.lkey = request->lkey[j];
                actual_sge_count++;
            }
        }

        ibv_send_wr wr_list[wr_count], *bad_wr = nullptr;
        memset(wr_list, 0, sizeof(ibv_send_wr) * wr_count);
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            auto &wr = wr_list[i];
            wr.wr_id = (uint64_t)request;
            wr.opcode = IBV_WR_SEND;
            wr.num_sge = actual_sge_count;
            wr.sg_list = &sge_list[i * kMaxSgeCount];
            wr.send_flags = IBV_SEND_SIGNALED;
            wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
            wr.wr.ud.ah = ah;
            wr.wr.ud.remote_qkey = 0xdeadbeef;
            wr.wr.ud.remote_qpn = remote_qpn;
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

    int RdmaUDEndPointStore::postReceiveRequest(const std::vector<Request *> &request_list, int qp_index)
    {
        int wr_count = std::min(max_wr_depth_ - recv_wr_depth_list_[qp_index], (int)request_list.size());
        if (wr_count == 0)
            return 0;

        ibv_sge sge_list[kMaxSgeCount * wr_count];
        int actual_sge_count = 0;
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            for (int j = 0; j < kMaxSgeCount; j++)
            {
                if (!request->addr[j])
                    break;
                auto &sge = sge_list[i * kMaxSgeCount + j];
                sge.addr = (uint64_t)request->addr[j];
                sge.length = request->length[j];
                sge.lkey = request->lkey[j];
                actual_sge_count++;
            }
        }

        ibv_recv_wr wr_list[wr_count], *bad_wr = nullptr;
        memset(wr_list, 0, sizeof(ibv_recv_wr) * wr_count);
        for (int i = 0; i < wr_count; ++i)
        {
            auto &request = request_list[i];
            auto &wr = wr_list[i];
            wr.wr_id = (uint64_t)request;
            wr.num_sge = actual_sge_count;
            wr.sg_list = &sge_list[i * kMaxSgeCount];
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
}