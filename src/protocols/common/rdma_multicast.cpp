#include "rdma_multicast.h"

#include <glog/logging.h>
#include <arpa/inet.h>
#include <sys/fcntl.h>

namespace rapid
{

    const static int max_cqe_count = 256;
    const static int max_wr_count = 256;
    const static int max_sge_count = 2;

    static int setNonBlocking(int fd)
    {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1)
        {
            PLOG(ERROR) << "Get file descriptor flags failed";
            return -1;
        }
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        {
            PLOG(ERROR) << "Set file descriptor nonblocking failed";
            return -1;
        }
        return 0;
    }

    RdmaMulticastContext::RdmaMulticastContext() 
        : event_channel_(nullptr), 
          local_addr_(nullptr),
          multicast_addr_(nullptr),
          running_(false),
          num_active_connections_(0)
    {
    }

    RdmaMulticastContext::~RdmaMulticastContext() 
    {
        deconstruct();
    }

    static int getAddress(const std::string &dst, struct sockaddr *addr)
    {
        struct addrinfo *res;
        int ret = getaddrinfo(dst.c_str(), NULL, NULL, &res);
        if (ret) {
            LOG(ERROR) << "Invalid hostname or IP address: " << gai_strerror(ret);
            return ret;
        }

        memcpy(addr, res->ai_addr, res->ai_addrlen);
        freeaddrinfo(res);
        return 0;
    }

    int RdmaMulticastContext::construct(const std::string &local_addr, const std::string &multicast_addr, size_t num_connections)
    {
        event_channel_ = rdma_create_event_channel();
        if (!event_channel_)
        {
            PLOG(ERROR) << "Failed to create event channel";
            return -1;
        }

        int ret = setNonBlocking(event_channel_->fd);
        if (ret)
            return ret;

        running_ = true;
        event_thread_ = std::thread([this]() {
            while (running_)
                processEvents();
        });

        local_addr_ = (struct sockaddr *) &local_addr_storage_;
        ret = getAddress(local_addr, local_addr_);
        if (ret)
            return ret;

        multicast_addr_ = (struct sockaddr *) &multicast_addr_storage_;
        ret = getAddress(multicast_addr, multicast_addr_);
        if (ret)
            return ret;

        connections_.resize(num_connections);
        for (size_t i = 0; i < num_connections; ++i)
        {
            auto &connection = connections_[i];
            ret = rdma_create_id(event_channel_, &connection.cm_id, &connection, RDMA_PS_UDP);
            if (ret)
            {
                PLOG(ERROR) << "Failed to create connection manager id";
                return -1;
            }

            ret = rdma_bind_addr(connection.cm_id, local_addr_);
            if (ret) 
            {
                PLOG(ERROR) << "Failed to bind address";
                return ret;
            }

            ret = rdma_resolve_addr(connection.cm_id, local_addr_, multicast_addr_, 2000);
            if (ret) 
            {
                PLOG(ERROR) << "Failed to resolve address";
                return ret;
            }
        }

        while (num_active_connections_.load() < (int) connections_.size())
            std::this_thread::yield();
        return 0;
    }

    int RdmaMulticastContext::deconstruct()
    {
        for (auto &connection: connections_)
        {
            if (connection.cm_id)
            {
                rdma_leave_multicast(connection.cm_id, multicast_addr_);
                rdma_destroy_id(connection.cm_id);
            }
        }
        connections_.clear();

        if (running_)
        {
            running_ = false;
            event_thread_.join();
        }
        
        if (event_channel_)
        {
            rdma_destroy_event_channel(event_channel_);
            event_channel_ = nullptr;
        }

        return 0;
    }


    int RdmaMulticastContext::registerMemoryRegion(void *addr, size_t length, int access)
    {
        for (size_t i = 0; i < connections_.size(); ++i)
        {
            for (const auto &entry : connections_[i].memory_regions)
            {
                bool start_overlapped = entry->addr <= addr && addr < (char *)entry->addr + entry->length;
                bool end_overlapped = entry->addr < (char *)addr + length && (char *)addr + length <= (char *)entry->addr + entry->length;
                bool covered = addr <= entry->addr && (char *)entry->addr + entry->length <= (char *)addr + length;
                if (start_overlapped || end_overlapped || covered)
                {
                    LOG(ERROR) << "Fail to register memory " << addr << ": overlap existing memory regions";
                    return -1;
                }
            }

            ibv_mr *mr = ibv_reg_mr(connections_[i].pd, addr, length, access);
            if (!mr)
            {
                PLOG(ERROR) << "Fail to register memory " << addr;
                return -1;
            }

            connections_[i].memory_regions.push_back(mr);
        }

        return 0;
    }

    int RdmaMulticastContext::unregisterMemoryRegion(void *addr)
    {
        for (size_t i = 0; i < connections_.size(); ++i)
        {
            auto &memory_regions = connections_[i].memory_regions;
            bool has_removed;
            do
            {
                has_removed = false;
                for (auto iter = memory_regions.begin(); iter != memory_regions.end(); ++iter)
                {
                    if ((*iter)->addr <= addr && addr < (char *)((*iter)->addr) + (*iter)->length)
                    {
                        if (ibv_dereg_mr(*iter))
                        {
                            PLOG(ERROR) << "Fail to unregister memory " << addr;
                            return -1;
                        }
                        memory_regions.erase(iter);
                        has_removed = true;
                        break;
                    }
                }
            } while (has_removed);
        }
        return 0;
    }

    int RdmaMulticastContext::processEvents()
    {
        struct rdma_cm_event *event;
        int ret = rdma_get_cm_event(event_channel_, &event);
        if (ret)
            return ret;

        auto cm_id = event->id;
        switch (event->event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            ret = onAddressResolved(cm_id->context);
            break;
        case RDMA_CM_EVENT_MULTICAST_JOIN:
            ret = onMulticastJoin(cm_id->context, &event->param.ud);
            break;
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_MULTICAST_ERROR:
            LOG(ERROR) << "Async event: " << rdma_event_str(event->event);
            ret = event->status;
            break;
        case RDMA_CM_EVENT_DEVICE_REMOVAL:
            /* Cleanup will occur after test completes. */
            break;
        default:
            break;
        }

        rdma_ack_cm_event(event);
        return ret;
    }

    std::pair<uint32_t, uint32_t> RdmaMulticastContext::key(void *addr, int conn_index)
    {
        if (conn_index < 0 || conn_index >= (int) connections_.size())
            return {0, 0};

        auto &memory_regions = connections_[conn_index].memory_regions;
        for (auto iter = memory_regions.begin(); iter != memory_regions.end(); ++iter)
            if ((*iter)->addr <= addr && addr < (char *)((*iter)->addr) + (*iter)->length)
                return {(*iter)->lkey, (*iter)->rkey};
        return {0, 0};
    }

    int RdmaMulticastContext::postSendRequest(const std::vector<Request *> &request_list, int conn_index)
    {
        if (conn_index < 0 || conn_index >= (int) connections_.size())
            return -1;

        auto &connection = connections_[conn_index];
        int wr_count = std::min(max_wr_count - connection.send_wr_depth,
                                (int)request_list.size());
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
            wr.wr.ud.ah = connection.ah;
            wr.wr.ud.remote_qkey = connection.remote_qkey;
            wr.wr.ud.remote_qpn = connection.remote_qpn;
            request->qp_depth = &connection.send_wr_depth;
        }
        __sync_fetch_and_add(&connection.send_wr_depth, wr_count);
        int rc = ibv_post_send(connection.cm_id->qp, wr_list, &bad_wr);
        if (rc)
        {
            PLOG(ERROR) << "ibv_post_send failed";
            while (bad_wr)
            {
                int i = bad_wr - wr_list;
                request_list[i]->status = FAILED;
                __sync_fetch_and_sub(&connection.send_wr_depth, 1);
                bad_wr = bad_wr->next;
            }
        }
        return wr_count;
    }

    int RdmaMulticastContext::postReceiveRequest(const std::vector<Request *> &request_list, int conn_index)
    {
        if (conn_index < 0 || conn_index >= (int) connections_.size())
            return -1;

        auto &connection = connections_[conn_index];
        int wr_count = std::min(max_wr_count - connection.recv_wr_depth,
                                (int)request_list.size());
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
            request->qp_depth = &connection.recv_wr_depth;
        }
        __sync_fetch_and_add(&connection.recv_wr_depth, wr_count);
        int rc = ibv_post_recv(connection.cm_id->qp, wr_list, &bad_wr);
        if (rc)
        {
            PLOG(ERROR) << "ibv_post_send failed";
            while (bad_wr)
            {
                int i = bad_wr - wr_list;
                request_list[i]->status = FAILED;
                __sync_fetch_and_sub(&connection.recv_wr_depth, 1);
                bad_wr = bad_wr->next;
            }
        }
        return wr_count;
    }

    int RdmaMulticastContext::poll(int num_entries, ibv_wc *wc, int conn_index)
    {
        if (conn_index < 0 || conn_index >= (int) connections_.size())
            return -1;
        int nr_poll = ibv_poll_cq(connections_[conn_index].cq, num_entries, wc);
        if (nr_poll < 0)
        {
            PLOG(ERROR) << "Failed to poll CQ #" << conn_index;
            return -1;
        }
        return nr_poll;
    }

    int RdmaMulticastContext::createQueuePair(Connection *connection)
    {
        auto verbs = connection->cm_id->verbs;
        connection->pd = ibv_alloc_pd(verbs);
        if (!connection->pd)
        {
            PLOG(ERROR) << "Failed to allocate PD";
            return -1;
        }

        connection->cq = ibv_create_cq(verbs, max_cqe_count, this, 0, 0);
        if (!connection->cq) 
        {
            PLOG(ERROR) << "Failed to create CQ";
            return -1;
        }

        struct ibv_qp_init_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.cap.max_send_wr = max_wr_count;
        attr.cap.max_recv_wr = max_wr_count;
        attr.cap.max_send_sge = max_sge_count;
        attr.cap.max_recv_sge = max_sge_count;
        attr.qp_context = this;
        attr.sq_sig_all = false;
        attr.qp_type = IBV_QPT_UD;
        attr.send_cq = connection->cq;
        attr.recv_cq = connection->cq;
        int ret = rdma_create_qp(connection->cm_id, connection->pd, &attr);
        if (ret) {
            PLOG(ERROR) << "Failed to create QP";
            return -1;
        }

        return 0;
    }
    
    int RdmaMulticastContext::onAddressResolved(void *context)
    {
        Connection *connection = (Connection *) context;
        int ret = createQueuePair(connection);
        if (ret)
            return ret;
        
        if (on_init_qp_hook_)
            ret = on_init_qp_hook_(connection->cm_id->qp);
        if (ret)
            return ret;

        ret = rdma_join_multicast(connection->cm_id, multicast_addr_, context);
        if (ret)
        {
            PLOG(ERROR) << "Failed to join multicast";
            return ret;
        }

        return 0;
    }

    int RdmaMulticastContext::onMulticastJoin(void *context, struct rdma_ud_param *param)
    {
        Connection *connection = (Connection *) context;
        char buf[40];
	    inet_ntop(AF_INET6, param->ah_attr.grh.dgid.raw, buf, 40);
        LOG(INFO) << "Joined: dgid " << buf 
                  << " mlid " << param->ah_attr.dlid 
                  << " sl " << param->ah_attr.sl;
        connection->remote_qpn = param->qp_num;
        connection->remote_qkey = param->qkey;
        connection->ah = ibv_create_ah(connection->pd, &param->ah_attr);
        if (!connection->ah)
        {
            PLOG(ERROR) << "Failed to create AH";
            return -1;
        }
        num_active_connections_++;
        return 0;
    }
    
} // namespace rapid
