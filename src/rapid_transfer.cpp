// rapid_transfer.cpp
// Copyright (C) 2024 Feng Ren

#include "rapid_transfer.h"
#include "protocol.h"
#include "protocols/rdma_reliable/rdma_reliable_protocol.h"
#include "session_manager.h"

namespace rapid
{
    std::shared_ptr<RapidTransfer> RapidTransfer::Create(const std::string &protocol,
                                                         const std::string &device_name,
                                                         const std::string &local_hostname,
                                                         uint8_t rdma_port,
                                                         int gid_index)
    {
        if (protocol == "rdma_reliable")
        {
            auto engine = std::make_shared<RapidTransfer>(device_name);
            engine->session_manager_ = new SessionManager();
            auto protocol_impl = new RdmaReliableProtocol();
            engine->protocol_ = protocol_impl;

            std::string hostname = local_hostname;
            if (hostname.empty())
            {
                const static size_t kHostnameBufLength = 1024;
                char hostname_buf[kHostnameBufLength];
                int ret = gethostname(hostname_buf, kHostnameBufLength);
                if (ret)
                {
                    PLOG(ERROR) << "Failed to get hostname";
                    return nullptr;
                }
            }

            int ret = protocol_impl->construct(hostname, device_name, rdma_port, gid_index);
            if (ret)
            {
                LOG(ERROR) << "Failed to construct protocol";
                return nullptr;
            }

            return engine;
        }

        LOG(ERROR) << "Unrecognized protocol";
        return nullptr;
    }

    RapidTransfer::RapidTransfer(const std::string &device_name)
        : session_manager_(nullptr),
          protocol_(nullptr) {}

    RapidTransfer::~RapidTransfer()
    {
        delete protocol_;
        delete session_manager_;
    }

    TaskID RapidTransfer::send(const std::vector<std::string> &target_list,
                               const Attributes &attributes,
                               const std::vector<Buffer> &buffer_list)
    {
        std::vector<Attributes> request_list, response_list;

        TaskID task_id = protocol_->allocateTask(SEND);
        if (task_id < 0)
        {
            LOG(ERROR) << "Unable to allocate task";
            return task_id;
        }

        int ret = protocol_->prepareSend(task_id, request_list, target_list, attributes, buffer_list);
        if (ret)
        {
            LOG(ERROR) << "Failed to prepare send request";
            protocol_->setFailedStatus(task_id);
            return task_id;
        }

        for (size_t i = 0; i < target_list.size(); ++i)
        {
            Attributes response;
            ret = session_manager_->connect(target_list[i], request_list[i], response);
            if (ret)
            {
                LOG(ERROR) << "Failed to connect target: " << target_list[i];
                protocol_->setFailedStatus(task_id);
                return task_id;
            }

            if (response.count("_error"))
            {
                LOG(ERROR) << "Peer rejects the connection: " << response.at("_error");
                protocol_->setFailedStatus(task_id);
                return task_id;
            }

            response_list.push_back(response);
        }

        ret = protocol_->issueSend(task_id, response_list);
        if (ret)
        {
            LOG(ERROR) << "Failed to issue send request";
            protocol_->setFailedStatus(task_id);
        }

        return task_id;
    }

    Status RapidTransfer::getStatus(TaskID task, size_t *transferred_bytes)
    {
        return protocol_->getStatus(task, transferred_bytes);
    }

    int RapidTransfer::freeTask(TaskID task_id)
    {
        return protocol_->freeTask(task_id);
    }

    int RapidTransfer::registerLocalMemory(void *addr, size_t length)
    {
        return protocol_->registerLocalMemory(addr, length);
    }

    int RapidTransfer::unregisterLocalMemory(void *addr)
    {
        return protocol_->unregisterLocalMemory(addr);
    }

    int RapidTransfer::startListener(const std::string &listen_address, const OnReceiveBeginCallback &on_receive_begin)
    {
        auto on_accept = [=](const Attributes &request, Attributes &response) -> int
        {
            TaskID task_id = protocol_->allocateTask(RECEIVE);
            if (task_id < 0)
            {
                LOG(ERROR) << "Unable to allocate task";
                response["_error"] = "unable to allocate task";
                return task_id;
            }
            int ret = protocol_->prepareReceive(task_id, request, response, on_receive_begin);
            if (ret)
                response["_error"] = "unable to start receive task";
            return ret;
        };
        return session_manager_->startListener(listen_address, on_accept);
    }

    int RapidTransfer::shutdownListener()
    {
        return session_manager_->shutdownListener();
    }
} // namespace rapid
