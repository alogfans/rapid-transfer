// rapid_transfer.cpp
// Copyright (C) 2024 Feng Ren

#include "rapid_transfer.h"
#include "protocol.h"
#include "protocols/rdma_reliable/rdma_reliable_protocol.h"
#include "session_manager.h"

namespace rapid
{
    const static uint16_t kDefaultOOBCommPort = 12348;

    std::shared_ptr<RapidTransfer> RapidTransfer::Create(const std::string &protocol)
    {
        return std::make_shared<RapidTransfer>();
    }

    RapidTransfer::RapidTransfer()
        : session_manager_(nullptr),
          protocol_(nullptr)
    {
        session_manager_ = new SessionManager();
        protocol_ = new RdmaReliableProtocol();

        char hostname[1024];
        gethostname(hostname, 1024);
        ((RdmaReliableProtocol *)protocol_)->construct(hostname, "mlx5_2", 1, 3);
    }

    RapidTransfer::~RapidTransfer()
    {
        ((RdmaReliableProtocol *)protocol_)->deconstruct();
        delete protocol_;
        delete session_manager_;
    }

    TaskID RapidTransfer::send(const std::vector<std::string> &target_list,
                               const Attributes &attributes,
                               const std::vector<Buffer> &buffers)
    {
        std::vector<Attributes> request_list, response_list;
        const size_t target_count = target_list.size();

        TaskID task = protocol_->allocateTask();
        if (task < 0)
            return task;

        protocol_->prepareSend(task, request_list, target_list, attributes, buffers);
        response_list.resize(target_count);

        for (size_t i = 0; i < target_list.size(); ++i)
        {
            int ret = session_manager_->connect(target_list[i], kDefaultOOBCommPort, request_list[i], response_list[i]);
            if (ret)
                protocol_->setFailedStatus(task);
        }

        protocol_->issueSend(task, response_list);
        return task;
    }

    Status RapidTransfer::getStatus(TaskID task, size_t *transferred_bytes)
    {
        return protocol_->getStatus(task, transferred_bytes);
    }

    int RapidTransfer::registerBuffer(void *addr, size_t length)
    {
        return protocol_->registerBuffer(addr, length);
    }

    int RapidTransfer::unregisterBuffer(void *addr)
    {
        return protocol_->unregisterBuffer(addr);
    }

    int RapidTransfer::start(const OnReceiveCallback &on_receive)
    {
        auto on_accept = [=](const Attributes &request, Attributes &response) -> int
        {
            TaskID task = protocol_->allocateTask();
            if (task < 0)
                return task;
            return protocol_->prepareReceive(task, request, response, on_receive);
        };
        return session_manager_->start(kDefaultOOBCommPort, on_accept);
    }

    int RapidTransfer::shutdown()
    {
        return session_manager_->shutdown();
    }
} // namespace rapid
