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
                                                         const std::string &local_name,
                                                         uint8_t rdma_port,
                                                         int gid_index)
    {
        if (protocol == "rdma_reliable")
        {
            auto engine = std::make_shared<RapidTransfer>(device_name);
            engine->session_manager_ = new SessionManager();
            engine->protocol_ = new RdmaReliableProtocol();

            std::string actual_local_name = local_name;
            if (actual_local_name.empty())
            {
                const static size_t kHostnameBufLength = 1024;
                char hostname_buf[kHostnameBufLength];
                int ret = gethostname(hostname_buf, kHostnameBufLength);
                if (ret)
                {
                    PLOG(ERROR) << "Failed to get hostname";
                    return nullptr;
                }
                actual_local_name = hostname_buf;
            }

            int ret = engine->protocol_->construct(actual_local_name, device_name, rdma_port, gid_index);
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

    TaskID RapidTransfer::send(const std::vector<std::string> &peer_name_list,
                               const std::vector<Buffer> &buffer_list)
    {
        for (auto &peer_name : peer_name_list)
        {
            int ret = makeConnectionIfNeeded(peer_name);
            if (ret)
                return ret;
        }
        int ret = protocol_->send(peer_name_list, buffer_list);
        if (ret < 0)
            for (auto &peer_name : peer_name_list)
                session_manager_->disconnect(peer_name);
        return ret;
    }

    TaskID RapidTransfer::receive(const std::string &peer_name, const std::vector<Buffer> &buffer_list)
    {
        // TODO
        int ret = protocol_->receive(peer_name, buffer_list);
        return ret;
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

    int RapidTransfer::startListener(const std::string &listen_address, const OnConnectionStateChange &callback)
    {
        auto on_accept = [=](const Attributes &request, Attributes &response) -> int
        {
            if (!request.count("name"))
            {
                LOG(ERROR) << "Malformed request: missing name";
                return -1;
            }

            auto peer_name = request.at("name");
            int ret = protocol_->prepareConnection(peer_name, response);
            if (ret)
            {
                LOG(ERROR) << "Unable to setup endpoint: get local attributes";
                return -1;
            }

            ret = protocol_->setupConnection(peer_name, request);
            if (ret)
            {
                LOG(ERROR) << "Unable to setup endpoint: set peer attributes";
                return -1;
            }

            if (callback)
                callback(peer_name, true);
            return 0;
        };

        // TBD on closing callback
        // auto on_close = [=]() -> void
        // {
        //     if (callback)
        //         callback(peer_name, false);
        // };

        return session_manager_->startListener(listen_address, on_accept);
    }

    int RapidTransfer::shutdownListener()
    {
        return session_manager_->shutdownListener();
    }

    int RapidTransfer::makeConnectionIfNeeded(const std::string &peer_name)
    {
        if (!session_manager_->hasConnection(peer_name))
        {
            LOG(INFO) << "conn";
            Attributes request, response;
            int ret = protocol_->prepareConnection(peer_name, request);
            if (ret)
            {
                LOG(ERROR) << "Unable to setup endpoint: get local attributes";
                return -1;
            }

            ret = session_manager_->connect(peer_name, request, response);
            if (ret)
            {
                LOG(ERROR) << "Unable to setup endpoint: perform out-of-band communication";
                return -1;
            }

            ret = protocol_->setupConnection(peer_name, response);
            if (ret)
            {
                LOG(ERROR) << "Unable to setup endpoint: set peer attributes";
                session_manager_->disconnect(peer_name);
                return -1;
            }
        }
        return 0;
    }
} // namespace rapid
