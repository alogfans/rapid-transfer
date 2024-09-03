// protocol.h
// Copyright (C) 2024 Feng Ren

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "rapid_transfer.h"

namespace rapid
{
    struct Protocol
    {
        Protocol() {}
        virtual ~Protocol() {}
        Protocol(const Protocol &) = delete;
        Protocol &operator=(const Protocol &) = delete;

        virtual TaskID allocateTask() = 0;

        virtual int freeTask(TaskID task) = 0;

        virtual int prepareSend(TaskID task,
                                std::vector<Attributes> &request_list,
                                const std::vector<std::string> &target_list,
                                const Attributes &attributes,
                                const std::vector<Buffer> &buffers) = 0;

        virtual int issueSend(TaskID task, const std::vector<Attributes> &response_list) = 0;

        virtual int prepareReceive(TaskID task,
                                   const Attributes &request,
                                   Attributes &response,
                                   const RapidTransfer::OnReceiveCallback &on_receive) = 0;

        virtual int setFailedStatus(TaskID task) = 0;

        virtual Status getStatus(TaskID task, size_t *transferred_bytes) = 0;

        virtual int registerBuffer(void *addr, size_t length) = 0;

        virtual int unregisterBuffer(void *addr) = 0;
    };
}

#endif // PROTOCOL_H
