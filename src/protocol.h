// protocol.h
// Copyright (C) 2024 Feng Ren

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "rapid_transfer.h"

namespace rapid
{
    enum RequestType
    {
        SEND,
        RECEIVE
    };

    struct Protocol
    {
        Protocol() {}
        virtual ~Protocol() {}
        Protocol(const Protocol &) = delete;
        Protocol &operator=(const Protocol &) = delete;

        virtual TaskID allocateTask(RequestType type) = 0;

        virtual int freeTask(TaskID task_id) = 0;

        virtual int prepareSend(TaskID task_id,
                                std::vector<Attributes> &request_list,
                                const std::vector<std::string> &target_list,
                                const Attributes &attributes,
                                const std::vector<Buffer> &buffers) = 0;

        virtual int issueSend(TaskID task_id, const std::vector<Attributes> &response_list) = 0;

        virtual int prepareReceive(TaskID task_id,
                                   const Attributes &request,
                                   Attributes &response,
                                   const RapidTransfer::OnReceiveBeginCallback &on_receive_begin) = 0;

        virtual int setFailedStatus(TaskID task_id) = 0;

        virtual Status getStatus(TaskID task_id, size_t *transferred_bytes) = 0;

        virtual int registerLocalMemory(void *addr, size_t length) = 0;

        virtual int unregisterLocalMemory(void *addr) = 0;
    };
}

#endif // PROTOCOL_H
