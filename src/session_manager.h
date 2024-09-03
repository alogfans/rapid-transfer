// session_manager.h
// Copyright (C) 2024 Feng Ren

#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include "rapid_transfer.h"

#include <atomic>
#include <netdb.h>
#include <thread>

namespace rapid
{
    class SessionManager
    {
    public:
        using OnAcceptCallback = std::function<int(const Attributes &, Attributes &)>;

        SessionManager() : listen_running_(false), listen_fd_(-1) {}
        virtual ~SessionManager() { shutdown(); }
        SessionManager(const SessionManager &) = delete;
        SessionManager &operator=(const SessionManager &) = delete;

        int start(uint16_t port, const OnAcceptCallback &on_accept);
        int shutdown();
        int connect(const std::string &hostname,
                    uint16_t rpc_port,
                    const Attributes &request,
                    Attributes &response);

    private:
        void listener();

        int postRequest(struct addrinfo *addr,
                        const Attributes &request,
                        Attributes &response);

        int readAttributes(int fd, Attributes &attr);

        int writeAttributes(int fd, const Attributes &attr);

    private:
        std::atomic<bool> listen_running_;
        int listen_fd_;
        std::thread listen_thread_;
        OnAcceptCallback on_accept_;
    };
}

#endif // SESSION_MANAGER_H
