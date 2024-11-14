// session_manager.h
// Copyright (C) 2024 Feng Ren

#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include <netdb.h>

#include <atomic>
#include <thread>

#include "concurrency.h"
#include "rapid_transfer.h"

namespace rapid {
class SessionManager {
   public:
    using OnAcceptCallback = std::function<int(
        const std::string &, const Attributes &, Attributes &)>;

    using OnErrorCallback = std::function<void(const std::string &)>;

    SessionManager() : listen_running_(false), listen_fd_(-1) {}

    virtual ~SessionManager();
    SessionManager(const SessionManager &) = delete;
    SessionManager &operator=(const SessionManager &) = delete;

    int startListener(const std::string &address,
                      const OnAcceptCallback &on_accept,
                      const OnErrorCallback &on_error);

    int shutdownListener();

    int connect(const std::string &address, const Attributes &request,
                Attributes &response);

    int disconnect(const std::string &address);

    bool hasConnection(const std::string &address);

   private:
    void listener();

    int makeConnect(const std::string &address);

    int sendRPC(int fd, const Attributes &request, Attributes &response);

    int readAttributes(int fd, Attributes &attr);

    int writeAttributes(int fd, const Attributes &attr);

   private:
    struct Session {
        int fd;
    };

    std::atomic<bool> listen_running_;
    int listen_fd_;
    std::thread listen_thread_;

    OnAcceptCallback on_accept_;
    OnErrorCallback on_error_;

    RWSpinlock session_map_lock_;
    std::unordered_map<std::string, Session> session_map_;
};
}  // namespace rapid

#endif  // SESSION_MANAGER_H
