// session_manager.h
// Copyright (C) 2024 Feng Ren

#ifndef SESSION_MANAGER_H
#define SESSION_MANAGER_H

#include <netdb.h>

#include <atomic>
#include <thread>
#include <unordered_set>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "concurrency.h"
#include "rapid_transfer.h"

namespace rapid {
using Attributes = std::unordered_map<std::string, std::string>;

class SessionManager {
   public:
    using OnAcceptCallback = std::function<int(
        const std::string &, const Attributes &, Attributes &)>;

    using OnErrorCallback = std::function<void(const std::string &)>;

    SessionManager() {}

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

    bool isMulticastAddress(const std::string &address);

   private:
    std::string exchangeMetadata(std::string request_json);

    int readAttributes(const std::string &str, Attributes &attr);

    int writeAttributes(std::string &str, const Attributes &attr);

   private:
    coro_rpc::coro_rpc_server *server_ = nullptr;
    OnAcceptCallback on_accept_;
    OnErrorCallback on_error_;

    RWSpinlock sessions_lock_;
    std::unordered_set<std::string> sessions_;

    std::atomic<int> uid_{0};
};
}  // namespace rapid

#endif  // SESSION_MANAGER_H
