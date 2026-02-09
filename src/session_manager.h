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

    // Callback for handling write requests from remote peers
    // Receives vector of RemoteBuffer descriptors and returns status (0 = success)
    using OnWriteRequestCallback = std::function<int(const std::string &peer_name,
                                                      const std::vector<RemoteBuffer> &remote_buffers)>;

    // Callback for handling read requests from remote peers
    using OnReadRequestCallback = std::function<int(const std::string &peer_name,
                                                     const std::vector<RemoteBuffer> &remote_buffers)>;

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

    // Get the generated session name for a peer address
    std::string getSessionName(const std::string &peer_address);

    // Get the cached RPC client for a peer (returns nullptr if not found)
    coro_rpc::coro_rpc_client* getRPCClient(const std::string &peer_address);

    // Set callbacks for write/read requests
    void setWriteReadCallbacks(const OnWriteRequestCallback &on_write,
                               const OnReadRequestCallback &on_read);

    // RPC handlers for write/read requests (need to be public for coro_rpc)
    int handleWriteRequest(const std::string &peer_name, const std::string &session_name, const std::string &buffers_json);
    int handleReadRequest(const std::string &peer_name, const std::string &session_name, const std::string &buffers_json);

   private:
    std::string exchangeMetadata(std::string request_json);

    int readAttributes(const std::string &str, Attributes &attr);

    int writeAttributes(std::string &str, const Attributes &attr);

   private:
    coro_rpc::coro_rpc_server *server_ = nullptr;
    OnAcceptCallback on_accept_;
    OnErrorCallback on_error_;
    OnWriteRequestCallback on_write_request_;
    OnReadRequestCallback on_read_request_;

    RWSpinlock sessions_lock_;
    std::unordered_set<std::string> sessions_;

    std::atomic<int> uid_{0};

    // Map from original peer address to generated session name (e.g., "localhost:12359" -> "server/0")
    std::unordered_map<std::string, std::string> peer_to_session_map_;

    // Map from original peer address to cached RPC client for reuse
    std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>> peer_client_map_;
};
}  // namespace rapid

#endif  // SESSION_MANAGER_H
