// session_manager.cpp
// Copyright (C) 2024 Feng Ren

#include "session_manager.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <json/json.h>
#include <poll.h>
#include <sys/socket.h>

#include <set>

namespace rapid {
using namespace coro_rpc;
using namespace async_simple::coro;

static inline bool isValidPort(int port) { return port >= 0 && port <= 65535; }

static inline int parseHostPort(const std::string &address,
                                std::string &hostname, uint16_t &port) {
    size_t pos = address.find(':');
    if (pos == std::string::npos) return -1;
    hostname = address.substr(0, pos);
    port = (uint16_t)std::stoi(address.substr(pos + 1));
    return 0;
}

SessionManager::~SessionManager() {
    shutdownListener();
    sessions_.clear();
}

std::string SessionManager::exchangeMetadata(std::string request_json) {
    Attributes request, response;
    if (readAttributes(request_json, request)) return "<error>";
    if (on_accept_("<undefined>", request, response)) return "<error>";
    std::string response_json;
    if (writeAttributes(request_json, response)) return "<error>";
    return request_json;
}

int SessionManager::startListener(const std::string &address,
                                  const OnAcceptCallback &on_accept,
                                  const OnErrorCallback &on_close) {
    std::string hostname;
    uint16_t port;
    if (parseHostPort(address, hostname, port)) {
        PLOG(ERROR) << "Illegal address format";
        return -1;
    }
    server_ = new coro_rpc::coro_rpc_server(1, port);
    server_->register_handler<&SessionManager::exchangeMetadata>(this);
    coro_rpc::err_code err = server_->start();
    if (err) return -1;
    return 0;
}

int SessionManager::shutdownListener() {
    delete server_;
    server_ = nullptr;
    return 0;
}

int SessionManager::connect(const std::string &address,
                            const Attributes &request, Attributes &response) {
    coro_rpc_client client;
    auto conn_result = async_simple::coro::syncAwait(client.connect(address));
    if (conn_result.val() != 0) {
        LOG(ERROR) << "Failed to connect to master: " << conn_result.message();
        return -1;
    }

    std::string request_json;
    if (writeAttributes(request_json, request)) return -1;
    auto request_result = client.send_request<&SessionManager::exchangeMetadata>(request_json);
    std::optional<std::string> result = async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<std::optional<std::string>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get replica list: "
                           << result.error().msg;
                co_return "";
            }
            co_return result->result();
        }());

    if (!result) return -1;
    if (readAttributes(result.value(), response)) return -1;
    RWSpinlock::WriteGuard guard(sessions_lock_);
    sessions_.insert(address);
    return 0;
}

bool SessionManager::isMulticastAddress(const std::string &address) {
    if (address.find(":") != address.npos) return false;
    std::istringstream iss(address);
    std::string token;
    std::vector<int> bytes;
    while (std::getline(iss, token, '.')) {
        bytes.push_back(std::stoi(token));
    }
    if (bytes.size() != 4) {
        return false;
    }
    return bytes[0] >= 224 && bytes[0] <= 239;
}

int SessionManager::disconnect(const std::string &address) {
    RWSpinlock::WriteGuard guard(sessions_lock_);
    sessions_.erase(address);
    return 0;
}

bool SessionManager::hasConnection(const std::string &address) {
    RWSpinlock::ReadGuard guard(sessions_lock_);
    return sessions_.count(address);
}

int SessionManager::readAttributes(const std::string &json_string, Attributes &attr) {
    Json::CharReaderBuilder reader;
    Json::Value json_object;
    std::string errs;
    if (json_string.empty()) return -2;  // Representing EOF
    std::istringstream iss(json_string);
    if (!Json::parseFromStream(reader, iss, &json_object, &errs)) {
        LOG(ERROR) << "Failed to parse: " << errs;
        return -1;
    }
    for (const auto &key : json_object.getMemberNames())
        attr[key] = json_object[key].asString();
    return 0;
}

int SessionManager::writeAttributes(std::string &json_string, const Attributes &attr) {
    Json::Value json_object;
    for (const auto &pair : attr) json_object[pair.first] = pair.second;
    Json::StreamWriterBuilder writer;
    json_string = Json::writeString(writer, json_object);
    return 0;
}
}  // namespace rapid
