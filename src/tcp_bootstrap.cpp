// tcp_bootstrap.cpp
// Copyright (C) 2026 RapidXfer Team

#include "tcp_bootstrap.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>

namespace rapid {
namespace v1 {

// ============================================================================
// TcpSocket RAII Wrapper
// ============================================================================

TcpBootstrap::TcpSocket::TcpSocket(const std::string& host, int port) : fd_(-1) {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        LOG(ERROR) << "[TcpBootstrap] Failed to create socket";
        return;
    }

    // Set socket timeout
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    // Connect to peer
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        LOG(ERROR) << "[TcpBootstrap] Invalid address: " << host;
        close(fd_);
        fd_ = -1;
        return;
    }

    if (connect(fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "[TcpBootstrap] Connection failed to " << host << ":" << port;
        close(fd_);
        fd_ = -1;
    }
}

TcpBootstrap::TcpSocket::~TcpSocket() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

// ============================================================================
// TcpBootstrap Static Methods
// ============================================================================

bool TcpBootstrap::parseAddress(const std::string& address, std::string& host, int& port) {
    size_t colon_pos = address.find_last_of(':');
    if (colon_pos == std::string::npos) {
        LOG(ERROR) << "[TcpBootstrap] Invalid address format: " << address;
        return false;
    }

    host = address.substr(0, colon_pos);
    std::string port_str = address.substr(colon_pos + 1);
    port = std::stoi(port_str);
    return true;
}

// ============================================================================
// Helper Functions for Reliable Transmission
// ============================================================================

bool TcpBootstrap::sendAll(int fd, const void* data, size_t len) {
    size_t total_sent = 0;
    const uint8_t* buf = static_cast<const uint8_t*>(data);

    while (total_sent < len) {
        ssize_t sent = ::send(fd, buf + total_sent, len - total_sent, 0);
        if (sent < 0) {
            LOG(ERROR) << "[TcpBootstrap] sendAll failed: " << strerror(errno);
            return false;
        }
        if (sent == 0) {
            LOG(ERROR) << "[TcpBootstrap] sendAll: connection closed";
            return false;
        }
        total_sent += sent;
    }
    return true;
}

bool TcpBootstrap::recvAll(int fd, void* data, size_t len) {
    size_t total_recv = 0;
    uint8_t* buf = static_cast<uint8_t*>(data);

    while (total_recv < len) {
        ssize_t recv_bytes = ::recv(fd, buf + total_recv, len - total_recv, MSG_WAITALL);
        if (recv_bytes < 0) {
            LOG(ERROR) << "[TcpBootstrap] recvAll failed: " << strerror(errno);
            return false;
        }
        if (recv_bytes == 0) {
            LOG(ERROR) << "[TcpBootstrap] recvAll: connection closed";
            return false;
        }
        total_recv += recv_bytes;
    }
    return true;
}

// ============================================================================
// UDInfo Serialization
// ============================================================================

int TcpBootstrap::UDInfo::send(int fd) const {
    // Calculate payload size (lid + gid + qp_count + qp_nums)
    uint32_t qp_count = static_cast<uint32_t>(qp_nums.size());
    uint32_t payload_size = sizeof(lid) + sizeof(gid) + sizeof(qp_count) +
                            sizeof(qp_nums[0]) * qp_count;

    // Send payload size first (for validation on receiver side)
    if (!TcpBootstrap::sendAll(fd, &payload_size, sizeof(payload_size))) {
        return -1;
    }

    // Send lid
    if (!TcpBootstrap::sendAll(fd, &lid, sizeof(lid))) {
        return -1;
    }

    // Send gid
    if (!TcpBootstrap::sendAll(fd, gid, sizeof(gid))) {
        return -1;
    }

    // Send qp_count
    if (!TcpBootstrap::sendAll(fd, &qp_count, sizeof(qp_count))) {
        return -1;
    }

    // Send qp_nums array
    if (qp_count > 0 && !qp_nums.empty()) {
        if (!TcpBootstrap::sendAll(fd, qp_nums.data(), sizeof(qp_nums[0]) * qp_count)) {
            return -1;
        }
    }

    return 0;
}

bool TcpBootstrap::UDInfo::recv(int fd, UDInfo& info) {
    // First receive payload size for validation
    uint32_t expected_payload_size;
    if (!TcpBootstrap::recvAll(fd, &expected_payload_size, sizeof(expected_payload_size))) {
        return false;
    }

    // Receive lid
    if (!TcpBootstrap::recvAll(fd, &info.lid, sizeof(info.lid))) {
        return false;
    }

    // Receive gid
    if (!TcpBootstrap::recvAll(fd, info.gid, sizeof(info.gid))) {
        return false;
    }

    // Receive qp_count
    uint32_t qp_count;
    if (!TcpBootstrap::recvAll(fd, &qp_count, sizeof(qp_count))) {
        return false;
    }

    // Validate qp_count is reasonable (prevent memory exhaustion)
    constexpr uint32_t MAX_QP_COUNT = 65536;  // Reasonable upper limit
    if (qp_count > MAX_QP_COUNT) {
        LOG(ERROR) << "[TcpBootstrap] Invalid QP count: " << qp_count;
        return false;
    }

    // Resize vector and receive qp_nums
    info.qp_nums.resize(qp_count);
    if (qp_count > 0) {
        if (!TcpBootstrap::recvAll(fd, info.qp_nums.data(), sizeof(info.qp_nums[0]) * qp_count)) {
            return false;
        }
    }

    // Validate payload size matches what we received
    uint32_t actual_payload_size = sizeof(info.lid) + sizeof(info.gid) +
                                   sizeof(qp_count) + sizeof(info.qp_nums[0]) * qp_count;
    if (actual_payload_size != expected_payload_size) {
        LOG(ERROR) << "[TcpBootstrap] Payload size mismatch: expected "
                   << expected_payload_size << ", got " << actual_payload_size;
        return false;
    }

    return true;
}

int TcpBootstrap::exchangeUDInfo(const std::string& peer_address,
                                  const TcpBootstrap::UDInfo& local,
                                  TcpBootstrap::UDInfo& peer) {
    std::string host;
    int port;
    if (!parseAddress(peer_address, host, port)) {
        return -1;
    }

    TcpSocket sock(host, port);
    if (!sock.valid()) {
        return -1;
    }

    // Send message type
    uint8_t msg_type = TcpBootstrap::UD_CONNECT_EXCHANGE;
    if (!sendAll(sock.fd(), &msg_type, sizeof(msg_type))) {
        LOG(ERROR) << "[TcpBootstrap] Failed to send message type";
        return -1;
    }

    // Send local UD info using the new serialization method
    if (local.send(sock.fd()) < 0) {
        LOG(ERROR) << "[TcpBootstrap] Failed to send local UD info";
        return -1;
    }

    LOG(INFO) << "[TcpBootstrap] Sent UD info: LID=" << local.lid
              << ", QP count=" << local.qp_nums.size();

    // Receive peer UD info using the new deserialization method
    if (!UDInfo::recv(sock.fd(), peer)) {
        LOG(ERROR) << "[TcpBootstrap] Failed to receive peer UD info";
        return -1;
    }

    LOG(INFO) << "[TcpBootstrap] Received UD info: LID=" << peer.lid
              << ", QP count=" << peer.qp_nums.size();

    return 0;
}

std::optional<TcpBootstrap::BufferInfo> TcpBootstrap::getBufferInfo(const std::string& peer_address) {
    std::string host;
    int port;
    if (!parseAddress(peer_address, host, port)) {
        return std::nullopt;
    }

    TcpSocket sock(host, port);
    if (!sock.valid()) {
        return std::nullopt;
    }

    // Send request type
    uint8_t msg_type = TcpBootstrap::BUFFER_INFO_REQUEST;
    send(sock.fd(), &msg_type, sizeof(msg_type), 0);

    // Receive response type
    uint8_t resp_type;
    ssize_t recv_len = recv(sock.fd(), &resp_type, sizeof(resp_type), MSG_WAITALL);
    if (recv_len != sizeof(resp_type) || resp_type != TcpBootstrap::BUFFER_INFO_RESPONSE) {
        LOG(ERROR) << "[TcpBootstrap] Invalid response type";
        return std::nullopt;
    }

    // Receive buffer info
    TcpBootstrap::BufferInfo info;
    recv_len = recv(sock.fd(), &info, sizeof(info), MSG_WAITALL);
    if (recv_len != sizeof(info)) {
        LOG(ERROR) << "[TcpBootstrap] Failed to receive buffer info";
        return std::nullopt;
    }

    LOG(INFO) << "[TcpBootstrap] Received buffer info: addr=0x"
              << std::hex << info.addr << std::dec
              << ", size=" << info.length;

    return info;
}

// ============================================================================
// Server Mode (Passive Listener)
// ============================================================================

TcpBootstrap::TcpBootstrap() = default;

TcpBootstrap::~TcpBootstrap() {
    stopListener();
}

int TcpBootstrap::startListener(const std::string& listen_address) {
    if (running_) {
        LOG(WARNING) << "[TcpBootstrap] Listener already running";
        return -1;
    }

    // Parse address
    std::string host;
    int port;
    if (!parseAddress(listen_address, host, port)) {
        LOG(ERROR) << "[TcpBootstrap] Invalid listen address: " << listen_address;
        return -1;
    }

    // Create listening socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        LOG(ERROR) << "[TcpBootstrap] Failed to create socket";
        return -1;
    }

    // Set SO_REUSEADDR
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG(ERROR) << "[TcpBootstrap] Failed to bind to " << listen_address;
        close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    // Listen
    if (listen(listen_fd_, 16) < 0) {
        LOG(ERROR) << "[TcpBootstrap] Failed to listen";
        close(listen_fd_);
        listen_fd_ = -1;
        return -1;
    }

    listen_address_ = listen_address;
    running_ = true;

    // Start accept thread
    accept_thread_ = std::thread(&TcpBootstrap::acceptThread, this);

    LOG(INFO) << "[TcpBootstrap] Listening on " << listen_address;
    return 0;
}

void TcpBootstrap::stopListener() {
    if (!running_) {
        return;
    }

    running_ = false;

    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
        close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    LOG(INFO) << "[TcpBootstrap] Listener stopped";
}

void TcpBootstrap::acceptThread() {
    while (running_) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (running_) {
                LOG(ERROR) << "[TcpBootstrap] Accept failed";
            }
            break;
        }

        // Handle client synchronously (or could use thread pool)
        handleClient(client_fd);
    }
}

void TcpBootstrap::handleClient(int client_fd) {
    // Receive message type
    uint8_t msg_type;
    if (!recvAll(client_fd, &msg_type, sizeof(msg_type))) {
        LOG(ERROR) << "[TcpBootstrap] Failed to receive message type";
        close(client_fd);
        return;
    }

    // Route to appropriate handler
    if (msg_type == UD_CONNECT_EXCHANGE) {
        handleUDConnectExchange(client_fd);
    } else if (msg_type == BUFFER_INFO_REQUEST) {
        handleBufferInfoRequest(client_fd);
    } else {
        LOG(ERROR) << "[TcpBootstrap] Unknown message type: " << (int)msg_type;
        close(client_fd);
    }
}

int TcpBootstrap::handleUDConnectExchange(int client_fd) {
    // Receive peer's UD info
    UDInfo peer_info;
    if (!UDInfo::recv(client_fd, peer_info)) {
        LOG(ERROR) << "[TcpBootstrap] Failed to receive peer UD info";
        close(client_fd);
        return -1;
    }

    // Get local UD info from callback
    UDInfo local_info;
    if (ud_connect_callback_) {
        // Extract peer address from client connection
        struct sockaddr_in peer_addr;
        socklen_t addr_len = sizeof(peer_addr);
        getpeername(client_fd, (struct sockaddr*)&peer_addr, &addr_len);

        char peer_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer_addr.sin_addr, peer_ip, sizeof(peer_ip));
        std::string peer_addr_str = std::string(peer_ip) + ":" + std::to_string(ntohs(peer_addr.sin_port));

        if (!ud_connect_callback_(peer_addr_str, local_info, peer_info)) {
            LOG(WARNING) << "[TcpBootstrap] UD connect callback rejected connection";
            close(client_fd);
            return -1;
        }
    } else {
        LOG(WARNING) << "[TcpBootstrap] No UD connect callback registered";
        close(client_fd);
        return -1;
    }

    // Send local UD info
    if (local_info.send(client_fd) < 0) {
        LOG(ERROR) << "[TcpBootstrap] Failed to send local UD info";
        close(client_fd);
        return -1;
    }

    close(client_fd);
    return 0;
}

int TcpBootstrap::handleBufferInfoRequest(int client_fd) {
    BufferInfo response{0, 0, 0};

    if (buffer_info_callback_) {
        auto info = buffer_info_callback_();
        if (info) {
            response = info.value();
        }
    }

    // Send response type
    uint8_t msg_type = BUFFER_INFO_RESPONSE;
    if (!sendAll(client_fd, &msg_type, sizeof(msg_type))) {
        close(client_fd);
        return -1;
    }

    // Send buffer info
    if (!sendAll(client_fd, &response, sizeof(response))) {
        close(client_fd);
        return -1;
    }

    close(client_fd);
    return 0;
}

} // namespace v1
} // namespace rapid
