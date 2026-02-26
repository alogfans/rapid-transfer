// tcp_bootstrap.h
// Copyright (C) 2026 RapidTransfer Team

#ifndef TCP_BOOTSTRAP_H
#define TCP_BOOTSTRAP_H

#include <string>
#include <optional>
#include <cstdint>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>

namespace rapid {
namespace v1 {

/// TCP Bootstrap for connection establishment and info exchange
/// Supports both client mode (active connection) and server mode (passive listener)
class TcpBootstrap {
public:
    /// Bootstrap message types
    enum MsgType : uint8_t {
        UD_CONNECT_EXCHANGE = 0x01,
        BUFFER_INFO_REQUEST = 0x02,
        BUFFER_INFO_RESPONSE = 0x03
    };

    /// UD connection information (binary format for network transmission)
    /// Protocol: [msg_type(1)] + [payload_size(4)] + [lid(4)] + [gid(16)] + [qp_count(4)] + [qp_nums...]
    struct UDInfo {
        uint32_t lid;
        uint8_t gid[16];
        std::vector<uint32_t> qp_nums;

        // Convert to network format and send
        int send(int fd) const;

        // Receive from network format
        static bool recv(int fd, UDInfo& info);
    };

    /// Buffer information for sharing between peers
    struct BufferInfo {
        uint64_t addr;
        uint64_t length;
        uint32_t rkey;
    } __attribute__((packed));

    // ========== Callback Types ==========

    /// Callback for handling UD connection exchange requests
    /// Called when a peer requests UD connection info
    /// Return true to accept, false to reject
    using UDConnectCallback = std::function<bool(const std::string& peer_addr, UDInfo& local_info, const UDInfo& peer_info)>;

    /// Callback for providing buffer info to peers
    /// Called when a peer requests buffer information
    using BufferInfoCallback = std::function<std::optional<BufferInfo>()>;

    // ========== Client Mode (Active) ==========

    /// Exchange UD connection info with peer (client mode)
    /// Returns 0 on success, -1 on failure
    static int exchangeUDInfo(const std::string& peer_address, const UDInfo& local, UDInfo& peer);

    /// Request buffer info from peer (client mode)
    /// Returns buffer info if successful, nullopt on failure
    static std::optional<BufferInfo> getBufferInfo(const std::string& peer_address);

    // ========== Server Mode (Passive Listener) ==========

    TcpBootstrap();
    ~TcpBootstrap();

    // Non-copyable, non-movable
    TcpBootstrap(const TcpBootstrap&) = delete;
    TcpBootstrap& operator=(const TcpBootstrap&) = delete;

    /// Start TCP bootstrap listener
    /// Returns 0 on success, -1 on failure
    int startListener(const std::string& listen_address);

    /// Stop TCP bootstrap listener
    void stopListener();

    /// Set callback for UD connection exchange
    void setUDConnectCallback(UDConnectCallback callback) { ud_connect_callback_ = std::move(callback); }

    /// Set callback for buffer info requests
    void setBufferInfoCallback(BufferInfoCallback callback) { buffer_info_callback_ = std::move(callback); }

private:
    /// RAII wrapper for TCP socket
    class TcpSocket {
    public:
        TcpSocket(const std::string& host, int port);
        ~TcpSocket();

        bool valid() const { return fd_ >= 0; }
        int fd() const { return fd_; }

    private:
        int fd_;
    };

    /// Helper to send exactly n bytes
    static bool sendAll(int fd, const void* data, size_t len);

    /// Helper to recv exactly n bytes
    static bool recvAll(int fd, void* data, size_t len);

    /// Parse peer address (format: "host:port")
    static bool parseAddress(const std::string& address, std::string& host, int& port);

    /// Accept thread function
    void acceptThread();

    /// Handle client connection
    void handleClient(int client_fd);

    /// Handle UD connect exchange from peer
    int handleUDConnectExchange(int client_fd);

    /// Handle buffer info request from peer
    int handleBufferInfoRequest(int client_fd);

    // ========== Server State ==========
    int listen_fd_{-1};
    std::string listen_address_;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};

    // ========== Callbacks ==========
    UDConnectCallback ud_connect_callback_;
    BufferInfoCallback buffer_info_callback_;
};

} // namespace v1
} // namespace rapid

#endif  // TCP_BOOTSTRAP_H
