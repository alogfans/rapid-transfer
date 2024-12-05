// session_manager.cpp
// Copyright (C) 2024 Feng Ren

#include "session_manager.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <json/json.h>
#include <poll.h>
#include <sys/socket.h>

#include <set>

namespace rapid {
static inline ssize_t writeFully(int fd, const void *buf, size_t len) {
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = write(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0) {
            PLOG(ERROR) << "Socket write failed";
            return rc;
        } else if (rc == 0) {
            // LOG(WARNING) << "Socket write incompleted: expected " << len
            //              << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline ssize_t readFully(int fd, void *buf, size_t len) {
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = read(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
            continue;
        else if (rc < 0) {
            PLOG(ERROR) << "Socket read failed";
            return rc;
        } else if (rc == 0) {
            // LOG(WARNING) << "Socket read incompleted: expected " << len
            //              << " bytes, actual " << len - nbytes << " bytes";
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static inline int writeString(int fd, const std::string &str) {
    uint64_t length = str.size();
    if (writeFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
        return -1;
    if (writeFully(fd, str.data(), length) != (ssize_t)length) return -1;
    return 0;
}

static inline std::string readString(int fd) {
    const static size_t kMaxLength = 1ull << 20;
    uint64_t length = 0;
    if (readFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
        return "";
    if (length > kMaxLength) return "";
    std::string str;
    std::vector<char> buffer(length);
    if (readFully(fd, buffer.data(), length) != (ssize_t)length) return "";

    str.assign(buffer.data(), length);
    return str;
}

static inline bool isValidPort(int port) {
    return port >= 0 && port <= 65535;
}

static inline int parseHostPort(const std::string &address,
                                std::string &hostname, uint16_t &port) {
    size_t pos = address.find(':');
    if (pos == std::string::npos) return -1;
    hostname = address.substr(0, pos);
    port = (uint16_t)std::stoi(address.substr(pos + 1));
    return 0;
}

static inline bool setNonBlocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == 0;
}

SessionManager::~SessionManager() {
    shutdownListener();
    for (auto &entry : session_map_) close(entry.second.fd);
    session_map_.clear();
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

    sockaddr_in bind_address;
    int on = 1, listen_fd = -1;
    memset(&bind_address, 0, sizeof(sockaddr_in));
    bind_address.sin_family = AF_INET;
    bind_address.sin_port = htons(port);
    bind_address.sin_addr.s_addr = INADDR_ANY;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        PLOG(ERROR) << "Failed to create socket";
        return -1;
    }

    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
        PLOG(ERROR) << "Failed to set address reusable";
        close(listen_fd);
        return -1;
    }

    if (!setNonBlocking(listen_fd)) {
        PLOG(ERROR) << "Failed to set listen fd non-blocking";
        close(listen_fd);
        return -1;
    }

    if (bind(listen_fd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) < 0) {
        PLOG(ERROR) << "Failed to bind address";
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, 5)) {
        PLOG(ERROR) << "Failed to listen";
        close(listen_fd);
        return -1;
    }

    listen_fd_ = listen_fd;
    on_accept_ = on_accept;
    on_error_ = on_close;
    listen_running_.exchange(true);
    listen_thread_ = std::thread(&SessionManager::listener, this);
    return 0;
}

static std::string getPeerName(int fd) {
    sockaddr_storage addr_buf;
    socklen_t addr_buf_len;
    if (getpeername(fd, (struct sockaddr *)&addr_buf, &addr_buf_len) < 0) {
        PLOG(ERROR) << "Failed to get peer name";
        return "";
    }

    const static size_t HOST_BUF_LEN = 512;
    const static size_t PORT_BUF_LEN = 64;
    char host_buf[HOST_BUF_LEN], port_buf[PORT_BUF_LEN];

    if (getnameinfo((struct sockaddr *)&addr_buf, addr_buf_len, host_buf,
                    HOST_BUF_LEN, port_buf, PORT_BUF_LEN,
                    NI_NUMERICHOST | NI_NUMERICSERV)) {
        PLOG(ERROR) << "Failed to convert peer name to string";
        return "";
    }
    return std::string(host_buf) + ":" + std::string(port_buf) + "/" + std::to_string(fd);
}

void SessionManager::listener() {
    std::vector<pollfd> fd_list;
    std::unordered_map<int, std::string> peer_name_map;
    pollfd listen_pollfd = {listen_fd_, POLLIN, 0};
    fd_list.push_back(listen_pollfd);

    while (listen_running_) {
        int ret = poll(fd_list.data(), fd_list.size(), 500);
        if (ret < 0) {
            PLOG(ERROR) << "Poll error";
            continue;
        }
        if (fd_list[0].revents & (POLLERR | POLLHUP | POLLNVAL)) {
            PLOG(ERROR) << "poll error on listen fd";
            break;
        } else if (fd_list[0].revents & POLLIN) {
            sockaddr_in addr;
            socklen_t addr_len = sizeof(sockaddr_in);
            int conn_fd = accept(listen_fd_, (sockaddr *)&addr, &addr_len);
            if (conn_fd < 0) {
                if (errno != EWOULDBLOCK)
                    PLOG(ERROR) << "Failed to accept socket connection";
                continue;
            }

            if (!setNonBlocking(conn_fd)) {
                PLOG(ERROR) << "Failed to set socket non-blocking";
                close(conn_fd);
                continue;
            }
            pollfd conn_pollfd = {conn_fd, POLLIN, 0};
            fd_list.push_back(conn_pollfd);
            continue;
        }

        for (size_t i = 1; i < fd_list.size(); ++i) {
            if (fd_list[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                int conn_fd = fd_list[i].fd;
                if (!(fd_list[i].revents & POLLHUP))
                    PLOG(ERROR) << "poll error on conn fd " << conn_fd;
                on_error_(peer_name_map[conn_fd]);
                peer_name_map.erase(conn_fd);
                close(conn_fd);
                fd_list.erase(fd_list.begin() + i);
                continue;
            } else if (fd_list[i].revents & POLLIN) {
                int conn_fd = fd_list[i].fd;
                Attributes request, response;
                int ret = readAttributes(conn_fd, request);
                if (ret) {
                    PLOG_IF(ERROR, ret == -1)
                        << "Failed to read request attributes";
                    close(conn_fd);
                    fd_list.erase(fd_list.begin() + i);
                    continue;
                }
                
                auto peer_name = getPeerName(conn_fd);
                if (peer_name.empty()) {
                    close(conn_fd);
                    fd_list.erase(fd_list.begin() + i);
                    continue;
                }

                if (on_accept_(peer_name, request, response))
                    response["error"] = "reject_connection";

                if (writeAttributes(conn_fd, response)) {
                    PLOG(ERROR) << "Failed to write response attributes";
                    close(conn_fd);
                    fd_list.erase(fd_list.begin() + i);
                    continue;
                }

                peer_name_map[conn_fd] = peer_name;
            }
        }
    }

    for (size_t i = 1; i < fd_list.size(); ++i) close(fd_list[i].fd);
    close(fd_list[0].fd);
}

int SessionManager::shutdownListener() {
    if (listen_running_.exchange(false)) listen_thread_.join();
    return 0;
}

int SessionManager::connect(const std::string &address,
                            const Attributes &request, Attributes &response) {
    RWSpinlock::WriteGuard guard(session_map_lock_);
    int conn_fd = -1;
    if (session_map_.count(address)) {
        auto &session = session_map_[address];
        conn_fd = session.fd;
    } else {
        conn_fd = makeConnect(address);
        if (conn_fd < 0) {
            PLOG(ERROR) << "Failed to make connection";
            return -1;
        }
        session_map_[address].fd = conn_fd;
    }
    return sendRPC(conn_fd, request, response);
}

int SessionManager::makeConnect(const std::string &address) {
    struct addrinfo hints;
    struct addrinfo *result, *addr;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string hostname;
    uint16_t port;
    if (parseHostPort(address, hostname, port)) {
        PLOG(ERROR) << "Illegal address format";
        return -1;
    }

    char service[16];
    sprintf(service, "%u", port);
    if (getaddrinfo(hostname.c_str(), service, &hints, &result)) {
        PLOG(ERROR)
            << "Failed to get IP address of peer server " << hostname
            << ", check DNS and /etc/hosts, or use IPv4 address instead";
        return -1;
    }

    for (addr = result; addr; addr = addr->ai_next) {
        int on = 1;
        int conn_fd =
            socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (conn_fd == -1) {
            PLOG(ERROR) << "Failed to create socket";
            continue;
        }

        if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "Failed to set address reusable";
            continue;
        }

        struct timeval timeout;
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            PLOG(ERROR) << "Failed to set socket timeout";
            continue;
        }

        if (::connect(conn_fd, addr->ai_addr, addr->ai_addrlen)) {
            PLOG(ERROR) << "Failed to connect";
            continue;
        }

        freeaddrinfo(result);
        return conn_fd;
    }

    freeaddrinfo(result);
    return -1;
}

int SessionManager::disconnect(const std::string &address) {
    RWSpinlock::WriteGuard guard(session_map_lock_);
    if (!session_map_.count(address)) return -1;
    auto &session = session_map_[address];
    close(session.fd);
    session_map_.erase(address);
    return 0;
}

bool SessionManager::hasConnection(const std::string &address) {
    RWSpinlock::ReadGuard guard(session_map_lock_);
    return session_map_.count(address);
}

int SessionManager::sendRPC(int fd, const Attributes &request,
                            Attributes &response) {
    if (writeAttributes(fd, request)) {
        PLOG(ERROR) << "Failed to write request attributes";
        return -1;
    }
    if (readAttributes(fd, response)) {
        PLOG(ERROR) << "Failed to read response attributes";
        return -1;
    }
    if (response.count("error")) {
        PLOG(ERROR) << "Connection rejected by peer: " << response.at("error");
        return -1;
    }
    return 0;
}

int SessionManager::readAttributes(int fd, Attributes &attr) {
    Json::CharReaderBuilder reader;
    Json::Value json_object;
    std::string json_string, errs;

    json_string = readString(fd);
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

int SessionManager::writeAttributes(int fd, const Attributes &attr) {
    Json::Value json_object;
    for (const auto &pair : attr) json_object[pair.first] = pair.second;
    Json::StreamWriterBuilder writer;
    std::string json_string = Json::writeString(writer, json_object);
    return writeString(fd, json_string);
}
}  // namespace rapid
