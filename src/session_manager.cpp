// session_manager.cpp
// Copyright (C) 2024 Feng Ren

#include "session_manager.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <json/json.h>
#include <set>
#include <sys/socket.h>

#define ERR_SOCKET (-1)

namespace rapid
{
    static inline ssize_t writeFully(int fd, const void *buf, size_t len)
    {
        char *pos = (char *)buf;
        size_t nbytes = len;
        while (nbytes)
        {
            ssize_t rc = write(fd, pos, nbytes);
            if (rc < 0 && (errno == EAGAIN || errno == EINTR))
                continue;
            else if (rc < 0)
            {
                PLOG(ERROR) << "Socket write failed";
                return rc;
            }
            else if (rc == 0)
            {
                LOG(WARNING) << "Socket write incompleted: expected " << len
                             << " bytes, actual " << len - nbytes << " bytes";
                return len - nbytes;
            }
            pos += rc;
            nbytes -= rc;
        }
        return len;
    }

    static inline ssize_t readFully(int fd, void *buf, size_t len)
    {
        char *pos = (char *)buf;
        size_t nbytes = len;
        while (nbytes)
        {
            ssize_t rc = read(fd, pos, nbytes);
            if (rc < 0 && (errno == EAGAIN || errno == EINTR))
                continue;
            else if (rc < 0)
            {
                PLOG(ERROR) << "Socket read failed";
                return rc;
            }
            else if (rc == 0)
            {
                LOG(WARNING) << "Socket read incompleted: expected " << len
                             << " bytes, actual " << len - nbytes << " bytes";
                return len - nbytes;
            }
            pos += rc;
            nbytes -= rc;
        }
        return len;
    }

    static inline int writeString(int fd, const std::string &str)
    {
        uint64_t length = str.size();
        if (writeFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
            return ERR_SOCKET;
        if (writeFully(fd, str.data(), length) != (ssize_t)length)
            return ERR_SOCKET;
        return 0;
    }

    static inline std::string readString(int fd)
    {
        const static size_t kMaxLength = 1ull << 20;
        uint64_t length = 0;
        if (readFully(fd, &length, sizeof(length)) != (ssize_t)sizeof(length))
            return "";
        if (length > kMaxLength)
            return "";
        std::string str;
        std::vector<char> buffer(length);
        if (readFully(fd, buffer.data(), length) != (ssize_t)length)
            return "";

        str.assign(buffer.data(), length);
        return str;
    }

    int SessionManager::start(uint16_t port, const OnAcceptCallback &on_accept)
    {
        sockaddr_in bind_address;
        int on = 1, listen_fd = -1;
        memset(&bind_address, 0, sizeof(sockaddr_in));
        bind_address.sin_family = AF_INET;
        bind_address.sin_port = htons(port);
        bind_address.sin_addr.s_addr = INADDR_ANY;

        listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0)
        {
            PLOG(ERROR) << "Failed to create socket";
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        if (setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)))
        {
            PLOG(ERROR) << "Failed to set socket timeout";
            close(listen_fd);
            return ERR_SOCKET;
        }

        if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)))
        {
            PLOG(ERROR) << "Failed to set address reusable";
            close(listen_fd);
            return ERR_SOCKET;
        }

        if (bind(listen_fd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) < 0)
        {
            PLOG(ERROR) << "Failed to bind address";
            close(listen_fd);
            return ERR_SOCKET;
        }

        if (listen(listen_fd, 5))
        {
            PLOG(ERROR) << "Failed to listen";
            close(listen_fd);
            return ERR_SOCKET;
        }

        listen_fd_ = listen_fd;
        on_accept_ = on_accept;
        listen_running_.exchange(true);
        listen_thread_ = std::thread(&SessionManager::listener, this);
        return 0;
    }

    void SessionManager::listener()
    {
        while (listen_running_)
        {
            sockaddr_in addr;
            socklen_t addr_len = sizeof(sockaddr_in);
            int conn_fd = accept(listen_fd_, (sockaddr *)&addr, &addr_len);
            if (conn_fd < 0)
            {
                if (errno != EWOULDBLOCK)
                    PLOG(ERROR) << "Failed to accept socket connection";
                continue;
            }

            if (addr.sin_family != AF_INET && addr.sin_family != AF_INET6)
            {
                LOG(ERROR) << "Unsupported socket type, should be AF_INET or AF_INET6";
                close(conn_fd);
                continue;
            }

            struct timeval timeout;
            timeout.tv_sec = 60;
            timeout.tv_usec = 0;
            if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)))
            {
                PLOG(ERROR) << "Failed to set socket timeout";
                close(conn_fd);
                continue;
            }

            Attributes request, response;
            if (readAttributes(conn_fd, request))
            {
                PLOG(ERROR) << "Failed to read request attributes";
                close(conn_fd);
                continue;
            }

            on_accept_(request, response);

            if (writeAttributes(conn_fd, response))
            {
                PLOG(ERROR) << "Failed to write response attributes";
                close(conn_fd);
                continue;
            }

            close(conn_fd);
        }
    }

    int SessionManager::shutdown()
    {
        if (listen_running_.exchange(false))
        {
            listen_thread_.join();
            close(listen_fd_);
            listen_fd_ = -1;
        }
        return -1;
    }

    int SessionManager::connect(const std::string &hostname,
                                uint16_t rpc_port,
                                const Attributes &request,
                                Attributes &response)
    {
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        char service[16];
        sprintf(service, "%u", rpc_port);
        if (getaddrinfo(hostname.c_str(), service, &hints, &result))
        {
            PLOG(ERROR) << "Failed to get IP address of peer server " << hostname
                        << ", check DNS and /etc/hosts, or use IPv4 address instead";
            return ERR_SOCKET;
        }

        int ret = 0;
        for (rp = result; rp; rp = rp->ai_next)
        {
            ret = postRequest(rp, request, response);
            if (ret == 0)
            {
                freeaddrinfo(result);
                return 0;
            }
        }

        freeaddrinfo(result);
        return ret;
    }

    int SessionManager::postRequest(struct addrinfo *addr,
                                    const Attributes &request,
                                    Attributes &response)
    {
        int on = 1;
        int conn_fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (conn_fd == -1)
        {
            PLOG(ERROR) << "Failed to create socket";
            return ERR_SOCKET;
        }
        if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)))
        {
            PLOG(ERROR) << "Failed to set address reusable";
            close(conn_fd);
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)))
        {
            PLOG(ERROR) << "Failed to set socket timeout";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (::connect(conn_fd, addr->ai_addr, addr->ai_addrlen))
        {
            PLOG(ERROR) << "Failed to connect";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (writeAttributes(conn_fd, request))
        {
            PLOG(ERROR) << "Failed to write request attributes";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (readAttributes(conn_fd, response))
        {
            PLOG(ERROR) << "Failed to read response attributes";
            close(conn_fd);
            return ERR_SOCKET;
        }

        close(conn_fd);
        return 0;
    }

    int SessionManager::readAttributes(int fd, Attributes &attr)
    {
        Json::CharReaderBuilder reader;
        Json::Value json_object;
        std::string json_string, errs;

        json_string = readString(fd);
        LOG(INFO) << json_string;
        std::istringstream iss(json_string);

        if (!Json::parseFromStream(reader, iss, &json_object, &errs))
        {
            LOG(ERROR) << "Failed to parse: " << errs;
            return ERR_SOCKET;
        }

        for (const auto &key : json_object.getMemberNames())
            attr[key] = json_object[key].asString();

        return 0;
    }

    int SessionManager::writeAttributes(int fd, const Attributes &attr)
    {
        Json::Value json_object;
        for (const auto &pair : attr)
            json_object[pair.first] = pair.second;
        Json::StreamWriterBuilder writer;
        std::string json_string = Json::writeString(writer, json_object);
        return writeString(fd, json_string);
    }
}
