// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "protocol.h"
#include "rapid_transfer.h"
#include "rdma_context.h"

namespace rapid {
struct Task;

const static int kMaxSgeCount = 2;

struct Request {
    void *addr[kMaxSgeCount] = {nullptr};
    size_t length[kMaxSgeCount] = {0};
    uint32_t lkey[kMaxSgeCount] = {0};
    uint32_t imm_data = 0;
    volatile int *qp_depth = nullptr;
    volatile Status status = UNKNOWN;
    void *context = nullptr;
};

struct RequestCache {
    RequestCache() : head_(0), tail_(0) {
        lazy_delete_requests_.resize(kLazyDeleteRequestCapacity);
    }

    ~RequestCache() {
        for (uint64_t i = tail_; i != head_; i++) {
            auto slice = lazy_delete_requests_[i % kLazyDeleteRequestCapacity];
            delete slice;
            freed_++;
        }
        // if (allocated_ != freed_) {
        //     LOG(WARNING) << "detected slice leak: allocated "
        //                  << allocated_ << " freed " << freed_;
        // }
    }

    Request *allocate() {
        if (head_ - tail_ == 0) {
            allocated_++;
            return new Request();
        }
        auto request = lazy_delete_requests_[tail_ % kLazyDeleteRequestCapacity];
        tail_++;
        new (request) Request();
        return request;
    }

    void deallocate(Request *request) {
        if (head_ - tail_ == kLazyDeleteRequestCapacity) {
            delete request;
            freed_++;
            return;
        }
        lazy_delete_requests_[head_ % kLazyDeleteRequestCapacity] = request;
        head_++;
    }

    const static size_t kLazyDeleteRequestCapacity = 4096;
    std::vector<Request *> lazy_delete_requests_;
    uint64_t head_, tail_;
    uint64_t allocated_ = 0, freed_ = 0;
};

struct RdmaEndPoint {
    RdmaEndPoint() {}

    virtual ~RdmaEndPoint() {}

    virtual bool connected() const = 0;

    virtual std::vector<uint32_t> qpNum() const = 0;

    virtual int setupConnection(const std::string &peer_gid, uint16_t peer_lid,
                                std::vector<uint32_t> peer_qp_num_list) = 0;

    virtual int postSendRequest(const std::vector<Request *> &request_list) = 0;

    virtual int postReceiveRequest(
        const std::vector<Request *> &request_list) = 0;
};
}  // namespace rapid

#endif  // RDMA_ENDPOINT_H
