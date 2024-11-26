// Copyright 2024 Feng Ren

#include "packet_manager.h"

#include <glog/logging.h>

namespace rapid {
PacketHandle::PacketHandle() : packet_buf(nullptr) {}

int PacketHandle::open(void *packet_buf, bool with_grh) {
    auto &handle = *this;
    if (handle.packet_buf) {
        LOG(ERROR) << "Unable to open handle: packet buf already specified";
        return -1;
    }
    handle.packet_buf = packet_buf;
    handle.session = 0;
    handle.cmd = 0;
    handle.wnd = 0;
    handle.sn = 0;
    handle.ts = 0;
    handle.compacted = false;
    handle.inflight = false;
    handle.data_buf = nullptr;
    handle.data_len = 0;
    handle.pkt_hdr_imm.raw = 0;
    handle.with_grh = with_grh;
    return 0;
}

int PacketHandle::close() {
    auto &handle = *this;
    handle.packet_buf = nullptr;
    handle.data_buf = nullptr;
    handle.data_len = 0;
    return 0;
}

int PacketHandle::setData(void *data, size_t length, bool do_copy) {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "Unable to set data: packet buf not specified";
        return -1;
    }

    if (!data && length) {
        LOG(ERROR) << "Invalid argument: data is nullptr";
        return -1;
    }

    if (do_copy) {
        auto pkt_data = (uint8_t *)handle.packet_buf + sizeof(PktHdr) +
                        (with_grh ? sizeof(ibv_grh) : 0);
        memmove(pkt_data, data, length);
        handle.data_buf = nullptr;
    } else
        handle.data_buf = data;
    handle.data_len = length;
    return 0;
}

void *PacketHandle::getData() {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "Unable to get data: packet buf not specified";
        return nullptr;
    }
    if (!handle.data_len) return nullptr;
    if (handle.data_buf) return handle.data_buf;
    return (char *)handle.packet_buf + sizeof(PktHdr) +
           (with_grh ? sizeof(ibv_grh) : 0);
}

int PacketHandle::fromBuffer(uint32_t imm_data, uint32_t packet_length) {
    if (packet_length < sizeof(PktHdr)) {
        LOG(ERROR) << "packet_length must be larger than header size";
        return -1;
    }
    pkt_hdr_imm.raw = imm_data;
    data_len = packet_length - sizeof(PktHdr);
    return decode();
}

int PacketHandle::toBuffers(std::vector<Buffer> &slices, uint32_t &imm_data) {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "Unable to get stream: packet buf not specified";
        return -1;
    }
    if (with_grh) {
        LOG(ERROR) << "Refuse to send packet with GRH field";
        return -1;
    }
    if (encode()) return -1;
    imm_data = handle.pkt_hdr_imm.raw;
    slices.clear();
    if (!handle.data_buf) {
        // 1. no attach data
        // 2a. 2a. has attach data, w/o zero copy
        slices.push_back(Buffer{.addr = handle.packet_buf,
                                .length = sizeof(PktHdr) + handle.data_len});
    } else {
        // 2b. has attach data, w/ zero copy
        slices.push_back(
            Buffer{.addr = handle.packet_buf, .length = sizeof(PktHdr)});
        slices.push_back(
            Buffer{.addr = handle.data_buf, .length = handle.data_len});
    }
    return 0;
}

int PacketHandle::encode() {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "Unable to encode: packet buf not specified";
        return -1;
    }
    PktHdrImm pkt_hdr;
    pkt_hdr.session = handle.session & ((1 << 6) - 1);
    pkt_hdr.cmd = handle.cmd & 0x1;
    pkt_hdr.compacted = handle.compacted ? 1 : 0;
    pkt_hdr.sn[0] = uint8_t(handle.sn & 0xff);
    pkt_hdr.sn[1] = uint8_t((handle.sn >> 8) & 0xff);
    pkt_hdr.sn[2] = uint8_t((handle.sn >> 16) & 0xff);
    handle.pkt_hdr_imm.raw = pkt_hdr.raw;
    if (!pkt_hdr.compacted) {
        PktHdr *hdr = (PktHdr *)handle.packet_buf;
        hdr->hdr_imm.raw = pkt_hdr.raw;
        hdr->wnd = htole16(handle.wnd);
        hdr->ts_lo = htole16(uint16_t(handle.ts & 0xffff));
        hdr->ts_hi = htole32(uint32_t((handle.ts >> 16) & 0xffffffff));
    }
    return 0;
}

int PacketHandle::decode() {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "Unable to decode: packet buf not specified";
        return -1;
    }
    PktHdrImm pkt_hdr = handle.pkt_hdr_imm;
    if (!pkt_hdr.compacted) {
        PktHdr *hdr = (PktHdr *)((char *)handle.packet_buf +
                                 (with_grh ? sizeof(ibv_grh) : 0));
        handle.wnd = le16toh(hdr->wnd);
        handle.ts = uint64_t(le32toh(hdr->ts_hi) << 16) | le16toh(hdr->ts_lo);
        pkt_hdr.raw = hdr->hdr_imm.raw;
    }
    handle.session = pkt_hdr.session;
    handle.cmd = pkt_hdr.cmd;
    handle.compacted = pkt_hdr.compacted ? true : false;
    handle.sn = pkt_hdr.sn[0] | (uint32_t(pkt_hdr.sn[1]) << 8) |
                (uint32_t(pkt_hdr.sn[2]) << 16);
    handle.data_buf = nullptr;
    return 0;
}

PacketBufferPool::PacketBufferPool(size_t mtu_size, size_t max_packets)
    : mtu_size_(mtu_size),
      max_packets_(max_packets),
      arena_(nullptr),
      global_free_buffer_(nullptr) {}

PacketBufferPool::~PacketBufferPool() { deconstruct(); }

int PacketBufferPool::construct() {
    int ret = posix_memalign(&arena_, 4096, mtu_size_ * max_packets_);
    if (ret) {
        PLOG(ERROR) << "posix_memalign failed";
        return ret;
    }

    for (size_t index = 0; index < max_packets_; ++index) {
        uint8_t *ptr = (uint8_t *)arena_ + mtu_size_ * index;
        *(uintptr_t *)ptr = (uintptr_t)global_free_buffer_;
        global_free_buffer_ = ptr;
    }

    return 0;
}

int PacketBufferPool::deconstruct() {
    if (arena_) {
        free(arena_);
        arena_ = nullptr;
    }
    return 0;
}

int PacketBufferPool::allocatePacket(PacketHandle &handle, bool with_grh) {
    RWSpinlock::WriteGuard guard(arena_lock_);
    void *packet_buf = global_free_buffer_;
    if (!packet_buf) {
        LOG(ERROR) << "Out of memory";
        return -1;
    }
    uintptr_t next = *(uintptr_t *)packet_buf;
    global_free_buffer_ = (void *)next;
    return handle.open(packet_buf, with_grh);
}

int PacketBufferPool::freePacket(PacketHandle &handle) {
    RWSpinlock::WriteGuard guard(arena_lock_);
    auto packet_buf = handle.packet_buf;
    if (!packet_buf) {
        LOG(ERROR) << "Invalid packet handle";
        return -1;
    }
    *(uintptr_t *)packet_buf = (uintptr_t)global_free_buffer_;
    global_free_buffer_ = packet_buf;
    handle.close();
    return 0;
}

int PacketBufferPool::freePacketDirect(void *addr) {
    RWSpinlock::WriteGuard guard(arena_lock_);
    if (!addr || (uint8_t *)addr < (uint8_t *)arena_ ||
        (uint8_t *)addr >= (uint8_t *)arena_ + getCapacity()) {
        LOG(ERROR) << "Invalid packet handle";
        return -1;
    }
    auto packet_buf =
        (uint8_t *)arena_ +
        ((uint8_t *)addr - (uint8_t *)arena_) / mtu_size_ * mtu_size_;
    *(uintptr_t *)packet_buf = (uintptr_t)global_free_buffer_;
    global_free_buffer_ = packet_buf;
    return 0;
}

SendQueue::SendQueue(size_t mtu_size, size_t queue_capacity,
                     PacketBufferPool &pool)
    : mtu_size_(mtu_size),
      queue_capacity_(queue_capacity),
      session_(0),
      head_(0),
      tail_(0),
      wnd_size_(queue_capacity),
      pool_(pool),
      secondary_queue_(mtu_size - sizeof(PktHdr)) {
    handle_.resize(queue_capacity);
}

int SendQueue::push(const std::vector<Buffer> &slice_list, uint32_t &last_sn) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto fragment_id = secondary_queue_.push(slice_list);
    last_sn = SHORT_SN(fragment_id.second - 1);
    return fillPrimaryQueue();
}

int SendQueue::markCompleted(uint32_t sn) {
    std::lock_guard<std::mutex> lock(mutex_);
    handle_[sn % queue_capacity_].inflight = false;
    while (tail_ < head_) {
        auto &handle = handle_[tail_ % queue_capacity_];
        if (handle.inflight) break;
        pool_.freePacket(handle);
        tail_++;
    }
    return fillPrimaryQueue();
}

int SendQueue::fillPrimaryQueue() {
    while (secondary_queue_.hasRemainingFragment() &&
           head_ - tail_ <= wnd_size_) {
        auto slice = secondary_queue_.popFragment();
        auto sn = uint32_t(head_ & 0x00ffffff);
        auto &handle = handle_[head_ % queue_capacity_];
        if (pool_.allocatePacket(handle)) return -1;
        handle.session = session_;
        handle.cmd = PKT_CMD_DATA;
        handle.wnd = wnd_size_;
        handle.sn = sn;
        handle.ts = 0;
        if (handle.setData(slice.addr, slice.length, true)) return -1;
        handle.inflight = true;
        head_++;
    }
    return 0;
}

int SendQueue::getIndexRange(uint64_t &head, uint64_t &tail) {
    head = head_;
    tail = tail_;
    return 0;
}

int SendQueue::forEach(std::function<int(PacketHandle &)> func) {
    while (tail_ < head_) {
        auto &handle = handle_[tail_ % queue_capacity_];
        func(handle);
    }
    return 0;
}

ReceiveQueue::ReceiveQueue(size_t mtu_size, size_t queue_capacity)
    : mtu_size_(mtu_size),
      queue_capacity_(queue_capacity),
      head_(0),
      tail_(0),
      wnd_size_(queue_capacity),
      secondary_queue_(mtu_size - sizeof(PktHdr)) {
    requests_.resize(queue_capacity);
}

int ReceiveQueue::push(const std::vector<Buffer> &slice_list,
                       uint32_t &last_sn) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto fragment_id = secondary_queue_.push(slice_list);
    last_sn = SHORT_SN(fragment_id.second - 1);
    return fillPrimaryQueue();
}

int ReceiveQueue::markCompleted(PacketHandle &handle) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto &request = requests_[handle.sn % queue_capacity_];
    if (request.inflight) {
        if (handle.getDataLength() != request.length)
            LOG(ERROR) << "Mismatch data length";
        else
            memmove(request.addr, handle.getData(), request.length);
        request.inflight = false;
    }
    while (tail_ < head_) {
        auto &request = requests_[tail_ % queue_capacity_];
        if (request.inflight) break;
        tail_++;
    }
    return fillPrimaryQueue();
}

int ReceiveQueue::fillPrimaryQueue() {
    while (secondary_queue_.hasRemainingFragment() &&
           head_ - tail_ <= wnd_size_) {
        auto slice = secondary_queue_.popFragment();
        auto &request = requests_[head_ % queue_capacity_];
        request.addr = slice.addr;
        request.length = slice.length;
        request.inflight = true;
        head_++;
    }
    return 0;
}

int ReceiveQueue::getIndexRange(uint64_t &head, uint64_t &tail) {
    head = head_;
    tail = tail_;
    return 0;
}

PacketManager::PacketManager(size_t mtu_size, size_t max_packets,
                             size_t queue_capacity)
    : mtu_size_(mtu_size),
      max_packets_(max_packets),
      queue_capacity_(queue_capacity),
      pool_(mtu_size, max_packets) {}

PacketManager::~PacketManager() { deconstruct(); }

int PacketManager::construct() { return pool_.construct(); }

int PacketManager::deconstruct() {
    for (auto &entry : send_queue_) delete entry.second;
    for (auto &entry : receive_queue_) delete entry.second;
    return pool_.deconstruct();
}

SendQueue &PacketManager::getSendQueue(int id) {
    queue_lock_.lockShared();
    if (send_queue_.count(id)) {
        auto &entry = send_queue_[id];
        queue_lock_.unlockShared();
        return *entry;
    }
    queue_lock_.unlockShared();
    queue_lock_.lock();
    if (!send_queue_.count(id)) {
        auto entry = new SendQueue(mtu_size_, queue_capacity_, pool_);
        entry->setSession(id % 256);
        send_queue_[id] = entry;
    }
    auto &entry = send_queue_[id];
    queue_lock_.unlock();
    return *entry;
}

ReceiveQueue &PacketManager::getReceiveQueue(int id) {
    queue_lock_.lockShared();
    if (receive_queue_.count(id)) {
        auto &entry = receive_queue_[id];
        queue_lock_.unlockShared();
        return *entry;
    }
    queue_lock_.unlockShared();
    queue_lock_.lock();
    if (!receive_queue_.count(id)) {
        auto entry = new ReceiveQueue(mtu_size_, queue_capacity_);
        entry->setSession(id % 256);
        receive_queue_[id] = entry;
    }
    auto &entry = receive_queue_[id];
    queue_lock_.unlock();
    return *entry;
}

}  // namespace rapid
