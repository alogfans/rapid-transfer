// Copyright 2024 Feng Ren

#include "packet_manager.h"

#include <glog/logging.h>

namespace rapid {

const static size_t kGRHSize = sizeof(ibv_grh);

PacketHandle::PacketHandle() : packet_buf(nullptr) {}

int PacketHandle::setRawPacket(void *packet_buf, bool with_grh) {
    auto &handle = *this;
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

int PacketHandle::setPayload(void *data, size_t length, bool do_copy) {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "unable to set data: packet buf not specified";
        return -1;
    }

    if (!data && length) {
        LOG(ERROR) << "invalid argument: data is nullptr";
        return -1;
    }

    if (do_copy) {
        auto pkt_data = (uint8_t *)handle.packet_buf + sizeof(PktHdr) +
                        (with_grh ? kGRHSize : 0);
        memmove(pkt_data, data, length);
        handle.data_buf = nullptr;
    } else
        handle.data_buf = data;
    handle.data_len = length;
    return 0;
}

void *PacketHandle::getPayload() {
    auto &handle = *this;
    if (!handle.packet_buf || !handle.data_len) return nullptr;
    if (handle.data_buf) return handle.data_buf;
    return (char *)handle.packet_buf + sizeof(PktHdr) +
           (with_grh ? kGRHSize : 0);
}

int PacketHandle::deserialize(uint32_t imm_data, uint32_t packet_length) {
    if (with_grh) {
        if (packet_length < sizeof(PktHdr) + kGRHSize) {
            LOG(ERROR) << "packet length must be larger than header & GRH size";
            return -1;
        }
        pkt_hdr_imm.raw = imm_data;
        data_len = packet_length - sizeof(PktHdr) - kGRHSize;
    } else {
        if (packet_length < sizeof(PktHdr)) {
            LOG(ERROR) << "packet length must be larger than header size";
            return -1;
        }
        pkt_hdr_imm.raw = imm_data;
        data_len = packet_length - sizeof(PktHdr);
    }
    return decode();
}

int PacketHandle::serialize(std::vector<Buffer> &slices, uint32_t &imm_data) {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "unable to get stream: packet buf not specified";
        return -1;
    }
    if (with_grh) {
        LOG(ERROR) << "refuse to send packet with GRH field";
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
        LOG(ERROR) << "unable to encode: packet buf not specified";
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
        // hdr->hdr_imm.raw = pkt_hdr.raw;
        hdr->wnd = htole16(handle.wnd);
        hdr->ts_lo = htole16(uint16_t(handle.ts & 0xffff));
        hdr->ts_hi = htole32(uint32_t((handle.ts >> 16) & 0xffffffff));
    }
    return 0;
}

int PacketHandle::decode() {
    auto &handle = *this;
    if (!handle.packet_buf) {
        LOG(ERROR) << "unable to decode: packet buf not specified";
        return -1;
    }
    PktHdrImm pkt_hdr = handle.pkt_hdr_imm;
    if (!pkt_hdr.compacted) {
        PktHdr *hdr =
            (PktHdr *)((char *)handle.packet_buf + (with_grh ? kGRHSize : 0));
        handle.wnd = le16toh(hdr->wnd);
        handle.ts = (uint64_t(le32toh(hdr->ts_hi)) << 16) | le16toh(hdr->ts_lo);
        // pkt_hdr.raw = hdr->hdr_imm.raw;
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
        LOG(ERROR) << "out of memory";
        return -1;
    }
    uintptr_t next = *(uintptr_t *)packet_buf;
    global_free_buffer_ = (void *)next;
    return handle.setRawPacket(packet_buf, with_grh);
}

int PacketBufferPool::freePacket(PacketHandle &handle) {
    RWSpinlock::WriteGuard guard(arena_lock_);
    auto packet_buf = handle.packet_buf;
    if (!packet_buf) {
        LOG(ERROR) << "invalid packet handle";
        return -1;
    }
    *(uintptr_t *)packet_buf = (uintptr_t)global_free_buffer_;
    global_free_buffer_ = packet_buf;
    return 0;
}

SendQueue::SendQueue(size_t mtu_size, size_t queue_capacity, size_t wnd_size,
                     PacketBufferPool &pool, uint8_t session)
    : mtu_size_(mtu_size),
      queue_capacity_(queue_capacity),
      session_(session),
      head_(0),
      tail_(0),
      wnd_size_(wnd_size),
      pool_(pool),
      secondary_queue_(mtu_size - sizeof(PktHdr) - kGRHSize) {
    assert(wnd_size <= queue_capacity);
    handle_.resize(queue_capacity);
}

SendQueue::~SendQueue() {
    for (auto &entry : handle_) {
        if (entry.getPayload()) {
            pool_.freePacket(entry);
        }
    }
    handle_.clear();
}

int SendQueue::push(const std::vector<Buffer> &slice_list, uint32_t &last_sn) {
    // RWSpinlock::WriteGuard guard(queue_lock_);
    auto fragment_id = secondary_queue_.push(slice_list);
    last_sn = SHORT_SN(fragment_id.second);
    return fillPrimaryQueue();
}

int SendQueue::markCompleted(uint32_t ack_sn) {
    // RWSpinlock::WriteGuard guard(queue_lock_);
    // case 1: XXX tail_ ... ack_sn ... head_ XXX
    // case 2: ... ack_sn ... head  XXX tail ...
    auto tail = SHORT_SN(tail_), head = SHORT_SN(head_);
    if (tail <= head && tail <= ack_sn && ack_sn <= head)
        tail_ = tail_ + (ack_sn - tail);
    else if (tail > head && ack_sn <= head)
        tail_ = head_ - (head - ack_sn);
    else if (tail > head && ack_sn >= tail)
        tail_ = tail_ + (ack_sn - tail);
    return fillPrimaryQueue();
}

int SendQueue::fillPrimaryQueue() {
    // including wrap-ups
    while (secondary_queue_.hasRemainingFragment() &&
           head_ - tail_ <= wnd_size_) {
        auto slice = secondary_queue_.popFragment();
        auto &handle = handle_[head_ % queue_capacity_];
        if (!handle.getRawPacket() && pool_.allocatePacket(handle)) return -1;
        handle.session = session_;
        handle.cmd = PKT_CMD_DATA;
        handle.wnd = wnd_size_;
        handle.sn = SHORT_SN(head_);
        handle.ts = 0;
        if (handle.setPayload(slice.addr, slice.length, false)) return -1;
        handle.inflight = true;
        head_++;
    }
    return 0;
}

int SendQueue::getIndexRange(uint64_t &head, uint64_t &tail) {
    // RWSpinlock::ReadGuard guard(queue_lock_);
    head = head_;
    tail = tail_;
    return 0;
}

int SendQueue::forEach(std::function<int(PacketHandle &)> func) {
    // RWSpinlock::ReadGuard guard(queue_lock_);
    for (auto curr = tail_.load(); curr != head_.load(); curr++) {
        auto &handle = handle_[curr % queue_capacity_];
        func(handle);
    }
    return 0;
}

McastSendQueue::McastSendQueue(size_t mtu_size, size_t queue_capacity,
                               size_t wnd_size, PacketBufferPool &pool,
                               uint8_t session, size_t replica_num)
    : mtu_size_(mtu_size),
      queue_capacity_(queue_capacity),
      session_(session),
      replica_num_(replica_num),
      head_(0),
      wnd_size_(wnd_size),
      pool_(pool),
      secondary_queue_(mtu_size - sizeof(PktHdr) - kGRHSize) {
    assert(wnd_size <= queue_capacity);
    handle_.resize(queue_capacity);
    tail_list_.resize(replica_num, 0);
}

McastSendQueue::~McastSendQueue() {
    for (auto &entry : handle_) {
        if (entry.getPayload()) {
            pool_.freePacket(entry);
        }
    }
    handle_.clear();
}

int McastSendQueue::push(const std::vector<Buffer> &slice_list,
                         uint32_t &last_sn) {
    // RWSpinlock::WriteGuard guard(queue_lock_);
    auto fragment_id = secondary_queue_.push(slice_list);
    last_sn = SHORT_SN(fragment_id.second);
    return fillPrimaryQueue();
}

int McastSendQueue::markCompleted(int index, uint32_t ack_sn) {
    // RWSpinlock::WriteGuard guard(queue_lock_);
    // case 1: XXX tail_ ... ack_sn ... head_ XXX
    // case 2: ... ack_sn ... head  XXX tail ...
    if (index < 0 || index >= (int)tail_list_.size()) {
        LOG(ERROR) << "invalid argument";
        return -1;
    }
    auto &tail_ = tail_list_[index];
    auto tail = SHORT_SN(tail_), head = SHORT_SN(head_);
    if (tail <= head && tail <= ack_sn && ack_sn <= head)
        tail_ = tail_ + (ack_sn - tail);
    else if (tail > head && ack_sn <= head)
        tail_ = head_ - (head - ack_sn);
    else if (tail > head && ack_sn >= tail)
        tail_ = tail_ + (ack_sn - tail);
    return fillPrimaryQueue();
}

uint32_t McastSendQueue::getAckSN(int index) const {
    if (index < 0 || index >= (int)tail_list_.size())
        return SHORT_SN(getMinTailIndex());
    return SHORT_SN(tail_list_[index]);
}

int McastSendQueue::fillPrimaryQueue() {
    // including wrap-ups
    while (secondary_queue_.hasRemainingFragment() &&
           head_ - getMinTailIndex() <= wnd_size_) {
        auto slice = secondary_queue_.popFragment();
        auto &handle = handle_[head_ % queue_capacity_];
        if (!handle.getRawPacket() && pool_.allocatePacket(handle)) return -1;
        handle.session = session_;
        handle.cmd = PKT_CMD_DATA;
        handle.wnd = wnd_size_;
        handle.sn = SHORT_SN(head_);
        handle.ts = 0;
        if (handle.setPayload(slice.addr, slice.length, false)) return -1;
        handle.inflight = true;
        head_++;
    }
    return 0;
}

int McastSendQueue::getIndexRange(uint64_t &head, uint64_t &tail) {
    // RWSpinlock::ReadGuard guard(queue_lock_);
    head = head_;
    tail = getMinTailIndex();
    return 0;
}

int McastSendQueue::forEach(std::function<int(PacketHandle &)> func) {
    // RWSpinlock::ReadGuard guard(queue_lock_);
    for (auto curr = getMinTailIndex(); curr != head_; curr++) {
        auto &handle = handle_[curr % queue_capacity_];
        func(handle);
    }
    return 0;
}

uint64_t McastSendQueue::getMinTailIndex() const {
    uint64_t min_tail = UINT64_MAX;
    for (auto &entry : tail_list_) {
        min_tail = std::min(min_tail, entry);
    }
    return min_tail;
}

ReceiveQueue::ReceiveQueue(size_t mtu_size, size_t queue_capacity,
                           size_t wnd_size, uint8_t session)
    : mtu_size_(mtu_size),
      queue_capacity_(queue_capacity),
      session_(session),
      head_(0),
      tail_(0),
      wnd_size_(wnd_size),
      secondary_queue_(mtu_size - sizeof(PktHdr) - kGRHSize),
      last_packet_ts_(0) {
    assert(wnd_size <= queue_capacity);
    requests_.resize(queue_capacity);
}

int ReceiveQueue::push(const std::vector<Buffer> &slice_list,
                       uint32_t &last_sn) {
    // RWSpinlock::WriteGuard guard(queue_lock_);
    auto fragment_id = secondary_queue_.push(slice_list);
    last_sn = SHORT_SN(fragment_id.second);
    return fillPrimaryQueue();
}

int ReceiveQueue::markCompleted(PacketHandle &handle) {
    // RWSpinlock::WriteGuard guard(queue_lock_);
    auto wnd_start = SHORT_SN(tail_);
    auto wnd_end = SHORT_SN(tail_ + wnd_size_);
    if (wnd_start <= wnd_end) {
        if (handle.sn < wnd_start || handle.sn >= wnd_end) return 0;
    } else {
        if (handle.sn < wnd_start && handle.sn >= wnd_end) return 0;
    }
    auto &request = requests_[handle.sn % queue_capacity_];
    if (request.inflight) {
        if (handle.getPayloadLength() != request.length) {
            LOG(ERROR) << "data length mismatched, packet "
                       << handle.getPayloadLength() << ", request "
                       << request.length;
            abort();
        } else
            memmove(request.addr, handle.getPayload(), request.length);
        last_packet_ts_ = std::max(last_packet_ts_, handle.ts);
        request.inflight = false;
    }
    while (tail_ != head_) {
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
    // RWSpinlock::ReadGuard guard(queue_lock_);
    head = head_;
    tail = tail_;
    return 0;
}

PacketManager::PacketManager(size_t mtu_size, size_t max_packets,
                             size_t queue_capacity, size_t wnd_size)
    : mtu_size_(mtu_size),
      max_packets_(max_packets),
      queue_capacity_(queue_capacity),
      wnd_size_(wnd_size),
      pool_(mtu_size, max_packets) {}

PacketManager::~PacketManager() { deconstruct(); }

int PacketManager::construct() { return pool_.construct(); }

int PacketManager::deconstruct() {
    for (auto &entry : send_queue_) delete entry.second;
    send_queue_.clear();
    for (auto &entry : receive_queue_) delete entry.second;
    receive_queue_.clear();
    return pool_.deconstruct();
}

SendQueue &PacketManager::getSendQueue(int sid) {
    queue_lock_.lockShared();
    if (send_queue_.count(sid)) {
        auto &entry = send_queue_[sid];
        queue_lock_.unlockShared();
        return *entry;
    }
    queue_lock_.unlockShared();
    queue_lock_.lock();
    if (!send_queue_.count(sid)) {
        auto entry = new SendQueue(mtu_size_, queue_capacity_, wnd_size_, pool_,
                                   sid % 256);
        send_queue_[sid] = entry;
    }
    auto &entry = send_queue_[sid];
    queue_lock_.unlock();
    return *entry;
}

ReceiveQueue &PacketManager::getReceiveQueue(int sid) {
    queue_lock_.lockShared();
    if (receive_queue_.count(sid)) {
        auto &entry = receive_queue_[sid];
        queue_lock_.unlockShared();
        return *entry;
    }
    queue_lock_.unlockShared();
    queue_lock_.lock();
    if (!receive_queue_.count(sid)) {
        auto entry =
            new ReceiveQueue(mtu_size_, queue_capacity_, wnd_size_, sid % 256);
        receive_queue_[sid] = entry;
    }
    auto &entry = receive_queue_[sid];
    queue_lock_.unlock();
    return *entry;
}

McastSendQueue &PacketManager::getMcastSendQueue(int sid) {
    queue_lock_.lockShared();
    if (mcast_send_queue_.count(sid)) {
        auto &entry = mcast_send_queue_[sid];
        queue_lock_.unlockShared();
        return *entry;
    }
    queue_lock_.unlockShared();
    queue_lock_.lock();
    if (!mcast_send_queue_.count(sid)) {
        auto entry =
            new McastSendQueue(mtu_size_, queue_capacity_, wnd_size_, pool_,
                               sid % 256, mcast_replica_num_[sid / 256]);
        mcast_send_queue_[sid] = entry;
    }
    auto &entry = mcast_send_queue_[sid];
    queue_lock_.unlock();
    return *entry;
}

int PacketManager::setMulticastReplicaNum(int group_id, size_t replica_num) {
    queue_lock_.lock();
    if (mcast_replica_num_.count(group_id)) {
        queue_lock_.unlock();
        return -1;
    }
    mcast_replica_num_[group_id] = replica_num;
    queue_lock_.unlock();
    return 0;
}

}  // namespace rapid
