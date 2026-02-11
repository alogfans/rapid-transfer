// rapid_transfer_engine.cpp
//
// RapidTransfer v1 Implementation
// Single-rail instance per class, multi-rail orchestration by upper layer
// Uses existing UD protocol (RdmaUnreliableProtocol Context)
//
// Copyright (C) 2024 Feng Ren

#include <glog/logging.h>

#include "impl.h"
#include "rapid_transfer.h"

namespace rapid {
namespace v1 {

// ============================================================================
// RapidTransfer v1 Implementation using UD Protocol
// ============================================================================

std::shared_ptr<RapidTransfer> RapidTransfer::Create(
    const RailConfig& rail_config, const std::string& listen_address) {
    auto engine = std::shared_ptr<RapidTransfer>(new RapidTransfer());
    engine->impl_ = std::make_unique<Impl>();

    int ret = engine->impl_->initialize(rail_config, listen_address);
    if (ret != 0) {
        LOG(ERROR) << "Failed to initialize RapidTransfer: " << ret;
        return nullptr;
    }

    return engine;
}

RapidTransfer::~RapidTransfer() = default;

TaskID RapidTransfer::write(const std::string& peer_name,
                            const std::vector<Buffer>& local_buffers,
                            const std::vector<Buffer>& remote_buffers,
                            const std::string& notify_message) {
    return impl_->write(peer_name, local_buffers, remote_buffers,
                        notify_message);
}

TaskID RapidTransfer::read(const std::string& peer_name,
                           const std::vector<Buffer>& local_buffers,
                           const std::vector<Buffer>& remote_buffers,
                           const std::string& notify_message) {
    return impl_->read(peer_name, local_buffers, remote_buffers,
                       notify_message);
}

void RapidTransfer::setNotificationCallback(NotificationCallback callback) {
    impl_->setNotificationCallback(std::move(callback));
}

int RapidTransfer::notify(const std::string& peer_name, TaskID task_id,
                          const std::string& message) {
    return impl_->notify(peer_name, task_id, message);
}

Status RapidTransfer::getStatus(TaskID task_id, size_t* transferred_bytes) {
    return impl_->getStatus(task_id, transferred_bytes);
}

Status RapidTransfer::wait(TaskID task_id, std::chrono::milliseconds timeout) {
    return impl_->wait(task_id, timeout);
}

std::optional<TransferResult> RapidTransfer::getTransferResult(TaskID task_id) {
    return impl_->getTransferResult(task_id);
}

int RapidTransfer::freeTask(TaskID task_id) { return impl_->freeTask(task_id); }

RailConfig RapidTransfer::getRailConfig() const {
    return impl_->getRailConfig();
}

RailStats RapidTransfer::getRailStats() const { return impl_->getRailStats(); }

RailState RapidTransfer::getRailState() const { return impl_->getRailState(); }

int RapidTransfer::registerLocalMemory(void* addr, size_t length) {
    return impl_->registerLocalMemory(addr, length);
}

int RapidTransfer::unregisterLocalMemory(void* addr) {
    return impl_->unregisterLocalMemory(addr);
}

void RapidTransfer::setBufferInfo(const RapidTransfer::BufferInfo& info) {
    impl_->setBufferInfo(info);
}

std::optional<RapidTransfer::BufferInfo> RapidTransfer::getRemoteBufferInfo(const std::string& peer_address) {
    return impl_->getRemoteBufferInfo(peer_address);
}

int RapidTransfer::runStep() { return impl_->runStep(); }

int RapidTransfer::shutdown() { return impl_->shutdown(); }

}  // namespace v1
}  // namespace rapid
