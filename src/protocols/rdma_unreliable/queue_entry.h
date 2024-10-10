// Copyright 2024 Feng Ren

#ifndef QUEUE_ENTRY_H
#define QUEUE_ENTRY_H

#include <atomic>
#include <mutex>

#include "rapid_transfer.h"

namespace rapid
{
    struct QueueEntry
    {
        QueueEntry(size_t fragment_capacity = 4000)
            : offset_(0),
              fragment_id_(0),
              fragment_capacity_(fragment_capacity) {}

        bool hasRemainingFragment()
        {
            return !buffer_list_.empty();
        }

        Buffer popFragment()
        {
            if (buffer_list_.empty())
                return Buffer{nullptr, 0};
            auto &buffer = buffer_list_[0];
            auto addr = static_cast<char *>(buffer.addr) + offset_;
            auto length = std::min(fragment_capacity_, buffer.length - offset_);
            if (offset_ + length == buffer.length)
            {
                buffer_list_.erase(buffer_list_.begin());
                offset_ = 0;
            }
            else
                offset_ += length;
            return Buffer{addr, length};
        }

        std::pair<uint64_t, uint64_t> push(const std::vector<Buffer> &buffer_list)
        {
            auto start_fragment_id = fragment_id_;
            for (auto &entry : buffer_list)
            {
                buffer_list_.push_back(entry);
                fragment_id_ += (entry.length + fragment_capacity_ - 1) / fragment_capacity_;
            }
            auto end_fragment_id = fragment_id_;
            return {start_fragment_id, end_fragment_id};
        }

        uint64_t fragment_id() const
        {
            return fragment_id_;
        }

    private:
        std::vector<Buffer> buffer_list_;
        size_t offset_;
        uint64_t fragment_id_;
        const size_t fragment_capacity_;
    };
}

#endif // QUEUE_ENTRY_H
