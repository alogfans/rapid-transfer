// Copyright 2024 Feng Ren

#ifndef PACKET_POOL_H_
#define PACKET_POOL_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <endian.h>
#include <linux/types.h>

#include "packet.h"
#include "protocols/common/rdma_context.h"

namespace rapid
{
    class PacketPool
    {
    public:
        const static size_t kPacketStorageSize = 4096 + 64; // 4160
        const static size_t kPacketBufferCount = 65536;

        PacketPool(RdmaContext &context) : context_(context), num_free_(0) {}

        void setupPacketPool()
        {
            packet_buffer_ = malloc(kPacketStorageSize * kPacketBufferCount);
            assert(packet_buffer_);
            int ret = context_.registerMemoryRegion(packet_buffer_,
                                                    kPacketStorageSize * kPacketBufferCount,
                                                    IBV_ACCESS_LOCAL_WRITE);
            assert(!ret);
            next_free_packet_buffer_ = nullptr;
            for (size_t index = 0; index < kPacketBufferCount; ++index)
            {
                void *ptr = (char *)packet_buffer_ + kPacketStorageSize * index;
                *(uintptr_t *)ptr = (uintptr_t)next_free_packet_buffer_;
                next_free_packet_buffer_ = ptr;
                num_free_++;
            }
        }

        void destroyPacketPool()
        {
            context_.unregisterMemoryRegion(packet_buffer_);
            free(packet_buffer_);
        }

        PacketHeader *allocatePacket()
        {
            void *ptr = next_free_packet_buffer_;
            assert(ptr);
            uintptr_t next = *(uintptr_t *)ptr;
            next_free_packet_buffer_ = (void *)next;
            num_free_--;
            return (PacketHeader *)ptr;
        }

        void freePacket(void *ptr)
        {
            if (!ptr)
                return;
            uint64_t index = ((uint64_t)ptr - (uint64_t)packet_buffer_) / kPacketStorageSize;
            void *header = (char *)packet_buffer_ + index * kPacketStorageSize;
            *(uintptr_t *)header = (uintptr_t)next_free_packet_buffer_;
            next_free_packet_buffer_ = header;
            num_free_++;
        };

    private:
        RdmaContext &context_;
        void *packet_buffer_;
        void *next_free_packet_buffer_;
        int num_free_;
    };
}

#endif // PACKET_POOL_H_
