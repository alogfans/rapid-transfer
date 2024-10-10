// Copyright 2024 Feng Ren

#ifndef PACKET_POOL_H_
#define PACKET_POOL_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cassert>
#include <linux/types.h>
#include <endian.h>

#include "packet.h"
#include "protocols/common/rdma_context.h"

namespace rapid
{
    class PacketPool
    {
    public:
        const static size_t kPacketStorageSize = 4096 + 40;
        const static size_t kPacketBufferCount = 512;

        PacketPool(RdmaContext &context) : context_(context) {}

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
            return (PacketHeader *)ptr;
        }

        void freePacket(PacketHeader *header)
        {
            *(uintptr_t *)header = (uintptr_t)next_free_packet_buffer_;
            next_free_packet_buffer_ = header;
        };
    
    private:
        RdmaContext &context_;
        void *packet_buffer_;
        void *next_free_packet_buffer_;
    };
}

#endif // PACKET_POOL_H_
