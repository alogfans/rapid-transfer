// Copyright 2024 Feng Ren

#ifndef PACKET_H_
#define PACKET_H_

#include <cstddef>
#include <cstdint>
#include <cassert>
#include <linux/types.h>
#include <endian.h>

namespace rapid
{
    const static uint32_t CMD_DATA = 81;
    const static uint32_t CMD_ACK = 82;
    struct PacketHeader
    {
        __le32 cid;
        __u8 cmd;
        __u8 resv1;
        __le16 wnd;
        __le64 ts;
        __le32 sn;
        __le32 una;
        __le32 len;
        __le32 resv2;
    };

    static inline void EncodePacket(PacketHeader *dst, const PacketHeader &src)
    {
        dst->cid = htole32(src.cid);
        dst->cmd = src.cmd;
        dst->resv1 = src.resv1;
        dst->wnd = htole16(src.wnd);
        dst->ts = htole64(src.ts);
        dst->sn = htole32(src.sn);
        dst->una = htole32(src.una);
        dst->len = htole32(src.len);
        dst->resv2 = htole32(src.resv2);
    }

    static inline void DecodePacket(PacketHeader &dst, const PacketHeader *src)
    {
        dst.cid = le32toh(src->cid);
        dst.cmd = src->cmd;
        dst.resv1 = src->resv1;
        dst.wnd = le16toh(src->wnd);
        dst.ts = le64toh(src->ts);
        dst.sn = le32toh(src->sn);
        dst.una = le32toh(src->una);
        dst.len = le32toh(src->len);
        dst.resv2 = le32toh(src->resv2);
    }
}

#endif // PACKET_H_
