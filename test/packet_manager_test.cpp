#include "protocols/rdma_unreliable/packet_manager.h"

#include <gtest/gtest.h>

using namespace rapid;

TEST(PacketManagerTest, PacketHandle) {
    const static size_t kPacketCapacity = 1024;
    char packet_buf[kPacketCapacity];
    char data[16];
    strcpy(data, "Hello world");
    PacketHandle handle;
    ASSERT_EQ(0, handle.setRawPacket(packet_buf));
    handle.session = 3;
    handle.cmd = 1;
    handle.wnd = 4096;
    handle.sn = 0x5f1234;
    handle.compacted = false;

    uint32_t pkt_hdr_imm;
    std::vector<Buffer> slices;
    ASSERT_EQ(0, handle.setPayload(data, 16, true));
    ASSERT_EQ(0, handle.serialize(slices, pkt_hdr_imm));
    ASSERT_EQ(slices.size(), 1);
    LOG(INFO) << slices[0].addr << " " << slices[0].length;
    ASSERT_EQ(slices[0].length, sizeof(PktHdr) + 16);

    LOG(INFO) << pkt_hdr_imm;

    PacketHandle handle_remote;
    ASSERT_EQ(0, handle_remote.setRawPacket(packet_buf));
    ASSERT_EQ(0, handle_remote.deserialize(pkt_hdr_imm, slices[0].length));
    void *data_remote = handle_remote.getPayload();
    LOG(INFO) << data_remote;
    LOG(INFO) << (char *)data_remote;
    ASSERT_EQ("Hello world", std::string((char *)data_remote));

    ASSERT_EQ(handle.session, handle_remote.session);
    ASSERT_EQ(handle.cmd, handle_remote.cmd);
    ASSERT_EQ(handle.wnd, handle_remote.wnd);
    ASSERT_EQ(handle.sn, handle_remote.sn);
}

TEST(PacketManagerTest, PacketBufferPool) {
    PacketBufferPool pool(1024, 4096);
    ASSERT_EQ(0, pool.construct());
    PacketHandle handle;
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(0, pool.allocatePacket(handle));
        // ASSERT_TRUE(handle.packet_buf);
        ASSERT_EQ(0, pool.freePacket(handle));
    }
}

TEST(PacketManagerTest, PacketManager) {
    PacketManager manager;
    ASSERT_EQ(0, manager.construct());
    char data[256] = "Hello world";
    uint32_t sn;
    uint64_t head;
    uint64_t tail;
    auto &send_queue = manager.getSendQueue(0);
    Buffer slice{data, 256};
    ASSERT_EQ(0, send_queue.push({slice}, sn));
    ASSERT_EQ(0, sn);
    ASSERT_EQ(0, send_queue.push({slice}, sn));
    ASSERT_EQ(1, sn);
    ASSERT_EQ(0, send_queue.markCompleted(1));
    ASSERT_EQ(0, send_queue.getIndexRange(head, tail));
    ASSERT_EQ(2, head);
    ASSERT_EQ(0, tail);
    ASSERT_EQ(0, send_queue.markCompleted(0));
    ASSERT_EQ(0, send_queue.getIndexRange(head, tail));
    ASSERT_EQ(2, head);
    ASSERT_EQ(2, tail);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
