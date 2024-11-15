#include "protocols/common/rdma_multicast.h"

#include <cassert>

using namespace rapid;

DEFINE_string(role, "sender", "Execution role: sender, receiver");
DEFINE_string(device, "192.168.3.87", "Device IP address");

const static std::string kMulticastAddress = "239.0.0.1";
const static size_t kBufferSize = 4096;

int sender()
{
    RdmaMulticastContext context;
    int ret = context.construct(FLAGS_device, kMulticastAddress);
    assert(ret == 0);
    void *buffer = malloc(kBufferSize);
    strcpy((char *) buffer, "Hello world!");
    ret = context.registerMemoryRegion(buffer, kBufferSize, IBV_ACCESS_LOCAL_WRITE);
    assert(ret == 0);
    Request *request = new Request{
        .addr = { buffer, nullptr },
        .length = { 13, 0 },
        .lkey = { context.key(buffer).first, 0 }
    };
    ret = context.postSendRequest({request});
    assert(ret >= 0);
    while (true)
    {
        ibv_wc wc;
        ret = context.poll(1, &wc);
        assert(ret >= 0);
        if (ret != 0)
            break;
    }
    LOG(INFO) << "Send Complete!";
    return 0;
}

int receiver()
{
    RdmaMulticastContext context;
    int ret = context.construct(FLAGS_device, kMulticastAddress);
    assert(ret == 0);
    void *buffer = malloc(kBufferSize);
    ret = context.registerMemoryRegion(buffer, kBufferSize, IBV_ACCESS_LOCAL_WRITE);
    assert(ret == 0);
    Request *request = new Request{
        .addr = { buffer, nullptr },
        .length = { kBufferSize, 0 },
        .lkey = { context.key(buffer).first, 0 }
    };
    
    ret = context.postReceiveRequest({request});
    assert(ret >= 0);
    while (true)
    {
        ibv_wc wc;
        ret = context.poll(1, &wc);
        assert(ret >= 0);
        if (ret != 0)
            break;
    }
    LOG(INFO) << "Receive Complete! Data = " << (char *) buffer + 40;
    return 0;
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_role == "sender")
        return sender();
    else if (FLAGS_role == "receiver")
        return receiver();

    LOG(ERROR) << "Wrong execution role: should be either 'sender' or 'receiver'";
    exit(EXIT_FAILURE);
}
