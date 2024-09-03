#include "rapid_transfer.h"

#include <atomic>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iomanip>
#include <numa.h>
#include <sys/time.h>
#include <thread>

DEFINE_string(mode, "sender", "Running mode: sender or receiver");
DEFINE_string(path, "", "Path to the file");
DEFINE_string(device_name, "mlx5_3", "RDMA device name");
DEFINE_string(send_to, "optane21", "Send to host name");

using namespace rapid;

static void *allocateMemoryPool(size_t size, int socket_id)
{
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size)
{
    numa_free(addr, size);
}

int receiver()
{
    auto engine = rapid::RapidTransfer::Create("rdma-rc");
    LOG_ASSERT(engine);

    const size_t dram_buffer_size = 1ull << 30;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    engine->registerBuffer(addr, dram_buffer_size);

    TaskID my_task;
    std::atomic<bool> start(false);

    auto on_receive = [&](TaskID task,
                          const std::string &source_hostname,
                          const Attributes &attributes,
                          std::vector<rapid::Buffer> &buffers) -> int
    {
        rapid::Buffer buffer{.addr = addr, .length = dram_buffer_size};
        buffers.push_back(buffer);
        my_task = task;
        start.exchange(true);
        return 0;
    };

    engine->start(on_receive);
    while (!start.load())
        ;
    while (true)
    {
        auto status = engine->getStatus(my_task, nullptr);
        if (status == rapid::SUCCESS || status == rapid::FAILED)
            break;
    }
    engine->shutdown();

    engine->unregisterBuffer(addr);
    freeMemoryPool(addr, dram_buffer_size);

    return 0;
}

int sender()
{
    auto engine = rapid::RapidTransfer::Create("rdma-rc");
    LOG_ASSERT(engine);

    const size_t dram_buffer_size = 1ull << 30;
    void *addr = allocateMemoryPool(dram_buffer_size, 0);
    engine->registerBuffer(addr, dram_buffer_size);

    rapid::Buffer buf = {.addr = addr, .length = dram_buffer_size};
    TaskID task = engine->send({FLAGS_send_to}, {}, {buf});
    while (true)
    {
        auto status = engine->getStatus(task, nullptr);
        if (status == rapid::SUCCESS || status == rapid::FAILED)
            break;
    }

    engine->unregisterBuffer(addr);
    freeMemoryPool(addr, dram_buffer_size);

    return 0;
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (FLAGS_mode == "sender")
        return sender();
    else if (FLAGS_mode == "receiver")
        return receiver();

    LOG(ERROR) << "Unsupported mode: must be 'sender' or 'receiver'";
    exit(EXIT_FAILURE);
}
