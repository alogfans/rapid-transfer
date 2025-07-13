# RapidTransfer: Reliable File Transfer over UD-based RDMA Networks

**RapidTransfer** is a high-performance, reliable file transfer framework built on top of RDMA's Unreliable Datagram (UD) transport. It provides TCP-like semantics for data transmission without the scalability limitations of RDMA Reliable Connection (RC) transport.

## Features

- ✅ **Reliable file transfer over RDMA UD**
- 🚀 **Scalable multicast support**
- 🔒 **Zero-copy and memory registration for high throughput**
- 💡 **Congestion control and loss recovery**
- 🧵 **Asynchronous, stream-based API**
- 📦 **Support for multiple concurrent sessions**

## Motivation

Traditional RDMA RC connections offer hardware-guaranteed reliability but suffer severe scalability issues in large clusters due to connection explosion. RDMA UD provides a connectionless alternative but lacks built-in reliability. RapidTransfer bridges this gap, offering a reliable and scalable transport over RDMA UD, including multicast support for efficient data replication.

## Architecture

RapidTransfer uses a software-driven protocol called **RTP (RapidTransfer Protocol)**. It includes:

- **Ordered delivery and retransmission** via Go-Back-N style ARQ
- **Per-session sliding windows** for congestion and flow control
- **Custom message headers** for session management and loss detection
- **Multicast queues** with per-receiver acknowledgment tracking

## Use Cases

- [X] Distributed storage systems
- [X] Machine learning model parameter broadcasting
- [X] High-throughput parallel data replication

## Supported Modes

1. **rdma_reliable**: Uses RDMA RC (hardware-reliable)
2. **rdma_unreliable**: Uses RDMA UD with software reliability (recommended)
3. **rdma_unreliable_mcast**: Uses RDMA UD with multicast (for one-to-many transfers)

## Quick Start

### 1. Create and initialize

```cpp
auto rt = RapidTransfer::Create("rdma_unreliable", "mlx5_1", 1, 0);
````

### 2. Register memory

```cpp
rt->registerLocalMemory(send_buffer, buffer_size);
```

### 3. Start listener (receiver only)

```cpp
rt->startListener(":12348", [](const std::string &peer, bool joined) {
    std::cout << (joined ? "Join" : "Leave") << ": " << peer << std::endl;
});
```

### 4. Send / Receive

```cpp
TaskID task = rt->send("10.1.100.3:12348", {Buffer(send_buffer, size)});
```

```cpp
TaskID task = rt->receive("10.1.101.3:12348", {Buffer(recv_buffer, size)});
```

### 5. Query task status

```cpp
Status status;
size_t bytes;
status = rt->getStatus(task, &bytes);
```

### 6. Shutdown

```cpp
rt->shutdownListener();
```

## Benchmark Results

RapidTransfer achieves over **4.5×** the throughput of RDMA RC under high concurrency, showing excellent scalability for large clusters. See the `docs/benchmark.png` for details.

## Build Instructions

```bash
mkdir build && cd build
cmake ..
make -j
```

## API Reference

### Create

```cpp
std::shared_ptr<RapidTransfer> Create(const std::string &protocol,
                                      const std::string &device_name,
                                      uint8_t rdma_port,
                                      int gid_index);
```

### Send / Receive

```cpp
TaskID send(const std::string &peer_name,
            const std::vector<Buffer> &buffer_list);

TaskID receive(const std::string &peer_name,
               const std::vector<Buffer> &buffer_list);
```

### Status and Cleanup

```cpp
Status getStatus(TaskID task_id, size_t *transferred_bytes);
int freeTask(TaskID task_id);
```

### Memory Registration

```cpp
int registerLocalMemory(void *addr, size_t length);
int unregisterLocalMemory(void *addr);
```

## License

This project is licensed under the Apache 2.0 License.

