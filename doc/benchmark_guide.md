# RapidTransfer Benchmark Tool

The `benchmark` tool is a performance testing utility for the **RapidTransfer** RDMA-based file transfer library. It measures aggregate throughput, latency behavior, and system scalability under concurrent RDMA transfers using reliable or unreliable transports.

## Usage

```bash
./benchmark --role=[sender|receiver] [options]
````

---

## Roles

| `--role` Value | Description                     |
| -------------- | ------------------------------- |
| `sender`       | Launches the benchmark sender   |
| `receiver`     | Launches the benchmark receiver |


## Common Options

| Option         | Type   | Description                                                                 |
| -------------- | ------ | --------------------------------------------------------------------------- |
| `--protocol`   | string | RDMA transport: `rdma_reliable`, `rdma_unreliable`, `rdma_unreliable_mcast` |
| `--rdma_port`  | uint32 | RDMA NIC port index (default: `1`)                                          |
| `--gid_index`  | uint32 | RDMA GID index (default: `3`)                                               |
| `--threads`    | uint32 | Number of concurrent threads (default: `8`)                                 |
| `--block_size` | uint32 | Block size for data transfers, in bytes (default: `65536`)                  |
| `--depth`      | uint32 | Number of outstanding work requests per thread (default: `1`)               |

## Sender-Specific Options

| Option              | Type   | Description                                                                |
| ------------------- | ------ | -------------------------------------------------------------------------- |
| `--target_hostname` | string | Comma-separated list of receiver hostnames (e.g., `host1,host2`)           |
| `--first_port`      | uint32 | Base TCP port (per thread) to match receiving endpoints (default: `18888`) |
| `--duration`        | uint32 | Duration of the test in seconds (default: `10`)                            |

## Receiver-Specific Options

| Option         | Type   | Description                                          |
| -------------- | ------ | ---------------------------------------------------- |
| `--first_port` | uint32 | Base port to listen for connections (one per thread) |

## Example Usage

### Launch Receiver (on each node)

```bash
./benchmark --role=receiver \
            --protocol=rdma_unreliable \
            --rdma_port=1 \
            --gid_index=3 \
            --threads=16 \
            --block_size=4096 \
            --first_port=18888
```

This starts 16 listening threads on ports `18888` to `18903`.

### Launch Sender

```bash
./benchmark --role=sender \
            --protocol=rdma_unreliable \
            --rdma_port=1 \
            --gid_index=3 \
            --target_hostname=vm-5-3-3 \
            --threads=16 \
            --block_size=4096 \
            --first_port=18888 \
            --duration=10
```

This initiates RDMA traffic to 16 receiver endpoints on the `vm-5-3-3` host using 4096-byte blocks for 10 seconds.

## Output

At the end of a sender test run, the following is printed:

```text
INFO: x.xx GB/s
```

Where `x.xx` is the total throughput in gigabytes per second.

## Performance Notes

* Each sender thread randomly selects receiver ports and issues asynchronous RDMA sends.
* NUMA-aware memory allocation and CPU binding are used to maximize performance.
* Sender uses software congestion and completion management for high concurrency.
* Reliability depends on the `RapidTransfer` library’s implementation per protocol.

## RDMA Device Discovery

The tool auto-discovers all available RDMA devices and their NUMA topology. It uses round-robin device allocation across threads.

Use `ibv_devinfo` to verify available devices and GID indices if needed.

## License

This benchmark tool is part of the **RapidTransfer** project and is released under the **Apache 2.0 License**.

---

For more details on the underlying transport library, see the main [RapidTransfer README](./README.md).

