# RapidTransfer File Transfer CLI Tool

The `file_transfer` CLI tool is a sample application for demonstrating reliable file transfer using the RapidTransfer RDMA library. It supports both **point-to-point** and **multicast** RDMA transfers over reliable or unreliable transports.

## Usage

```bash
./file_transfer --role=[sender|receiver] [options]
````

## Required Options

| Option        | Type   | Description                                                                             |
| ------------- | ------ | --------------------------------------------------------------------------------------- |
| `--role`      | string | Execution role: `sender` or `receiver`                                                  |
| `--protocol`  | string | Transport protocol. One of: `rdma_reliable`, `rdma_unreliable`, `rdma_unreliable_mcast` |
| `--device`    | string | RDMA device name (e.g., `mlx5_1`, `ibp6s0`)                                             |
| `--rdma_port` | uint32 | RDMA port number (typically `1`)                                                        |
| `--gid_index` | uint32 | GID index for RDMA device                                                               |

## Sender-Specific Options

| Option             | Type   | Description                                                                       |
| ------------------ | ------ | --------------------------------------------------------------------------------- |
| `--path`           | string | Path to the local file to send                                                    |
| `--target`         | string | Target host(s) and port(s), separated by commas (e.g., `host1:12348,host2:12348`) |
| `--multicast_addr` | string | Multicast address for `rdma_unreliable_mcast` mode (default: `239.0.0.1`)         |

## Receiver-Specific Options

| Option             | Type   | Description                                         |
| ------------------ | ------ | --------------------------------------------------- |
| `--listen`         | string | Address and port to listen on (e.g., `:12348`)      |
| `--path`           | string | Output file path prefix (e.g., `/tmp/output`)       |
| `--num_recv_files` | uint32 | Number of concurrent receiving files (default: `1`) |

## Examples

### Point-to-Point Transfer

**Sender:**

```bash
./file_transfer --role=sender \
                --protocol=rdma_unreliable \
                --device=mlx5_1 \
                --rdma_port=1 \
                --gid_index=0 \
                --target=10.1.100.3:12348 \
                --path=./myfile.dat
```

**Receiver:**

```bash
./file_transfer --role=receiver \
                --protocol=rdma_unreliable \
                --device=mlx5_1 \
                --rdma_port=1 \
                --gid_index=0 \
                --listen=:12348 \
                --path=/tmp/received.dat
```

### Multicast Transfer (One-to-Many)

**Sender:**

```bash
./file_transfer --role=sender \
                --protocol=rdma_unreliable_mcast \
                --device=mlx5_1 \
                --rdma_port=1 \
                --gid_index=0 \
                --multicast_addr=239.0.0.1 \
                --target=10.1.100.3:12348,10.1.100.4:12348 \
                --path=./dataset.bin
```

**Receivers (run on multiple nodes):**

```bash
./file_transfer --role=receiver \
                --protocol=rdma_unreliable_mcast \
                --device=mlx5_1 \
                --rdma_port=1 \
                --gid_index=0 \
                --listen=:12348 \
                --path=/tmp/received \
                --num_recv_files=2
```

Files will be saved to `/tmp/received`, `/tmp/received.1`, etc.

## Behavior

* The tool preallocates 1 GiB of DRAM for buffer space.
* The sender sends the file size first (8 bytes), followed by file content in chunks.
* The receiver reconstructs the file using RDMA and saves it to the specified path.
* In multicast mode, the sender sends to a multicast group and distributes data to all listed receivers.

## Notes

* Ensure RDMA devices are correctly configured and accessible.
* Use `ibv_devinfo` to query available RDMA devices and GID indices.
* The receiver waits for all expected file transfers to complete based on `--num_recv_files`.
* Memory buffers are allocated using NUMA-aware APIs for better performance.

## License

This tool is part of the **RapidTransfer** project and is released under the Apache 2.0 License.

---

For more information, visit the main [RapidTransfer README](./README.md).
