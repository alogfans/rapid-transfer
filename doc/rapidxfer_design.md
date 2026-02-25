# RapidXfer 详细设计文档

## 文档信息

- **项目名称**: RapidXfer
- **版本**: v2.0
- **日期**: 2025-02
- **作者**: RapidXfer Team

---

## 1. 概述 (Overview)

### 1.1 设计目标

RapidXfer 是一个面向大规模存储系统的软件定义 RDMA UD 传输引擎，核心目标：

1. **万级扩展性**: 突破 RDMA RC 的 QPC Thrashing 瓶颈，支持 10K+ 节点
2. **零拷贝传输**: 利用 DMA 直接数据安置（DDP），避免用户态 memcpy
3. **确定性 QoS**: 通过调度器和流控，保证老鼠流不被大象流饿死
4. **极简协议**: 统一的数据消息通道，无独立控制面

### 1.2 核心洞察

**问题**: RDMA RC 在网卡硬件中维护连接状态，受限于片上 SRAM：
- 每个QP状态 ≈ 16KB
- 网卡内存 ≈ 32MB
- 最大连接数 ≈ 2000（实际可用）

当连接数超过阈值时，发生 **Cache Thrashing**：
- QP状态无法全部放入 L1/L2 Cache
- 每次处理数据包都需要访问主内存
- P99 延迟急剧上升（从 2μs → 20μs+）

**解决方案**: 将可靠性机制移至软件，利用主内存资源：
- 软件维护 Session 状态：主内存 128GB，支持 100K+ Sessions
- 物理QP池化：16-32 个 QP 时间复用，保护 Cache
- 按需绑定：只有活跃传输的 Session 才占用物理 QP

### 1.3 架构原则

1. **接口兼容**: 保持 `rapid_transfer.h` 接口不变
2. **统一消息通道**: 所有消息（数据、Read请求、ACK）都是数据包
3. **零控制面**: 完全删除独立的控制消息通道
4. **Pull-based Read**: Read 操作通过数据包中的 flag 标识

---

## 2. 系统架构 (System Architecture)

### 2.1 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层接口                              │
│  write() / read() / wait() / getStatus() / notify()         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   RapidTransfer::Impl                       │
│  - 任务管理 (TaskID → TransferResult)                       │
│  - 进度引擎 (runStep 循环)                                   │
│  - TCP Bootstrap (一次性连接建立)                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                        Context                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  QP Pool (16-32 个物理 QP)                           │  │
│  │  - 动态分配/回收                                      │  │
│  │  - 时间复用                                           │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Scheduler (调度器)                                  │  │
│  │  - QP 分配决策                                       │  │
│  │  - 空闲 QP 回收                                       │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Session Table (虚拟 Session)                        │  │
│  │  - 软件维护状态 (主内存)                              │  │
│  │  - 与物理 QP 解耦                                     │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Unified Message Handler (统一消息处理)              │  │
│  │  - 普通数据包 (flags = 0)                            │  │
│  │  - Read Request (flags = FLAGS_READ_REQUEST)        │  │
│  │  - SACK (flags = FLAGS_SACK)                         │  │
│  │  - Chunk-ACK (flags = FLAGS_CHUNK_ACK)              │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                   RDMA 硬件层                                │
│  - UD QPs (不可靠数据报)                                    │
│  - Completion Queues                                       │
│  - Memory Regions                                          │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 组件职责

| 组件 | 职责 | 状态存储 |
|------|------|----------|
| **Impl** | 任务管理、生命周期 | 主内存 |
| **Context** | 统一消息处理、QP池 | 主内存 + 网卡 |
| **Scheduler** | QP分配调度 | 主内存 |
| **SessionTable** | 虚拟Session状态 | 主内存 |

---

## 3. 统一消息协议 (Unified Message Protocol)

### 3.1 设计原则

**核心思想**: 所有消息都是数据包，通过 `flags` 字段区分类型。

**优势**:
- 无独立控制面
- 统一的消息处理路径
- 简化的代码结构
- 更低的延迟（无额外跳转）

### 3.2 统一数据包格式

```
┌────────────────────────────────────────────────────────┐
│ RapidXfer Header (固定大小，24 字节)                   │
├────────────────────────────────────────────────────────┤
│ uint32_t session_id           │ Session ID             │
│ uint32_t chunk_id             │ Chunk ID (4MB 对齐)    │
│ uint16_t seq_num              │ 序列号 (0-65535)       │
│ uint16_t flags                │ 标志位 (关键!)          │
│ uint64_t timestamp            │ 时间戳                 │
└────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────┐
│ Payload (根据 flags 类型不同)                           │
│                                                          │
│ 如果 flags = 0 (普通数据):                              │
│   uint8_t data[MTU - sizeof(Header)]                    │
│                                                          │
│ 如果 flags = FLAGS_READ_REQUEST:                        │
│   uint32_t num_local_targets                            │
│   Buffer local_targets[num_local_targets]              │
│   uint32_t num_remote_sources                           │
│   Buffer remote_sources[num_remote_sources]            │
│                                                          │
│ 如果 flags = FLAGS_SACK:                                │
│   uint64_t recv_bitmap                                 │
│   uint16_t bitmap_start                                │
│                                                          │
│ 如果 flags = FLAGS_CHUNK_ACK:                           │
│   uint32_t total_pkts                                  │
│   uint64_t recv_bitmap                                 │
└────────────────────────────────────────────────────────┘
```

### 3.3 消息类型定义

```cpp
// 消息标志位
enum MessageFlags : uint16_t {
    DATA_PACKET       = 0x0000,  // 普通数据包
    READ_REQUEST      = 0x0001,  // Read 请求
    SACK              = 0x0002,  // 选择性确认
    CHUNK_ACK         = 0x0004,  // Chunk 完成确认
    NOTIFICATION      = 0x0008,  // 应用层通知（可选）
};

// 统一的包头
struct RapidXferHeader {
    uint32_t session_id;
    uint32_t chunk_id;
    uint16_t seq_num;
    uint16_t flags;       // MessageFlags
    uint64_t timestamp;

    // 序列化/反序列化
    void serialize(ibv_sge* sge, uint32_t& imm_data);
    void deserialize(const ibv_sge* sge, uint32_t imm_data);
};
```

---

## 4. 操作模型 (Operation Model)

### 4.1 Write 操作 (Push-based)

**模型**: 发送方主动推送数据到接收方

**流程**:
```
Sender                          Receiver
  │                                │
  │  write(peer, local, remote)    │
  │                                │
  ├─ [数据包序列, flags=0] ───────>│
  │  - SN: 0, 1, 2, ...            │  更新 bitmap
  │                                │
  ├─ [数据包序列, flags=0] ───────>│
  │                                │
  │  <── [SACK, flags=0x0002] ─────┤
  │  告知哪些包丢了                 │
  │                                │
  ├─ [重传包, flags=0] ───────────>│
  │                                │
  │  <── [Chunk-ACK, flags=0x0004]─┤
  │  Chunk 完成                     │
```

**特点**:
- 单向数据流：Sender → Receiver
- 接收方被动：无需协商
- SACK 确认：只报告丢失的包
- Chunk-ACK：每 4MB 发送一次

### 4.2 Read 操作 (Pull-based)

**模型**: 发送方通过特殊数据包请求，接收方响应

**流程**:
```
Sender                          Receiver
  │                                │
  │  read(peer, local, remote)     │
  │                                │
  ├─ [Read Request, flags=0x0001]─>│
  │  - local_targets: 我的接收地址   │
  │  - remote_sources: 你的数据地址 │
  │                                │
  │                                │  解析请求
  │                                │  创建反向 Session
  │                                │
  │  <─── [数据包, flags=0] ────────┤
  │  发送到 local_targets           │
  │                                │
  │  <─── [数据包, flags=0] ────────┤
  │                                │
  ├─ [SACK, flags=0x0002] ─────────>│
  │  告知哪些包丢了                 │
  │                                │
  │  <─── [重传包, flags=0] ────────┤
  │                                │
  ├─ [Chunk-ACK, flags=0x0004] ───>│
  │  确认接收完成                   │
```

**关键点**:
1. **Read Request 是数据包**：`flags = READ_REQUEST`
2. **无需独立响应**：Receiver 直接开始发送数据
3. **数据流向**：Receiver → Sender（反向 Write）
4. **完成确认**：Sender 发送 Chunk-ACK 给 Receiver

---

## 5. 协议细节 (Protocol Details)

### 5.1 Read Request 消息

```cpp
struct ReadRequestPayload {
    // 发送方的接收缓冲区（Receiver push到这里）
    uint32_t num_local_targets;
    Buffer local_targets[];  // 发送方地址

    // 接收方的数据源（Receiver从这里读取）
    uint32_t num_remote_sources;
    Buffer remote_sources[]; // 接收方地址
};
```

**打包**:
```cpp
void sendReadRequest(const std::string& peer_name,
                     const std::vector<Buffer>& local_targets,
                     const std::vector<Buffer>& remote_sources) {

    RapidXferHeader header;
    header.session_id = getSessionId(peer_name);
    header.chunk_id = 0;
    header.seq_num = 0;
    header.flags = READ_REQUEST;  // 关键：标识这是 Read Request
    header.timestamp = getCurrentTimestamp();

    // 构建 payload
    std::vector<uint8_t> payload;
    serializeReadRequest(local_targets, remote_sources, payload);

    // 发送（与普通数据包一样）
    sendPacket(peer_name, header, payload);
}
```

**接收处理**:
```cpp
void onPacketReceived(const RapidXferHeader& header,
                     const std::vector<uint8_t>& payload) {

    if (header.flags & READ_REQUEST) {
        // 这是 Read Request
        handleReadRequest(header, payload);
    } else if (header.flags & SACK) {
        // 这是 SACK
        handleSACK(header, payload);
    } else if (header.flags & CHUNK_ACK) {
        // 这是 Chunk-ACK
        handleChunkAck(header, payload);
    } else {
        // 普通数据包
        handleDataPacket(header, payload);
    }
}
```

### 5.2 SACK 消息

```cpp
struct SACKPayload {
    uint64_t recv_bitmap;      // 64-bit bitmap
    uint16_t bitmap_start;     // Bitmap 起始序列号
};
```

### 5.3 Chunk-ACK 消息

```cpp
struct ChunkAckPayload {
    uint32_t total_pkts;       // Chunk 总包数
    uint64_t recv_bitmap;      // 完整 bitmap (全1表示完整)
};
```

---

## 6. QP 池化管理 (QP Pooling)

### 6.1 设计目标

- 用 16-32 个物理 QP 支持 100K+ 虚拟 Session
- 动态分配：按需绑定
- 自动回收：空闲超时释放

### 6.2 数据结构

```cpp
// 物理 QP
struct PhysicalQP {
    uint32_t qp_num;              // QP 编号
    int bound_session_id;         // 绑定的 Session ID (-1=空闲)
    uint64_t last_active_ts;      // 上次活跃时间戳
};

// 虚拟 Session
struct VirtualSession {
    int session_id;               // Session ID
    std::string peer_addr;        // 对端地址

    // QP 绑定
    int bound_qp_id;              // 绑定的物理 QP ID (-1=未绑定)

    // 发送状态
    uint32_t next_chunk_id;       // 下一个 Chunk ID
    uint16_t next_seq_num;        // 下一个序列号

    // 接收状态
    uint64_t recv_bitmap;         // 接收位图
    uint32_t expected_chunk_id;   // 期望的 Chunk ID

    // 缓冲区
    std::vector<Buffer> pending_buffers;
};
```

### 6.3 QP 生命周期

```
1. Session 创建（无 QP）
   VirtualSession { session_id: 100, bound_qp_id: -1 }
   状态: IDLE

2. 首次传输
   Scheduler::acquireQP(100)
   → 从池中分配 QP #5
   → VirtualSession { session_id: 100, bound_qp_id: 5 }
   状态: BUSY

3. 数据传输
   使用 QP #5 发送所有数据包

4. Chunk 完成
   → 收到 Chunk-ACK
   → Scheduler::releaseQP(100)
   → QP #5 归还到池
   → VirtualSession { session_id: 100, bound_qp_id: -1 }
   状态: IDLE
```

---

## 7. Context 统一设计

### 7.1 核心接口

```cpp
class Context {
public:
    // ========== 公共接口（保持不变）==========
    TaskID send(const std::string& peer_name,
                const std::vector<Buffer>& local_buffers,
                const std::vector<Buffer>& remote_buffers);

    Status getStatus(TaskID task_id, size_t* transferred_bytes);
    int runStep();

    // ========== 连接管理 ==========
    int prepareConnection(const std::string& peer_addr, Attributes& local);
    int setupConnection(const std::string& peer_addr, const Attributes& peer);

    // ========== 统一消息处理 ==========
    using ReadCallback = std::function<TaskID(
        const std::string&, const std::vector<Buffer>&, const std::vector<Buffer>&)>;

    void setReadCallback(ReadCallback callback);

private:
    // ========== QP 池 ==========
    std::vector<PhysicalQP> qp_pool_;
    std::queue<int> free_qp_ids_;
    std::unique_ptr<Scheduler> scheduler_;

    // ========== Session 表 ==========
    std::unordered_map<int, VirtualSession> sessions_;

    // ========== 统一消息处理 ==========
    int handlePacket(const RapidXferHeader& header,
                    const std::vector<uint8_t>& payload,
                    const std::string& peer_name);

    int handleReadRequest(const RapidXferHeader& header,
                         const std::vector<uint8_t>& payload);
    int handleDataPacket(const RapidXferHeader& header,
                        const std::vector<uint8_t>& payload);
    int handleSACK(const RapidXferHeader& header,
                  const std::vector<uint8_t>& payload);
    int handleChunkAck(const RapidXferHeader& header,
                      const std::vector<uint8_t>& payload);

    // ========== TCP Bootstrap ==========
    int startBootstrapListener(const std::string& tcp_address);
    void handleBootstrapConnection(int client_fd);
};
```

### 7.2 统一消息处理流程

```cpp
int Context::processReceivedPacket(uint64_t current_ts, ibv_wc& wc) {
    if (wc.opcode == IBV_WC_RECV) {
        // 1. 反序列化包头
        RapidXferHeader header;
        std::vector<uint8_t> payload;
        deserializePacket(wc, header, payload);

        // 2. 提取 peer 名称
        std::string peer_name = extractPeerName(wc);

        // 3. 根据 flags 路由
        return handlePacket(header, payload, peer_name);
    }
}

int Context::handlePacket(const RapidXferHeader& header,
                          const std::vector<uint8_t>& payload,
                          const std::string& peer_name) {

    if (header.flags & READ_REQUEST) {
        return handleReadRequest(header, payload);
    } else if (header.flags & SACK) {
        return handleSACK(header, payload);
    } else if (header.flags & CHUNK_ACK) {
        return handleChunkAck(header, payload);
    } else {
        // 普通数据包
        return handleDataPacket(header, payload);
    }
}
```

---

## 8. Read 操作实现 (Pull-based Read)

### 8.1 发送端逻辑

```cpp
TaskID Context::read(const std::string& peer_name,
                     const std::vector<Buffer>& local_buffers,
                     const std::vector<Buffer>& remote_buffers) {

    // 1. 立即生成 task_id
    TaskID task_id = allocateTaskId();

    // 2. 创建/获取 Session
    int session_id = getOrCreateSession(peer_name);
    VirtualSession& session = sessions_[session_id];
    session.task_id = task_id;
    session.state = TaskState::PENDING;
    session.local_targets = local_buffers;

    // 3. 发送 Read Request（作为特殊数据包）
    RapidXferHeader header;
    header.session_id = session_id;
    header.chunk_id = 0;
    header.seq_num = 0;
    header.flags = READ_REQUEST;  // 关键标识
    header.timestamp = getCurrentTimestamp();

    // 构建 payload
    std::vector<uint8_t> payload;
    serializeReadRequest(local_buffers, remote_buffers, payload);

    // 发送（与普通数据包相同路径）
    sendPacket(peer_name, header, payload);

    // 4. 立即返回 task_id
    return task_id;
}
```

### 8.2 接收端逻辑

```cpp
int Context::handleReadRequest(const RapidXferHeader& header,
                               const std::vector<uint8_t>& payload) {

    // 1. 反序列化 Read Request
    ReadRequestPayload req;
    deserializeReadRequest(payload, req);

    // 2. 创建反向 Session（用于发送数据回发送方）
    std::string peer_name = getPeerName(header.session_id);
    int reverse_session_id = createSession(peer_name);

    // 3. 调用回调（让上层应用准备数据）
    if (on_read_request_) {
        TaskID task_id = on_read_request_(peer_name,
            req.local_targets,   // 发送到这里
            req.remote_sources); // 从这里读取
    }

    // 4. 开始发送数据到 req.local_targets
    VirtualSession& session = sessions_[reverse_session_id];
    session.pending_buffers = req.remote_sources;

    sendBufferData(session, req.local_targets);

    return 0;
}
```

---

## 9. 性能优化 (Performance Optimization)

### 9.1 控制流量优化

**目标**: 控制流量 < 0.1% of 数据流量

**策略**:
```
传统方案（每包 ACK）:
  4MB 数据 ≈ 2731 个包
  → 2731 个 ACK
  → 控制流量 ≈ 175 KB

优化方案（Chunk-ACK）:
  4MB 数据 ≈ 2731 个包
  → 1 个 Chunk-ACK (24 B header + 8 B payload = 32 B)
  → 偶尔的 SACK (假设 10 个包丢失，32 B)
  → 控制流量 ≈ 64 B

减少: 99.96%
```

### 9.2 QP 池效率

**配置**:
- QP 池: 32 个物理 QP
- 最大 Session: 100,000 个
- 活跃 Session: 1,000 个（同时传输）

**QP 复用率**:
```
1000 个活跃 Session / 32 个 QP ≈ 31

每个 QP 平均服务 31 个 Session（时间复用）
```

**Cache 友好性**:
```
活跃 QP 状态: 32 × ~100 bytes = 3.2 KB
可完全放入 L1 Cache

QP 切换开销: O(1)
Cache 命中率: >99%
```

### 9.3 消息路径统一

```
旧架构（控制面分离）:
  Impl → UDControlManager → UD send
  Impl → Context → Data send

新架构（统一消息）:
  Impl → Context → Unified send
     ├─ Read Request (flags=0x0001)
     ├─ Data (flags=0x0000)
     ├─ SACK (flags=0x0002)
     └─ Chunk-ACK (flags=0x0004)

优势:
- 单一代码路径
- 无组件间通信开销
- 更好的内联优化
```

---

## 10. 可靠性保证 (Reliability)

### 10.1 数据包丢失

**检测**: 超时 (RTO)

**恢复**: 选择性重传（通过 SACK）
```cpp
void onTimeout(int session_id) {
    auto& session = sessions_[session_id];

    // 发送 SACK（携带当前接收状态）
    RapidXferHeader header;
    header.flags = SACK;
    header.session_id = session_id;
    header.seq_num = 0;

    SACKPayload sack;
    sack.recv_bitmap = session.recv_bitmap;
    sack.bitmap_start = 0;

    sendPacket(peer_name, header, sack);
}
```

### 10.2 Chunk 完成

**条件**: `recv_bitmap == 0xFFFFFFFFFFFFFFFFULL`

**确认**: 发送 Chunk-ACK
```cpp
void sendChunkAck(int session_id, uint32_t chunk_id) {
    RapidXferHeader header;
    header.flags = CHUNK_ACK;
    header.session_id = session_id;
    header.chunk_id = chunk_id;

    ChunkAckPayload ack;
    ack.total_pkts = 64;  // 假设 64 个包
    ack.recv_bitmap = 0xFFFFFFFFFFFFFFFFULL;

    sendPacket(peer_name, header, ack);
}
```

### 10.3 At-most-once 语义

对于 Read Request，需要去重：

```cpp
struct DedupKey {
    std::string peer_name;
    std::string request_hash;  // hash(local_targets, remote_sources)
};

std::unordered_map<DedupKey, TaskID> dedup_cache_;

int handleReadRequest(const RapidXferHeader& header,
                     const std::vector<uint8_t>& payload) {
    // 计算哈希
    ReadRequestPayload req;
    deserializeReadRequest(payload, req);

    std::string hash = computeHash(req.local_targets, req.remote_sources);
    DedupKey key{peer_name, hash};

    // 检查重复
    if (dedup_cache_.find(key) != dedup_cache_.end()) {
        // 重复请求：忽略（数据已经在传输或已完成）
        return 0;
    }

    // 处理新请求
    TaskID task_id = on_read_request_(...);
    dedup_cache_[key] = task_id;

    return 0;
}
```

---

## 11. 性能指标 (Performance Metrics)

### 11.1 扩展性

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 最大 Session 数 | 100,000 | 主内存限制 |
| 活跃 Session 数 | 10,000 | 同时传输 |
| 物理 QP 数 | 16-32 | Cache 友好 |
| QP 复用率 | >100x | 32 QP 服务 10K Session |

### 11.2 延迟

| 指标 | 目标值 | 对比 RC |
|------|--------|---------|
| 平均延迟 | <2μs | 相同 |
| P99 延迟 | <5μs | 更好（RC >10μs @ 2K conns）|
| P999 延迟 | <10μs | 更好（RC >50μs @ 2K conns）|

### 11.3 吞吐量

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 单流吞吐 | 100 Gbps | 线速 |
| 总吞吐 | 400 Gbps | 4 端口聚合 |
| 零拷贝率 | 100% | 无 memcpy |

### 11.4 消息处理效率

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 每包处理 | <50 cycles | 统一消息路径 |
| 消息分发 | O(1) | flags 判断 |
| Cache 命中率 | >99% | QP 状态在 L1 |

---

## 12. 实现路线图 (Implementation Roadmap)

### Phase 1: 基础架构 (Week 1-2)
- [ ] 定义统一消息格式（RapidXferHeader）
- [ ] 实现消息 flags 路由
- [ ] Scheduler 实现（QP 池管理）

### Phase 2: 数据传输 (Week 3-4)
- [ ] Chunk-based 传输
- [ ] SACK 机制
- [ ] Chunk-ACK

### Phase 3: Read 操作 (Week 5)
- [ ] Read Request 作为数据包
- [ ] 反向 Session
- [ ] Pull-based 数据流

### Phase 4: 集成与优化 (Week 6-7)
- [ ] 移除 UDControlManager
- [ ] 统一到 Context
- [ ] 性能测试

### Phase 5: 测试与验证 (Week 8)
- [ ] e2e 测试
- [ ] 性能基准测试
- [ ] 文档完善

---

## 13. 架构对比

### 13.1 消息路径对比

**旧架构（控制面分离）**:
```
Read 操作:
  Sender → UDControlManager::sendReadRequest()
          → UD send (控制消息)
  Receiver → UDControlManager::handleReadRequest()
           → callback
           → UDControlManager::sendReadResponse()
           → UD send (控制消息)
  Sender → UDControlManager::wait()
          → 收到 Read Response
  Receiver → Context::send()
           → UD send (数据)
```

**新架构（统一消息）**:
```
Read 操作:
  Sender → Context::read()
          → UD send (Read Request 数据包, flags=0x0001)
  Receiver → Context::handlePacket()
           → handleReadRequest()
           → Context::send()
           → UD send (数据包, flags=0x0000)
```

### 13.2 代码复杂度对比

| 指标 | 旧架构 | 新架构 | 减少 |
|------|--------|--------|------|
| 源文件 | ~15 | ~10 | 33% |
| 代码行数 | ~5000 | ~3500 | 30% |
| 消息类型 | 8 | 4 | 50% |
| 消息路径 | 2 | 1 | 50% |

---

## 14. 附录 (Appendix)

### A. 文件结构

```
include/
  └── rapid_transfer.h         (不变)

src/
  ├── rapid_transfer.cpp       (不变)
  ├── impl.h                   (简化)
  ├── impl.cpp                 (简化)
  ├── context.h                (重大修改：统一消息)
  ├── context.cpp              (重大修改)
  ├── scheduler.h              (新增)
  ├── scheduler.cpp            (新增)
  └── protocol.h               (删除)

test/
  └── e2e_test.cpp             (不变)

doc/
  └── rapidxfer_design.md      (本文档)
```

### B. 删除的文件

```
src/ud_control_manager.h       (删除：功能并入 Context)
src/ud_control_manager.cpp     (删除)
src/ud_control_protocol.h      (删除：用统一包头替代)
src/ud_control_protocol.cpp    (删除)
src/session_manager.h          (删除：用 SessionTable 替代)
src/session_manager.cpp        (删除)
```

### C. 关键数据结构大小

```cpp
sizeof(RapidXferHeader)     ≈ 24 bytes
sizeof(PhysicalQP)          ≈ 24 bytes
sizeof(VirtualSession)       ≈ 1 KB
sizeof(SendWindow)           ≈ 64 bytes

100K Sessions: 1 KB * 100K ≈ 100 MB (可接受)
32 QPs: 24 bytes * 32 ≈ 768 bytes (可忽略)
```

---

## 15. 总结 (Summary)

RapidXfer v2.0 通过**统一消息协议**实现了极致简化：

1. **零控制面**: 所有消息都是数据包，通过 flags 区分
2. **Read 作为数据包**: `flags = READ_REQUEST`，无独立控制通道
3. **QP 池化**: 16-32 个物理 QP 支持 100K+ Session
4. **极简代码**: 统一消息路径，代码量减少 30%

**核心优势**:
- **扩展性**: 50x-100x 提升（vs RC）
- **简洁性**: 单一消息处理路径
- **性能**: P99 延迟更低，控制流量减少 99.96%
- **可维护性**: 代码量减少，架构清晰

---

**文档版本**: v2.0
**最后更新**: 2025-02
**下一步**: 开始 Phase 1 实现