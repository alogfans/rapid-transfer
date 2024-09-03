// rapid_transfer.h
// Copyright (C) 2024 Feng Ren

#ifndef RAPID_TRANSFER_H
#define RAPID_TRANSFER_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace rapid
{
    struct Buffer
    {
        void *addr;
        size_t length;
    };

    enum Status
    {
        UNKNOWN,
        PENDING,
        SUCCESS,
        FAILED
    };

    using Attributes = std::unordered_map<std::string, std::string>;
    using TaskID = int;

    class SessionManager;
    class Protocol;
    class SessionManager;

    class RapidTransfer
    {
    public:
        static std::shared_ptr<RapidTransfer> Create(const std::string &protocol);

        RapidTransfer();

        virtual ~RapidTransfer();

        // 作为发起方，主动调用此函数以开始执行一次异步的文件传输过程。
        // - target_list：拟传输的目标服务器名称列表。如果成员数量为多个，将会执行多播
        // - attributes：用户定义的附加属性
        // - buffers：传输的数据来源，用 iovec 形式表示。
        // 返回值：TaskID 标识符，通过 getStatus() 可以获知进度&状态
        TaskID send(const std::vector<std::string> &target_list,
                    const Attributes &attributes,
                    const std::vector<Buffer> &buffers);

        // 作为接收方，RapidTransfer 实例启动时会开启监听服务。当发送方发起 send 请求时，
        // RapidTransfer 实例调用此接口，用户需指定缓冲空间并填充到 buffers 向量结构中
        // 之后，RapidTransfer 将自动推进可靠的数据传输。注意传入数据还包括 TaskID 标识符，
        // 因此可通过 getStatus() 获知传输进度&状态
        // 返回值：非0值表示拒绝传输。
        using OnReceiveCallback = std::function<int(TaskID,
                                                    const std::string &,
                                                    const Attributes &,
                                                    std::vector<Buffer> &)>;

        // 获取收/发进度
        Status getStatus(TaskID task, size_t *transferred_bytes);

        // 注册本地内存区域，buffers 所指向的内存空间必须包含其中（暂不可跨越）
        int registerBuffer(void *addr, size_t length);

        // 反注册相应的本地内存区域
        int unregisterBuffer(void *addr);

        // 启动监听服务
        int start(const OnReceiveCallback &on_receive);

        // 停止监听服务
        int shutdown();

    private:
        SessionManager *session_manager_;
        Protocol *protocol_;
    };
} // namespace rapid

#endif // RAPID_TRANSFER_H
