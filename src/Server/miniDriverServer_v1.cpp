#include "network/TcpServer.hpp"
#include "utils/ThreadPool.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>


using namespace miniKV::network;
using namespace miniKV::utils;

ThreadPool g_threadPool(4);

// 模拟简单的哈希存储
std::unordered_map<std::string, std::string> g_storage;

// 辅助函数：从 Buffer 中查找并切分 \r\n 结尾的字符串
std::string findLine(Buffer* buf) {
    const char* peek = buf->peek();
    size_t len = buf->readableBytes();
    for (size_t i = 0; i < len - 1; ++i) {
        if (peek[i] == '\r' && peek[i+1] == '\n') {
            std::string line = buf->retrieveAsString(i + 2); // 提取包括 \r\n
            return line.substr(0, i); // 返回不含 \r\n 的部分
        }
    }
    return "";
}


// 辅助函数：仅寻找 \r\n 的位置，不移动 Buffer 指针（安全探测）
const char* findCRLF(const char* start, size_t len) {
    for (size_t i = 0; i < len - 1; ++i) {
        if (start[i] == '\r' && start[i+1] == '\n') {
            return start + i;
        }
    }
    return nullptr;
}

// 业务逻辑：处理并解析标准的 RESP 协议命令
void onMessage(const TcpConnectionPtr& conn, Buffer* buf) {
    // 只要缓冲区里还有数据，就持续尝试解析
    while (buf->readableBytes() > 0) {
        const char* start = buf->peek();
        size_t totalLen = buf->readableBytes();

        // 1. 安全校验：标准的 Redis 请求必须以 '*' 开头（RESP 数组）
        if (start[0] != '*') {
            // 收到非法协议，安全起见：清空缓冲区，发回错误并断开/拒绝
            buf->retrieve(totalLen); // 清空缓冲区
            conn->send("-ERR unknown protocol\r\n");
            return;
        }

        // 2. 探测数组长度的第一行 (如 *3\r\n)
        const char* crlf = findCRLF(start, totalLen);
        if (!crlf) {
            // 说明第一行数据都还没收全，直接退出，等待下一次读事件
            return; 
        }

        int numArgs = 0;
        try {
            // 将 * 后面的数字转换为整型参数个数
            numArgs = std::stoi(std::string(start + 1, crlf));
        } catch (const std::exception& e) {
            // 防止 stoi 转换失败崩溃（例如恶意发来 *abc\r\n）
            buf->retrieve(totalLen);
            conn->send("-ERR protocol error: invalid array length\r\n");
            return;
        }

        size_t consumed = (crlf - start) + 2; // 当前已探测的字节数
        std::vector<std::string> args;
        bool complete = true;

        // 3. 循环探测后续所有的参数是否完整
        for (int i = 0; i < numArgs; ++i) {
            // 安全边界检查：如果剩余数据连 "$1\r\n" 这种最短参数头都不够，说明包没收全
            if (totalLen - consumed < 4) { 
                complete = false; 
                break; 
            }

            const char* argStart = start + consumed;
            if (argStart[0] != '$') {
                buf->retrieve(totalLen);
                conn->send("-ERR protocol error: expected '$' for bulk string\r\n");
                return;
            }

            // 寻找当前参数长度的 \r\n (如 $3\r\n)
            const char* argCrlf = findCRLF(argStart, totalLen - consumed);
            if (!argCrlf) { 
                complete = false; 
                break; 
            }

            int argLen = 0;
            try {
                argLen = std::stoi(std::string(argStart + 1, argCrlf));
            } catch (const std::exception& e) {
                buf->retrieve(totalLen);
                conn->send("-ERR protocol error: invalid bulk string length\r\n");
                return;
            }

            size_t headLen = (argCrlf - argStart) + 2; // "$3\r\n" 的长度

            // 安全边界检查：检查缓冲区剩余的字节是否够装【参数内容 + 结尾的\r\n】
            if (totalLen - consumed < headLen + argLen + 2) { 
                complete = false; 
                break; 
            }

            // 探测成功，提取参数内容
            args.push_back(std::string(argStart + headLen, argLen));
            consumed += headLen + argLen + 2; // 累加探测进度
        }

        // 4. 判断这一条完整的 Redis 命令是否完全到达了缓冲区
        if (!complete) {
            // 如果没收全，【千万不能调用 retrieve！】
            // 保留 Buffer 里的所有数据，直接退出，等待网络读取剩下的半截数据
            return; 
        }

        // 5. 到这一步，说明一整条命令已经100%在缓冲区里了。此时一次性从 Buffer 中移出它们
        buf->retrieve(consumed);

        if (args.empty()) {
            continue;
        }

        // 6. 路由并执行业务命令逻辑
        std::string cmd = args[0];
        if (cmd == "PING" || cmd == "ping") {
            conn->send("+PONG\r\n");
        } 
        else if (cmd == "SET" || cmd == "set") {
            if (args.size() == 3) {
                g_storage[args[1]] = args[2]; // 写入哈希表
                conn->send("+OK\r\n");
            } else {
                conn->send("-ERR wrong number of arguments for 'set' command\r\n");
            }
        } 
        else if (cmd == "GET" || cmd == "get") {
            if (args.size() == 2) {
                auto it = g_storage.find(args[1]);
                if (it != g_storage.end()) {
                    std::string val = it->second;
                    // 返回符合 RESP 规范的大字符串：$长度\r\n内容\r\n
                    conn->send("$" + std::to_string(val.size()) + "\r\n" + val + "\r\n");
                } else {
                    conn->send("$-1\r\n"); // 找不到返回 Redis 规范的 NULL 空字符串
                }
            } else {
                conn->send("-ERR wrong number of arguments for 'get' command\r\n");
            }
        } 
        else {
            conn->send("-ERR unknown command '" + cmd + "'\r\n");
        }
    }
}
int main() {
    EventLoop loop;
    TcpServer server(&loop, 8888);
    server.setMessageCallback(onMessage);
    server.start();
    std::cout << "MiniKV RESP Server 已就绪, 端口 8888" << std::endl;
    loop.loop();
    return 0;
}