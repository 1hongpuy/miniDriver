#pragma once

#include <string>
#include <fstream>
#include <sstream>
#include <cstring>
#include <ctime>
#include <sys/stat.h>
#include <unistd.h>


namespace miniKV {
namespace handler {


// ===== MIME 类型 =====
inline std::string getMimeType(const std::string& filename) {
    size_t dot = filename.find_last_of('.');
    if (dot == std::string::npos) return "application/octet-stream";
    std::string ext = filename.substr(dot);
    for (auto& c : ext) c = std::tolower(c);
    if (ext == ".html" || ext == ".htm") return "text/html; charset=utf-8";
    if (ext == ".css")   return "text/css; charset=utf-8";
    if (ext == ".js")    return "application/javascript; charset=utf-8";
    if (ext == ".json")  return "application/json; charset=utf-8";
    if (ext == ".jpg" || ext == ".jpeg") return "image/jpeg";
    if (ext == ".png")   return "image/png";
    if (ext == ".gif")   return "image/gif";
    if (ext == ".webp")  return "image/webp";
    if (ext == ".svg")   return "image/svg+xml";
    if (ext == ".mp4")   return "video/mp4";
    if (ext == ".mov")   return "video/quicktime";
    if (ext == ".mkv")   return "video/x-matroska";
    return "application/octet-stream";
}

// ===== 路径标准化 =====
inline std::string normalizePath(const std::string& p) {
    std::string s = p;
    if (s.empty()) return "/";
    if (s[0] != '/') s = "/" + s;
    while (s.size() > 1 && s.back() == '/') s.pop_back();
    size_t pos;
    while ((pos = s.find("//")) != std::string::npos) s.replace(pos, 2, "/");
    if (s.empty() || s == "/") return "/";
    return s + "/";
}

// ===== 读文件 =====
//把文件里面得数据读取成一个string写入
inline std::string readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return "";
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// ===== 确保目录 =====
inline bool ensureDir(const std::string& path) {
    std::string dir = path;
    while (!dir.empty() && dir.back() == '/') dir.pop_back();
    if (dir.empty()) return true;
    struct stat st;
    if (stat(dir.c_str(), &st) == 0) return S_ISDIR(st.st_mode);
    size_t pos = dir.find_last_of('/');
    if (pos != std::string::npos) ensureDir(dir.substr(0, pos));
    return mkdir(dir.c_str(), 0755) == 0 || errno == EEXIST;
}

// ===== Query String 解析 =====
inline std::string getQueryParam(const std::string& query, const std::string& key) {
    std::string search = key + "=";
    size_t pos = query.find(search);
    if (pos == std::string::npos) return "";
    pos += search.size();
    size_t end = query.find('&', pos);
    if (end == std::string::npos) end = query.size();
    return query.substr(pos, end - pos);
}

// ===== URL Decode =====
inline std::string urlDecode(const std::string& s) {
    std::string result;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size()) {
            int c;
            sscanf(s.substr(i + 1, 2).c_str(), "%x", &c);
            result += static_cast<char>(c);
            i += 2;
        } else if (s[i] == '+') {
            result += ' ';
        } else {
            result += s[i];
        }
    }
    return result;
}

// ===== JSON 转义 =====
inline std::string escapeJson(const std::string& s) {
    std::string out;
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;
        }
    }
    return out;
}

// ===== SHA256 流式计算（需要 OpenSSL） =====
// 见 StreamHash.hpp

// ===== 流式拷贝 =====
inline bool copyFileStreaming(const std::string& srcPath, const std::string& dstPath) {
    std::ifstream src(srcPath, std::ios::binary);
    std::ofstream dst(dstPath, std::ios::binary);
    if (!src || !dst) return false;
    char buf[1024 * 1024];
    while (src.read(buf, sizeof(buf)) || src.gcount() > 0)
        dst.write(buf, src.gcount());
    return dst.good();
}


}
}


















