#pragma once


#include "storage/IDateStorage.hpp"
#include "storage/IMetaStorage.hpp"
#include <cstddef>
#include <leveldb/db.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/slice.h>
#include <leveldb/status.h>
#include <leveldb/write_batch.h>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <iostream>

// Key 设计（双索引，职责分离）：
//
//   p:{path}{name} → 完整 MetaInfo JSON
//     用户视角的独立副本。张三和李四各有一份，互不覆盖。
//     示例: p:/张三/照片/海.jpg → {"file_name":"海.jpg","file_hash":"abc",...}
//
//   h:{hash} → 物理信息 JSON（精简版）
//     只存物理属性 + 引用计数。所有路径共享同一份。
//     示例: h:abc → {"file_size":12345,"content_type":"image/jpeg","ref_count":2}
//

namespace miniKV {
namespace storage {

class FileMetaStorage : public IMetaStorage {
public:
    explicit FileMetaStorage(const std::string& dbPath, IDateStorage* dataSorage) : dbPath_(dbPath), 
                            dataSorage_(dataSorage) {}

    //不在构造函数里面写，就是因为这个时候
    bool init() override {
        leveldb::Options opts;
        opts.create_if_missing = true;
        leveldb::DB* ptr = nullptr;
        leveldb::Status s = leveldb::DB::Open(opts, dbPath_, &ptr);
        if(!s.ok()) return false;
        
        db_.reset(ptr);
        return true;
    }

    bool saveMeta(const std::string& key, const MetaInfo& meta) override {
        std::string cleanPath = normalizePath(meta.file_path);
        std::string pathKey   = makePathKey(cleanPath, meta.file_name);
        std::string hashKey   = makeHashKey(key);

        leveldb::WriteBatch batch;

        batch.Put(pathKey, toJson(meta));

        std::string existingPhys;
        if(db_->Get(leveldb::ReadOptions(), hashKey, &existingPhys).ok())
        {
            //存在
            PhysInfo phys = physFromJson(existingPhys);
            phys.ref_count++;
            batch.Put(hashKey, physToJson(phys));
        }
        else {
            PhysInfo phys;
            phys.file_size      = meta.file_size;
            phys.content_type   = meta.content_type;
            phys.ref_count      = 1;
            batch.Put(hashKey, physToJson(phys));
        }
        //保证原子性
        std::cout << "【写入检查】PathKey: " << pathKey << "  Hash: " << key << std::endl;
        leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
        return s.ok();
    }

    bool getMetaByPath(const std::string& path, const std::string& name, MetaInfo& meta) override
    {
        std::string cleanPath = normalizePath(path);
        std::string pathKey   = makePathKey(cleanPath, name);
        std::string json;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(), pathKey, &json);
        if(!s.ok()) return false;
        meta = fromJson(json);
        return true;
    }

    // 兼容旧接口：按 hash 删除（不推荐，请用 removeByPath）
    bool removeMate(const std::string& key) override {
        PhysInfo phys;
        if (!getPhysInfo(key, phys)) return false;

        leveldb::WriteBatch batch;
        batch.Delete(makeHashKey(key));
        // 注意：旧接口不清理 p: 索引，建议外部用 removeByPath
        leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
        return s.ok();
    }

    bool getMeta(const std::string& key, MetaInfo& meta) override
    {
        PhysInfo phys;
        if(!getPhysInfo(key, phys)) return false;

        meta.file_hash    = key;
        meta.file_size    = phys.file_size;
        meta.content_type = phys.content_type;
        meta.file_path    = "";
        meta.file_name    = "";
        return true;
    } 

    bool removeByPath(const std::string& path, const std::string& name) override {
        std::string cleanPath = normalizePath(path);
        std::string pathKey   = makePathKey(cleanPath, name);

        std::string metaJson;
        leveldb::Status s = db_->Get(leveldb::ReadOptions(), pathKey, &metaJson);
        if(!s.ok()) return false;

        MetaInfo meta = fromJson(metaJson);
        

        PhysInfo phys;
        if(!getPhysInfo(meta.file_hash, phys)) return false;
        std::string HashKey = makeHashKey(meta.file_hash);
        leveldb::WriteBatch batch;
        batch.Delete(pathKey);
        if(phys.ref_count <= 1)
        {
            batch.Delete(HashKey);
            dataSorage_->removeByHash(meta.file_hash);
        }
        else {
            phys.ref_count--;
            batch.Put(HashKey, physToJson(phys));
        }
        s = db_->Write(leveldb::WriteOptions(), &batch);
        return s.ok();
    }

     // ================================================================
    // 移动文件（路径间迁移）
    // ================================================================
    bool moveFile(const std::string& hash, const std::string& newPath) override {
        // 注意：moveFile 现在是"移动一个特定的路径条目"
        // 使用 removeByPath + 重新 save 的组合
        // 但为了原子性，最好传 oldPath + oldName + newPath + newName

        // 兼容旧接口：从 h: 查物理信息，但无法确定 oldPath
        // V2 建议新增 moveByPath(oldPath, oldName, newPath, newName)
        PhysInfo phys;
        if (!getPhysInfo(hash, phys)) return false;

        // 退回：无法确定源路径，moveFile 应该用 moveByPath 替代
        return false;  // 标记为 deprecated
    }

    bool moveByPath(const std::string& oldPath, const std::string& oldName, const std::string& newPath,
        const std::string& newName) override {
            MetaInfo meta;
            if(!getMetaByPath(oldPath, oldName, meta)) return false;
            
            std::string oldCleanPath = normalizePath(oldPath);
            std::string newCleanName = normalizePath(newPath);

            meta.file_path = newCleanName;
            meta.file_name = newName;

            leveldb::WriteBatch batch;
            batch.Delete(makePathKey(oldCleanPath, oldName));
            batch.Put(makePathKey(newCleanName, newName), toJson(meta));
            leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
            return s.ok();
        }

    //Return true iff "x" is a prefix of "*this"
    std::vector<MetaInfo> listByPath(const std::string& path) const override {
        std::string cleanPath = normalizePath(path);
        std::string prefix = makePathPrefix(cleanPath);
        std::vector<MetaInfo> result;

        auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));
        //条件就是这个valid在迭代器内，还有这个x就是查找内容是这个key的前缀
        for(it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next())
        {
            MetaInfo m = fromJson(it->value().ToString());
            if (m.file_path == cleanPath) {  // 精确匹配，排除子目录内的文件
                result.push_back(std::move(m));
            }
        }
        return result;
    }

    std::vector<std::string> listSubDirs(const std::string& path) const override {
        std::string prefix = makePathPrefix(normalizePath(path));
        std::set<std::string> dirs;

        auto it = std::unique_ptr<leveldb::Iterator>(db_->NewIterator(leveldb::ReadOptions()));

        for(it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next())
        {
            std::string rest = it->key().ToString().substr(prefix.size());
            size_t slash = rest.find('/');
            if(slash != std::string::npos)
                dirs.insert(rest.substr(0, slash));
        }
        return std::vector<std::string>(dirs.begin(), dirs.end());
    }

    std::vector<MetaInfo> listAll() const override {
        return listByPath("/");  // 根目录下所有文件
    }

    bool isHashReferenced(const std::string& hash) {
        PhysInfo phys;
        return getPhysInfo(hash, phys) && phys.ref_count > 0;
    }

    int getRefCount(const std::string& hash) {
        PhysInfo phys;
        if(!getPhysInfo(hash, phys)) return 0;
        return phys.ref_count;
    }


private:

    //工具函数
    //属于类本身，可以不用实例来调用，没有 this 指针：普通函数内部隐藏了一个 this 指针，指向当前的对象；静态函数没有 this，所以它不能访问类里的普通成员变量
    static std::string makeHashKey(const std::string& hash)
    {
        return "h:" + hash;
    }

    /*
    Key 格式: h:{文件内容的Hash}
    Value 格式: 整个文件的详情 JSON（大小、创建者、真实物理路径、AI 标签）。
    设计目的：
    Key 格式: p:{标准化路径}:{文件名}
    Value 格式: 对应的 Hash 字符串（指向 h: 索引的指针）。
    */
    static std::string makePathKey(const std::string& path, const std::string& name)
    {
        return "p:" + path + name;
    }

    static std::string makePathPrefix(const std::string& path)
    {
        return "p:" + path;
    }

    //强制转换成以 / 开头、以 / 结尾的标准格式。
    //为了统一文件路径，还有搜索前缀
    static std::string normalizePath(const std::string& p)
    {
        std::string s = p;
        if(s.empty()) return "/";
        if(s[0] != '/') s = "/" + s;                  // 确保开头有 /
        while(s.size() > 1 && s.back() == '/') s.pop_back(); // 去末尾 /
        size_t pos;                                    // 合并多重 ///
        while((pos = s.find("//")) != std::string::npos)
            s.replace(pos, 2, "/");
        if(s.empty()) return "/";
        if(s == "/") return "/";                       // 根目录不加双斜杠
        return s + "/";
    }

    std::vector<MetaInfo> batchGetMeta(const std::vector<std::string>& hashes) const {
        std::vector<MetaInfo> result;

        for(const auto& hash : hashes)
        {
            std::string json;
            leveldb::Status s = db_->Get(leveldb::ReadOptions(), makeHashKey(hash), &json);
            if(s.ok())
            {
                result.push_back(fromJson(json));
            }
        }
        return result;
    }


private:
    struct PhysInfo {
        size_t file_size = 0;
        std::string content_type;
        int    ref_count = 0; //引用计数
    };

    bool getPhysInfo(const std::string& hash, PhysInfo& phys) {
        std::string json;

        leveldb::Status s = db_->Get(leveldb::ReadOptions(), makeHashKey(hash), &json);
        if(!s.ok()) return false;
        phys = physFromJson(json);
        return true;
    }


    std::string physToJson(const PhysInfo& phys) const {
        return "{"
            "\"file_size\":"    + std::to_string(phys.file_size) + ","
            "\"content_type\":\"" + esc(phys.content_type) + "\","
            "\"ref_count\":"    + std::to_string(phys.ref_count) +
        "}";
    }

    PhysInfo physFromJson(const std::string& json) const {
        PhysInfo p;
        p.file_size    = getNum(json, "file_size");
        p.content_type = getStr(json, "content_type");
        p.ref_count    = (int)getNum(json, "ref_count");
        return p;  // ← 之前少了这行，UB！
    }

    //JSON序列化, const 把this变成const指针
    // ========== JSON 序列化（和 FileMetaStorage 相同）==========

    std::string toJson(const MetaInfo& meta) const {
        return "{"
            "\"file_name\":\""    + esc(meta.file_name)    + "\","
            "\"file_hash\":\""    + esc(meta.file_hash)    + "\","
            "\"file_path\":\""    + esc(meta.file_path)    + "\","
            "\"file_author\":\""  + esc(meta.file_author)  + "\","
            "\"file_time\":\""    + esc(meta.file_time)    + "\","
            "\"file_size\":"      + std::to_string(meta.file_size) + ","
            "\"content_type\":\"" + esc(meta.content_type) + "\""
        "}";
    }

    MetaInfo fromJson(const std::string& json) const {
        MetaInfo m;
        m.file_name    = getStr(json, "file_name");
        m.file_hash    = getStr(json, "file_hash");
        m.file_path    = getStr(json, "file_path");
        m.file_author  = getStr(json, "file_author");
        m.file_time    = getStr(json, "file_time");
        m.file_size    = getNum(json, "file_size");
        m.content_type = getStr(json, "content_type");
        return m;
    }
    //"f":"" 所以要加3开始
    std::string getStr(const std::string& j, const std::string& f) const {
        size_t p = j.find('"' + f + '"'); if (p == std::string::npos) return "";
        p = j.find('"', p + f.size() + 3); if (p == std::string::npos) return "";
        size_t e = j.find('"', p + 1); if (e == std::string::npos) return "";
        return unesc(j.substr(p + 1, e - p - 1));
    }
    //"f":1024 所以要加2开始
    size_t getNum(const std::string& j, const std::string& f) const {
        size_t p = j.find('"' + f + '"'); if (p == std::string::npos) return 0;
        p = j.find(':', p + f.size() + 2); if (p == std::string::npos) return 0;
        size_t n = 0;
        while (++p < j.size() && j[p] >= '0' && j[p] <= '9') n = n * 10 + (j[p] - '0');
        return n;
    }

    std::string esc(const std::string& s) const {
        std::string o;
        for (char c: s) {
            switch (c) {
                //JSON 标准规定，字符串内部的每一个双引号，前面必须加一个反斜杠 \ 来转义。
                case '"': o+="\\\""; break; case '\\': o+="\\\\"; break;
                case '\n':o+="\\n"; break;  case '\r': o+="\\r"; break;
                case '\t':o+="\\t"; break;  default: o+=c;
            }
        }
        return o;
    }

    std::string unesc(const std::string& s) const {
        std::string o;
        for (size_t i=0; i<s.size(); ++i) {
            if (s[i]=='\\' && i+1<s.size()) {
                switch (s[i+1]) {
                    case '"': o+='"'; break; case '\\':o+='\\'; break;
                    case 'n': o+='\n';break; case 'r': o+='\r'; break;
                    case 't': o+='\t';break; default:  o+=s[i+1];
                }
                ++i;
            } else o+=s[i];
        }
        return o;
    }

private:

    IDateStorage* dataSorage_;
    //局部变量
    std::string dbPath_;
    std::unique_ptr<leveldb::DB> db_;
};

}
}























