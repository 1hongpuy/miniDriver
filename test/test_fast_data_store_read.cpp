#include "DataNode/FastDataStore.hpp"

#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::filesystem::path directory =
        std::filesystem::temp_directory_path() / "minikv_fast_data_store_read_test";
    std::error_code error;
    std::filesystem::remove_all(directory, error);

    const std::string hash =
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    const std::string bytes = "abc";

    bool alreadyExists = false;
    {
        miniKV::datanode::FastDataStore store(directory.string());
        if (!store.open() || !store.put(hash, bytes, alreadyExists) || alreadyExists) {
            std::cerr << "FAIL: cannot persist known SHA-256 chunk\n";
            return 1;
        }

        std::string read;
        if (!store.get(hash, read)) {
            std::cerr << "FAIL: persisted chunk is not readable by its correct SHA-256\n";
            return 1;
        }
        if (read != bytes) {
            std::cerr << "FAIL: read bytes differ from written bytes\n";
            return 1;
        }
    }

    std::filesystem::remove_all(directory, error);
    std::cout << "PASS: FastDataStore reads a chunk verified by its SHA-256\n";
    return 0;
}
