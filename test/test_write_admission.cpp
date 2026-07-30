#include "DataNode/WriteAdmission.hpp"

#include <iostream>

namespace {

bool check(bool condition, const char* expression, int line) {
    if (condition) return true;
    std::cerr << "FAIL:" << line << ": " << expression << '\n';
    return false;
}

#define CHECK(expression) \
    do { if (!check((expression), #expression, __LINE__)) return 1; } while (false)

}  // namespace

int main() {
    miniKV::datanode::WriteAdmission admission(2);

    CHECK(admission.limit() == 2);
    CHECK(admission.tryAcquire());
    CHECK(admission.tryAcquire());
    CHECK(admission.active() == 2);
    CHECK(!admission.tryAcquire());

    admission.release();
    CHECK(admission.active() == 1);
    CHECK(admission.tryAcquire());
    CHECK(admission.active() == 2);

    admission.release();
    admission.release();
    CHECK(admission.active() == 0);
    std::cout << "PASS: DataNode write admission enforces its fixed limit\n";
    return 0;
}
