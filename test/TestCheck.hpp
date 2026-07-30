#pragma once

#include <cstdio>
#include <cstdlib>

inline void minikvTestCheck(bool condition, const char* expression, int line)
{
    if(condition) return;
    std::fprintf(stderr, "FAIL:%d: %s\n", line, expression);
    std::exit(1);
}

#define MINIKV_CHECK(expression) \
    minikvTestCheck(static_cast<bool>(expression), #expression, __LINE__)
