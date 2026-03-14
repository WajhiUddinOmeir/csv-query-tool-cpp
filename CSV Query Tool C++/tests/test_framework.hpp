#pragma once
/**
 * Minimal zero-dependency test framework for CSV Query Tool C++.
 *
 * Usage:
 *   TEST("description") { ASSERT_EQ(2+2, 4); }
 *   int main() { return run_all_tests(); }
 */

#include <iostream>
#include <string>
#include <vector>
#include <functional>
#include <sstream>
#include <cmath>
#include <stdexcept>

// ── ANSI colour codes ─────────────────────────────────────────
// Modern Windows 10+ terminals support VT sequences natively; no
// windows.h needed.  Older terminals just show the raw escape codes.

static const char* CLR_GREEN  = "\033[32m";
static const char* CLR_RED    = "\033[31m";
static const char* CLR_YELLOW = "\033[33m";
static const char* CLR_BOLD   = "\033[1m";
static const char* CLR_RESET  = "\033[0m";

// ── Test registry ─────────────────────────────────────────────
struct TestCase {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& test_registry() {
    static std::vector<TestCase> reg;
    return reg;
}

struct TestRegistrar {
    TestRegistrar(const char* name, std::function<void()> fn) {
        test_registry().push_back({name, std::move(fn)});
    }
};

// ── Assertion helpers ─────────────────────────────────────────

struct TestFailure : std::exception {
    std::string msg;
    explicit TestFailure(std::string m) : msg(std::move(m)) {}
    const char* what() const noexcept override { return msg.c_str(); }
};

// Stream helper — enums are cast to int; everything else is streamed directly
template<typename T>
std::string _val_to_str(const T& v) {
    if constexpr (std::is_enum_v<T>) {
        return std::to_string(static_cast<long long>(v));
    } else {
        std::ostringstream ss;
        ss << v;
        return ss.str();
    }
}

template<typename A, typename B>
inline void _assert_eq(const A& a, const B& b,
                       const char* exprA, const char* exprB,
                       const char* file, int line)
{
    if (!(a == b)) {
        std::ostringstream ss;
        ss << "ASSERT_EQ failed at " << file << ":" << line << "\n"
           << "  Expected: " << exprA << " == " << exprB << "\n"
           << "  Left:     " << _val_to_str(a) << "\n"
           << "  Right:    " << _val_to_str(b);
        throw TestFailure(ss.str());
    }
}

template<typename A, typename B>
inline void _assert_ne(const A& a, const B& b,
                       const char* exprA, const char* exprB,
                       const char* file, int line)
{
    if (a == b) {
        std::ostringstream ss;
        ss << "ASSERT_NE failed at " << file << ":" << line << "\n"
           << "  Expected: " << exprA << " != " << exprB << "\n"
           << "  Value:    " << _val_to_str(a);
        throw TestFailure(ss.str());
    }
}

inline void _assert_true(bool cond, const char* expr, const char* file, int line) {
    if (!cond) {
        std::ostringstream ss;
        ss << "ASSERT_TRUE failed at " << file << ":" << line << "\n"
           << "  Expression: " << expr;
        throw TestFailure(ss.str());
    }
}

inline void _assert_false(bool cond, const char* expr, const char* file, int line) {
    if (cond) {
        std::ostringstream ss;
        ss << "ASSERT_FALSE failed at " << file << ":" << line << "\n"
           << "  Expression: " << expr << " (expected false, got true)";
        throw TestFailure(ss.str());
    }
}

inline void _assert_near(double a, double b, double eps,
                         const char* exprA, const char* exprB,
                         const char* file, int line)
{
    if (std::abs(a - b) > eps) {
        std::ostringstream ss;
        ss << "ASSERT_NEAR failed at " << file << ":" << line << "\n"
           << "  Expected: |" << exprA << " - " << exprB << "| <= " << eps << "\n"
           << "  Left: " << a << "  Right: " << b
           << "  Diff: " << std::abs(a - b);
        throw TestFailure(ss.str());
    }
}

template<typename Fn>
inline void _assert_throws(Fn fn, const char* expr, const char* file, int line) {
    bool threw = false;
    try { fn(); } catch (...) { threw = true; }
    if (!threw) {
        std::ostringstream ss;
        ss << "ASSERT_THROWS failed at " << file << ":" << line << "\n"
           << "  Expected exception from: " << expr;
        throw TestFailure(ss.str());
    }
}

template<typename Fn>
inline void _assert_no_throw(Fn fn, const char* expr, const char* file, int line) {
    try { fn(); }
    catch (const std::exception& e) {
        std::ostringstream ss;
        ss << "ASSERT_NO_THROW failed at " << file << ":" << line << "\n"
           << "  Unexpected exception from: " << expr << "\n"
           << "  what(): " << e.what();
        throw TestFailure(ss.str());
    }
}

// ── Public macros ─────────────────────────────────────────────
#define ASSERT_EQ(a, b)    _assert_eq((a),(b),#a,#b,__FILE__,__LINE__)
#define ASSERT_NE(a, b)    _assert_ne((a),(b),#a,#b,__FILE__,__LINE__)
#define ASSERT_TRUE(e)     _assert_true((e),#e,__FILE__,__LINE__)
#define ASSERT_FALSE(e)    _assert_false((e),#e,__FILE__,__LINE__)
#define ASSERT_NEAR(a,b,e) _assert_near((double)(a),(double)(b),(e),#a,#b,__FILE__,__LINE__)
#define ASSERT_THROWS(fn)  _assert_throws([&](){ fn; },#fn,__FILE__,__LINE__)
#define ASSERT_NO_THROW(fn) _assert_no_throw([&](){ fn; },#fn,__FILE__,__LINE__)
#define ASSERT_NULL(v)     ASSERT_TRUE(::isNull(v))
#define ASSERT_NOT_NULL(v) ASSERT_FALSE(::isNull(v))

// Two-level indirection so __LINE__ expands before token-pasting
#define _TC_(a, b) a##b
#define _TC(a, b) _TC_(a, b)

// Register a test (block follows the macro)
#define TEST(name) \
    static void _TC(_test_body_, __LINE__)(); \
    static TestRegistrar _TC(_reg_, __LINE__)(name, _TC(_test_body_, __LINE__)); \
    static void _TC(_test_body_, __LINE__)()

// ── Test runner ───────────────────────────────────────────────
inline int run_all_tests() {
    int passed = 0, failed = 0;
    const auto& tests = test_registry();

    std::cout << CLR_BOLD << "\n=== CSV Query Tool C++ Tests (" << tests.size()
              << " tests) ===\n" << CLR_RESET;

    for (const auto& t : tests) {
        try {
            t.fn();
            std::cout << CLR_GREEN << "  PASS" << CLR_RESET << "  " << t.name << "\n";
            ++passed;
        } catch (const TestFailure& f) {
            std::cout << CLR_RED << "  FAIL" << CLR_RESET << "  " << t.name << "\n"
                      << CLR_YELLOW << "       " << f.msg << CLR_RESET << "\n";
            ++failed;
        } catch (const std::exception& e) {
            std::cout << CLR_RED << "  FAIL" << CLR_RESET << "  " << t.name << "\n"
                      << CLR_YELLOW << "       Unexpected exception: " << e.what()
                      << CLR_RESET << "\n";
            ++failed;
        } catch (...) {
            std::cout << CLR_RED << "  FAIL" << CLR_RESET << "  " << t.name << "\n"
                      << CLR_YELLOW << "       Unknown exception" << CLR_RESET << "\n";
            ++failed;
        }
    }

    std::cout << CLR_BOLD << "\n--- Results: "
              << CLR_GREEN << passed << " passed" << CLR_RESET
              << CLR_BOLD  << ", ";
    if (failed > 0)
        std::cout << CLR_RED << failed << " failed";
    else
        std::cout << CLR_GREEN << "0 failed";
    std::cout << CLR_RESET << CLR_BOLD << " (" << tests.size() << " total) ---\n\n" << CLR_RESET;

    return failed > 0 ? 1 : 0;
}
