/**
 * Test runner entry point.
 *
 * All test TUs (test_storage, test_parser, test_executor) register their
 * TEST() blocks automatically via static constructors.
 * main() just invokes run_all_tests(), which returns 0 on full pass or 1
 * on any failure (compatible with CMake CTest / CI).
 */
#include "test_framework.hpp"

int main() {
    return run_all_tests();
}
