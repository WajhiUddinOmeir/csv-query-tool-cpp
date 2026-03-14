cmake_minimum_required(VERSION 3.16)
project(csv-query-tool LANGUAGES CXX)

# ── Language standard ──────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Shared library sources (everything except main.cpp) ───────────────────────
set(LIB_SOURCES
    src/CsvQueryEngine.cpp

    # Storage layer
    src/storage/Schema.cpp
    src/storage/CsvTable.cpp

    # Parser layer
    src/parser/Lexer.cpp
    src/parser/SQLParser.cpp

    # Executor layer
    src/executor/ExpressionEvaluator.cpp
    src/executor/JoinExecutor.cpp
    src/executor/QueryExecutor.cpp

    # Util layer
    src/util/TablePrinter.cpp
)

# ── Main executable ────────────────────────────────────────────────────────────
add_executable(csv-query-tool src/main.cpp ${LIB_SOURCES})
target_include_directories(csv-query-tool PRIVATE src)

# ── Test executable ────────────────────────────────────────────────────────────
set(TEST_SOURCES
    ${LIB_SOURCES}
    tests/test_main.cpp
    tests/test_storage.cpp
    tests/test_parser.cpp
    tests/test_executor.cpp
)

add_executable(csv-query-tool-tests ${TEST_SOURCES})
target_include_directories(csv-query-tool-tests PRIVATE src tests)

# ── CTest integration ──────────────────────────────────────────────────────────
enable_testing()
add_test(NAME all_tests COMMAND csv-query-tool-tests)

# ── Compiler warnings ─────────────────────────────────────────────────────────
if(MSVC)
    target_compile_options(csv-query-tool        PRIVATE /W3)
    target_compile_options(csv-query-tool-tests  PRIVATE /W3)
else()
    target_compile_options(csv-query-tool        PRIVATE -Wall -Wextra -Wpedantic)
    target_compile_options(csv-query-tool-tests  PRIVATE -Wall -Wextra -Wpedantic)
endif()

# ── Optional: copy sample-data next to the binary ─────────────────────────────
add_custom_command(TARGET csv-query-tool POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
        "${CMAKE_SOURCE_DIR}/sample-data"
        "$<TARGET_FILE_DIR:csv-query-tool>/sample-data"
    COMMENT "Copying sample-data to build directory"
)

# ── Install ───────────────────────────────────────────────────────────────────
install(TARGETS csv-query-tool DESTINATION bin)
install(DIRECTORY sample-data DESTINATION .)
