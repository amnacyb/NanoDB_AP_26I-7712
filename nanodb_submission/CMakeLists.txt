cmake_minimum_required(VERSION 3.14)
project(NanoDB VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags
if(MSVC)
    add_compile_options(/W4 /O2)
else()
    add_compile_options(-Wall -Wextra -O2 -Wpedantic -Wno-unused-parameter)
endif()

include_directories(${CMAKE_SOURCE_DIR})

# Main NanoDB executable
add_executable(nanodb main.cpp)
target_link_libraries(nanodb m)

# Automated Test Runner
add_executable(test_runner test_runner.cpp)
target_link_libraries(test_runner m)

# Ensure data directory exists
add_custom_command(TARGET nanodb POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/data
    COMMENT "Creating data directory"
)

# Copy queries.txt to build directory
add_custom_command(TARGET test_runner POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/queries.txt ${CMAKE_BINARY_DIR}/queries.txt
    COMMENT "Copying queries.txt"
)

message(STATUS "NanoDB build configured.")
message(STATUS "  Run: cmake --build . && ./test_runner data queries.txt")
