cmake_minimum_required(VERSION 3.20)
project(BPTree)

set(CMAKE_CXX_STANDARD 11)
include_directories(
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src
)
file(GLOB all_SRCS
        "${PROJECT_SOURCE_DIR}/include/*.hpp"
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        )
add_executable(BPTree ${all_SRCS})