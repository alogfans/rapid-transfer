cmake_minimum_required(VERSION 3.16)
project(rapid CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-O3 -g -Wall -static-libstdc++")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(include)
include_directories(src)
include_directories(thirdparty/include)
link_directories(thirdparty/lib)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(example)
