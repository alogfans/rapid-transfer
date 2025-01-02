cmake_minimum_required(VERSION 3.16)
project(rapid CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-O3 -g -Wall -static-libstdc++")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(CONFIG_MCAST "option for enable multicast" OFF)
if (CONFIG_MCAST)
  add_compile_definitions(CONFIG_MCAST)
  message(STATUS "multicast is enabled")
endif()

include_directories(include)
include_directories(src)
include_directories(/usr/include/jsoncpp)

#include_directories(thirdparty/include)
#link_directories(thirdparty/lib)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(example)
