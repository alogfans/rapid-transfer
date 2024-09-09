add_executable(file_transfer file_transfer.cpp)
target_link_libraries(file_transfer PUBLIC rapid)

add_executable(benchmark benchmark.cpp)
target_link_libraries(benchmark PUBLIC rapid)