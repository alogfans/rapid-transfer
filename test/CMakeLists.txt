add_executable(e2e_test e2e_test.cpp)
target_link_libraries(e2e_test PUBLIC rapid)

add_executable(perf_test perf_test.cpp)
target_link_libraries(perf_test PUBLIC rapid)
