file(GLOB SRC_FILES *.cpp protocols/*.cpp protocols/*/*.cpp)
add_library(rapid ${SRC_FILES})
target_link_libraries(rapid PUBLIC ibverbs rdmacm glog gflags pthread jsoncpp numa unwind)