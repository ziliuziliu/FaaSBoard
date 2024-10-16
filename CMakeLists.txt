# CMakeLists.txt

# Set the minimum required CMake version
cmake_minimum_required(VERSION 3.22)

include(CMakePrintHelpers)

# Define the project and its language
project(faasboard CXX)

# Find OpenMP
find_package(OpenMP)
# find_package(aws-lambda-runtime)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")

# Include cmake module
include(CMakePrintHelpers)

# Specify include directories
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/third-party/aws-lambda-app/include)

add_definitions("-Wall -g -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=~/lambda-install -DGLOG_USE_GLOG_EXPORT")

# Create an executable target
add_executable(preprocess_and_save ${CMAKE_SOURCE_DIR}/test/preprocess/preprocess_and_save.cpp)
add_executable(test_bfs ${CMAKE_SOURCE_DIR}/test/test_bfs.cpp)
add_executable(meta_server ${CMAKE_SOURCE_DIR}/test/meta_server.cpp)
add_executable(proxy_server ${CMAKE_SOURCE_DIR}/test/proxy_server.cpp)
add_executable(correctness_test ${CMAKE_SOURCE_DIR}/test/others/correctness_test.cpp)

# Link
target_link_libraries(preprocess_and_save PRIVATE OpenMP::OpenMP_CXX glog::glog gflags)
target_link_libraries(test_bfs PRIVATE OpenMP::OpenMP_CXX glog::glog gflags)
target_link_libraries(meta_server PRIVATE OpenMP::OpenMP_CXX glog::glog gflags)
target_link_libraries(proxy_server PRIVATE OpenMP::OpenMP_CXX glog::glog gflags)
target_link_libraries(correctness_test PRIVATE OpenMP::OpenMP_CXX glog::glog gflags)

# create zip
# aws_lambda_package_target(cpu_test)