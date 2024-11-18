# CMakeLists.txt

# Set the minimum required CMake version
cmake_minimum_required(VERSION 3.22)

include(CMakePrintHelpers)

# Define the project and its language
project(faasboard CXX)

set(SERVICE_COMPONENTS s3)

# Find OpenMP
find_package(OpenMP REQUIRED)
find_package(aws-lambda-runtime REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS ${SERVICE_COMPONENTS})
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -O3 -g -fno-strict-aliasing -mavx2")

# Include cmake module
include(CMakePrintHelpers)

# Specify include directories
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/third-party/aws-lambda-app/include)

add_definitions("-Wall -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=~/lambda-install -DGLOG_USE_GLOG_EXPORT")

# Create an executable target
add_executable(preprocess_and_save ${CMAKE_SOURCE_DIR}/test/preprocess/preprocess_and_save.cpp)
add_executable(local_bfs ${CMAKE_SOURCE_DIR}/test/local_bfs.cpp)
add_executable(local_pr ${CMAKE_SOURCE_DIR}/test/local_pr.cpp)
add_executable(lambda_bfs ${CMAKE_SOURCE_DIR}/test/lambda_bfs.cpp)
add_executable(lambda_pr ${CMAKE_SOURCE_DIR}/test/lambda_pr.cpp)
add_executable(meta_server ${CMAKE_SOURCE_DIR}/test/meta_server.cpp)
add_executable(proxy_server ${CMAKE_SOURCE_DIR}/test/proxy_server.cpp)
add_executable(correctness_test ${CMAKE_SOURCE_DIR}/test/others/correctness_test.cpp)

# Link
target_link_libraries(preprocess_and_save OpenMP::OpenMP_CXX glog::glog gflags ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(local_bfs OpenMP::OpenMP_CXX glog::glog gflags ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(local_pr OpenMP::OpenMP_CXX glog::glog gflags ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(lambda_bfs OpenMP::OpenMP_CXX glog::glog gflags AWS::aws-lambda-runtime ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(lambda_pr OpenMP::OpenMP_CXX glog::glog gflags AWS::aws-lambda-runtime ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(meta_server OpenMP::OpenMP_CXX glog::glog gflags ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(proxy_server OpenMP::OpenMP_CXX glog::glog gflags ${AWSSDK_LINK_LIBRARIES})
target_link_libraries(correctness_test OpenMP::OpenMP_CXX glog::glog gflags ${AWSSDK_LINK_LIBRARIES})

# create zip
aws_lambda_package_target(lambda_bfs)
aws_lambda_package_target(lambda_pr)