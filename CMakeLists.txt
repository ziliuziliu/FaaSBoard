# CMakeLists.txt

# Set the minimum required CMake version
cmake_minimum_required(VERSION 3.22)

include(CMakePrintHelpers)

# Define the project and its language
project(faasboard CXX)

# Find OpenMP
find_package(OpenMP)
find_package(aws-lambda-runtime)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Include cmake module
include(CMakePrintHelpers)

# Specify include directories
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/third-party/aws-lambda-app/include)

add_definitions("-Wall -g")

# Create an executable target
add_executable(naive_binpack ${CMAKE_SOURCE_DIR}/test/preprocess/naive_binpack.cpp)
add_executable(naive_cyclic ${CMAKE_SOURCE_DIR}/test/preprocess/naive_cyclic.cpp)
add_executable(naive_stagger ${CMAKE_SOURCE_DIR}/test/preprocess/naive_stagger.cpp)
add_executable(optimal_binpack ${CMAKE_SOURCE_DIR}/test/preprocess/optimal_binpack.cpp)
add_executable(row ${CMAKE_SOURCE_DIR}/test/preprocess/row.cpp)
add_executable(column ${CMAKE_SOURCE_DIR}/test/preprocess/column.cpp)
add_executable(mondriaan_row_column ${CMAKE_SOURCE_DIR}/test/preprocess/mondriaan_row_column.cpp)
add_executable(mondriaan_column_row ${CMAKE_SOURCE_DIR}/test/preprocess/mondriaan_column_row.cpp)
add_executable(cpu_test ${CMAKE_SOURCE_DIR}/test/others/cpu_test.cpp)
add_executable(weight_test ${CMAKE_SOURCE_DIR}/test/others/weight_test.cpp)

# Link OpenMP
target_link_libraries(naive_binpack PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(naive_cyclic PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(naive_stagger PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(optimal_binpack PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(row PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(column PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(mondriaan_row_column PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(mondriaan_column_row PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(cpu_test PRIVATE OpenMP::OpenMP_CXX AWS::aws-lambda-runtime) 
target_link_libraries(weight_test PRIVATE OpenMP::OpenMP_CXX)

# create zip
aws_lambda_package_target(cpu_test)