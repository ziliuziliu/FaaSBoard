# CMakeLists.txt

# Set the minimum required CMake version
cmake_minimum_required(VERSION 3.22)

include(CMakePrintHelpers)

# Define the project and its language
project(faasboard CXX)

# Find OpenMP
find_package(OpenMP)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Include cmake module
include(CMakePrintHelpers)

# Specify include directories
include_directories(${CMAKE_SOURCE_DIR}/include)

add_definitions("-Wall -g")

# Create an executable target
add_executable(naive_binpack ${CMAKE_SOURCE_DIR}/test/preprocess/naive_binpack.cpp)
add_executable(naive_cyclic ${CMAKE_SOURCE_DIR}/test/preprocess/naive_cyclic.cpp)
add_executable(naive_stagger ${CMAKE_SOURCE_DIR}/test/preprocess/naive_stagger.cpp)
add_executable(optimal_binpack ${CMAKE_SOURCE_DIR}/test/preprocess/optimal_binpack.cpp)
add_executable(row ${CMAKE_SOURCE_DIR}/test/preprocess/row.cpp)
add_executable(column ${CMAKE_SOURCE_DIR}/test/preprocess/column.cpp)

# Link OpenMP
target_link_libraries(naive_binpack PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(naive_cyclic PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(naive_stagger PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(optimal_binpack PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(row PRIVATE OpenMP::OpenMP_CXX)
target_link_libraries(column PRIVATE OpenMP::OpenMP_CXX)
