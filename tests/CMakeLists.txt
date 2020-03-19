cmake_minimum_required(VERSION 3.10)

project(mydolphin)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(mydolphin tests.cpp)

target_include_directories(mydolphin PUBLIC
    "${PROJECT_SOURCE_DIR}/../include"
    "${PROJECT_SOURCE_DIR}/../thirdparty/include"
)

find_package(MySQL REQUIRED)
