cmake_minimum_required(VERSION 3.10)
project(xdelta)

set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall -fPIC")

add_library(xdelta STATIC xdelta3.cpp)