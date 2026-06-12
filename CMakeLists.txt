cmake_minimum_required(VERSION 3.10)
project(SharedMemoryIPC CXX)

set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_compile_options(-Wall -Wextra -O2)

find_package(Threads REQUIRED)

add_executable(producer producer.cpp)
# target_link_libraries(producer PRIVATE rt Threads::Threads) # use on Linux if needed

add_executable(consumer consumer.cpp)
# target_link_libraries(consumer PRIVATE rt Threads::Threads) # use on Linux if needed