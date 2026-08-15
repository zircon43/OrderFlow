cmake_minimum_required(VERSION 3.10)
project(OrderFlowCore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -pthread -march=native")

# Source files
set(SOURCES
    main.cpp
    lob.cpp
    engine.cpp
    tcp_server.cpp
    pubsub_server.cpp
)

# Executable
add_executable(matching_engine ${SOURCES})
target_link_libraries(matching_engine pthread)
