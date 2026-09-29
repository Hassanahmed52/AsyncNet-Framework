cmake_minimum_required(VERSION 3.10)
project(AsyncNetFramework)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add include directory
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Find ASIO (header-only library)
# Try different locations based on OS
if(APPLE)
    # macOS with Homebrew
    include_directories(/opt/homebrew/include)
    include_directories(/usr/local/include)
elseif(UNIX)
    # Linux
    include_directories(/usr/include)
endif()

# ASIO is also available via: vcpkg, manual download, or system package manager
# If ASIO is not found in default locations, download from: https://think-async.com/Asio/

# Find system libraries (for threading)
find_package(Threads REQUIRED)

# Server executable
add_executable(NetworkServer examples/SimpleServer.cpp)
target_link_libraries(NetworkServer PRIVATE Threads::Threads)

# Client executable  
add_executable(NetworkClient examples/SimpleClient.cpp)
target_link_libraries(NetworkClient PRIVATE Threads::Threads)

# Platform-specific setup
if(WIN32)
    target_link_libraries(NetworkServer PRIVATE ws2_32 wsock32)
    target_link_libraries(NetworkClient PRIVATE ws2_32 wsock32)
elseif(UNIX AND NOT APPLE)
    # Linux specific
elseif(APPLE)
    # macOS specific
endif()

# Set output directory
set_target_properties(NetworkServer PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set_target_properties(NetworkClient PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

message(STATUS "AsyncNet Framework")
message(STATUS "Server: NetworkServer")
message(STATUS "Client: NetworkClient")
