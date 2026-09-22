cmake_minimum_required(VERSION 3.22)
project(PeerSync VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

# Enable strict warning flags to keep development clean
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wall -Wextra -Wpedantic -Wshadow -O2)
elseif(MSVC)
    add_compile_options(/W4 /O2)
endif()

# -------------------------------------------------------------------
# Dependency Management via FetchContent (Bypassing URL Parsing)
# -------------------------------------------------------------------
include(FetchContent)

# 1. Asio (Standalone Networking using Git)
FetchContent_Declare(
    asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio
    GIT_TAG        asio-1-30-2
    GIT_SHALLOW    ON
)
FetchContent_MakeAvailable(asio)

# 2. nlohmann/json (Metadata Serialization using Git)
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json
    GIT_TAG        v3.11.3
    GIT_SHALLOW    ON
)
FetchContent_MakeAvailable(json)

# -------------------------------------------------------------------
# Target Configuration
# -------------------------------------------------------------------
set(SRC_FILES
    src/main.cpp
    src/network/sync_node.cpp
    src/storage/snapshot.cpp
)

# The two watcher backends are mutually exclusive; each guards itself with
# _WIN32 internally, but only the matching one is compiled to keep the other
# platform's system headers out of the build entirely.
if(WIN32)
    list(APPEND SRC_FILES src/watcher/watcher_windows.cpp)
else()
    list(APPEND SRC_FILES src/watcher/watcher_linux.cpp)
endif()

add_executable(peer_sync ${SRC_FILES})

# Link dependencies and set include paths
target_include_directories(peer_sync PRIVATE 
    include
    ${asio_SOURCE_DIR}/asio/include
)

# Header-only Asio requires manual macro definition when standalone
target_compile_definitions(peer_sync PRIVATE ASIO_STANDALONE)

# Link FetchContent targets
target_link_libraries(peer_sync PRIVATE nlohmann_json::nlohmann_json)

# OS-Specific system libraries for socket networking
if(WIN32)
    target_link_libraries(peer_sync PRIVATE ws2_32 mswsock)
else()
    target_link_libraries(peer_sync PRIVATE pthread)
endif()
