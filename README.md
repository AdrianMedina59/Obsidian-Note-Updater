# PeerSync: Cross-Platform P2P Sync Engine for Obsidian

A high-performance, real-time peer-to-peer file synchronization engine written in **Modern C++ (C++20)**. PeerSync automatically bridges and harmonizes Markdown vaults between **Windows 10 (Win32)** and **Ubuntu Linux (POSIX)** filesystems over a custom bi-directional network transport layer without relying on third-party cloud servers.

## 🚀 Core Systems Architecture

PeerSync splits execution path workloads into isolated, thread-safe domain modules:

*   **Transport Layer (Asio Standalone):** A multi-threaded, symmetric bi-directional network pipeline utilizing length-prefixed framing layouts to bypass packet bleeding over TCP socket channels.
*   **State Analysis Engine (std::filesystem):** Generates deterministic, fast cryptographic content hashes (FNV-1a streaming hashes) to scan structural state catalogs and run differential index comparisons.
*   **OS-Native File Monitors:** Real-time, platform-dependent event watchers leveraging native kernel subsystems (**POSIX `inotify`** on Ubuntu and **Win32 `ReadDirectoryChangesW`** on Windows 10) featuring debounced event-throttling.
*   **Atomic Storage Commit Pipeline:** Implements atomic write staging buffers (`.tmp` allocations) before execution swap renames (`std::filesystem::rename`) to completely prevent target file corruption during transmission.

## 📊 Custom Framing Protocol

To optimize raw packet streaming and enforce packet boundary alignments, data payloads utilize a strict length-prefixed protocol sequence:

┌───────────────────────────┬──────────────────────────────────────────┐
│  Header (4-Byte uint32_t) │           Body (Variable Length)          │
├───────────────────────────┼──────────────────────────────────────────┤
│ Payload Size in Bytes     │ Serialized Structural JSON Data Payload   │
└───────────────────────────┴──────────────────────────────────────────┘

### Protocol Message Signatures:
*   `MSG_VAULT_INDEX`: Streams local catalog catalogs recursively.
*   `MSG_FILE_REQ`: Dispatches explicit pull-requests for out-of-date assets.
*   `MSG_FILE_PAYLOAD`: Delivers text blocks down to atomic storage layers.

## 🛠️ Build & Installation Dependencies

### Prerequisites
*   CMake (v3.22+)
*   Ninja or Make Build Tools
*   Modern C++20 Compiler (GCC 11+, Clang 13+, or MSVC 2022+)

The project automatically handles external dependencies (`nlohmann_json` and Standalone `Asio`) at configuration runtime via CMake's `FetchContent` module.

### Compiling on Ubuntu Linux / WSL2
```bash
mkdir build && cd build
cmake -G Ninja ..
ninja
```

### Compiling on Windows 10 (Developer Command Prompt)
```cmd
mkdir build && cd build
cmake -G "Visual Studio 17 2022" ..
cmake --build . --config Release
```

## 💻 Technical Implementation Highlights
*   **Race Condition Mitigation:** Thread-safe state isolation utilizing `std::mutex` and custom scoped thread-local socket resource retention handles (`keeper` idioms) to completely avoid socket dangling or accidental deallocation crash vectors.
*   **Decoupled Worker Pools:** Offloads heavy data reconciliation processing onto detached thread paths to keep the core socket read loops unblocked, preventing TCP window buffer overflows.
*   **Content Shield Filters:** Explicit runtime regex/string exclusions targeting hidden `.obsidian/` application settings to shield nodes from infinite metadata ping-pong loops.
