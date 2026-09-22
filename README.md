# PeerSync: Cross-Platform P2P Sync Engine for Obsidian

A high-performance, real-time peer-to-peer file synchronization engine written in **Modern C++ (C++20)**. PeerSync automatically bridges and harmonizes Markdown vaults between **Windows 10 (Win32)** and **Ubuntu Linux (POSIX)** filesystems over a custom bi-directional network transport layer without relying on third-party cloud servers.

## 🚀 Core Systems Architecture

PeerSync splits execution path workloads into isolated, thread-safe domain modules:

*   **Transport Layer (Asio Standalone):** A multi-threaded, symmetric bi-directional network pipeline utilizing length-prefixed framing layouts to bypass packet bleeding over TCP socket channels.
*   **State Analysis Engine (std::filesystem):** Generates deterministic, fast cryptographic content hashes (FNV-1a streaming hashes) to scan structural state catalogs and run differential index comparisons.
*   **OS-Native File Monitors:** Real-time, platform-dependent event watchers leveraging native kernel subsystems (**POSIX `inotify`** on Ubuntu and **Win32 `ReadDirectoryChangesW`** on Windows 10) featuring debounced event-throttling.
*   **Atomic Storage Commit Pipeline:** Implements atomic write staging buffers (`.tmp` allocations) before execution swap renames (`std::filesystem::rename`) to completely prevent target file corruption during transmission.

