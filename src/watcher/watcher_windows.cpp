#ifdef _WIN32
#include "watcher/file_watcher.hpp"
#include <windows.h>
#include <chrono>
#include <iostream>

FileWatcher::FileWatcher(const std::string& path, std::function<void()> change_callback)
    : path_to_watch_(path), callback_(change_callback) {}

FileWatcher::~FileWatcher() { stop(); }

void FileWatcher::start() {
    std::cout << "[DEBUG_WATCHER] Initializing Win32 File Monitoring on: " << path_to_watch_ << "\n";

    HANDLE dir_handle = CreateFileA(
        path_to_watch_.c_str(), FILE_LIST_DIRECTORY,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL
    );

    if (dir_handle == INVALID_HANDLE_VALUE) {
        std::cerr << "[DEBUG_WATCHER_ERR] Win32 failed to lock target vault path handle (code "
                  << GetLastError() << ").\n";
        handle_ = nullptr; // keep the "null means no handle" invariant stop() relies on
        return;
    }

    // INVALID_HANDLE_VALUE is (HANDLE)-1, not null, so it must never be stored:
    // every guard below is a plain null check.
    handle_ = dir_handle;
    is_running_ = true;
    watch_thread_ = std::thread(&FileWatcher::watch_loop, this);
}

void FileWatcher::stop() {
    if (!is_running_) return;
    is_running_ = false;

    // Order matters: cancel the pending read so the loop returns, join the thread,
    // and only then close the handle. Closing first would let watch_loop touch a
    // freed (possibly recycled) handle while it is still running.
    if (handle_) CancelIoEx(handle_, NULL);
    if (watch_thread_.joinable()) watch_thread_.join();

    if (handle_) {
        CloseHandle(handle_);
        handle_ = nullptr;
    }

    std::cout << "[DEBUG_WATCHER] Win32 watcher stopped cleanly.\n";
}

void FileWatcher::watch_loop() {
    char buffer[4096];
    DWORD bytes_returned = 0;

    while (is_running_) {
        if (!ReadDirectoryChangesW(
                handle_, buffer, sizeof(buffer), TRUE,
                FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_LAST_WRITE,
                &bytes_returned, NULL, NULL)) {

            // A cancelled read during stop() is expected; anything else is a genuine
            // failure, and retrying it would spin this thread at full speed.
            if (is_running_) {
                std::cerr << "[DEBUG_WATCHER_ERR] ReadDirectoryChangesW failed (code "
                          << GetLastError() << "); halting watcher.\n";
            }
            break;
        }

        if (bytes_returned == 0) {
            // Buffer overflow: the change set was too large to report. Individual
            // filenames are lost, so treat it as a blanket "something changed".
            std::cout << "[DEBUG_WATCHER] Win32 change buffer overflowed; forcing a full re-index.\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            callback_();
            continue;
        }

        FILE_NOTIFY_INFORMATION* file_info = reinterpret_cast<FILE_NOTIFY_INFORMATION*>(buffer);
        bool should_trigger = false;

        do {
            std::wstring wide_name(file_info->FileName, file_info->FileNameLength / sizeof(WCHAR));
            std::string filename(wide_name.begin(), wide_name.end());

            if (filename.find(".md") != std::string::npos && filename.find(".tmp") == std::string::npos) {
                std::cout << "[DEBUG_WATCHER] Win32 event caught on file: " << filename << "\n";
                should_trigger = true;
            }
            file_info = file_info->NextEntryOffset ?
                reinterpret_cast<FILE_NOTIFY_INFORMATION*>(reinterpret_cast<char*>(file_info) + file_info->NextEntryOffset) : nullptr;
        } while (file_info);

        if (should_trigger) {
            // Debounce settling interval
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            callback_();
        }
    }
}
#endif
