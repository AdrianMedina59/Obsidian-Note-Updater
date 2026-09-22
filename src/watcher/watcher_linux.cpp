#ifndef _WIN32
#include "watcher/file_watcher.hpp"
#include <unistd.h>
#include <iostream>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/inotify.h>

FileWatcher::FileWatcher(const std::string& path, std::function<void()> change_callback)
    : path_to_watch_(path), callback_(change_callback) {}

FileWatcher::~FileWatcher() { stop(); }


void FileWatcher::start() {
    std::cout << "[DEBUG_WATCHER] Initializing Linux inotify on: " << path_to_watch_ << "\n";
    inotify_fd_ = inotify_init();
    if (inotify_fd_ < 0) {
        std::cerr << "[DEBUG_WATCHER_ERR] Failed to initialize inotify: "
                  << std::strerror(errno) << "\n";
        return;
    }

    watch_fd_ = inotify_add_watch(inotify_fd_, path_to_watch_.c_str(), IN_MODIFY | IN_CREATE | IN_DELETE);
    if (watch_fd_ < 0) {
        std::cerr << "[DEBUG_WATCHER_ERR] Failed to add inotify watch: "
                  << std::strerror(errno) << "\n";
        close(inotify_fd_);
        inotify_fd_ = -1;
        return;
    }

    // An eventfd used purely as a doorbell: stop() writes to it so the reader
    // leaves poll() on purpose, rather than being closed out from under itself.
    wake_fd_ = eventfd(0, EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        std::cerr << "[DEBUG_WATCHER_ERR] Failed to create wakeup eventfd: "
                  << std::strerror(errno) << "\n";
        inotify_rm_watch(inotify_fd_, watch_fd_);
        close(inotify_fd_);
        inotify_fd_ = -1;
        watch_fd_ = -1;
        return;
    }

    is_running_ = true;
    watch_thread_ = std::thread(&FileWatcher::watch_loop, this);
}



void FileWatcher::stop() {
    if (!is_running_) return;
    is_running_ = false;

    // Order matters: ring the doorbell, join the reader, and only then release the
    // descriptors. Closing first would leave watch_loop parked in poll() on a dead
    // fd -- closing a descriptor does not wake a thread already blocked on it -- and
    // the join below would never return.
    if (wake_fd_ >= 0) {
        uint64_t signal_value = 1;
        ssize_t written = write(wake_fd_, &signal_value, sizeof(signal_value));
        if (written != static_cast<ssize_t>(sizeof(signal_value))) {
            std::cerr << "[DEBUG_WATCHER_ERR] Could not signal watcher shutdown: "
                      << std::strerror(errno) << "\n";
        }
    }

    if (watch_thread_.joinable()) watch_thread_.join();

    if (inotify_fd_ >= 0 && watch_fd_ >= 0) inotify_rm_watch(inotify_fd_, watch_fd_);
    if (inotify_fd_ >= 0) close(inotify_fd_);
    if (wake_fd_ >= 0) close(wake_fd_);
    inotify_fd_ = -1;
    watch_fd_ = -1;
    wake_fd_ = -1;

    std::cout << "[DEBUG_WATCHER] Linux watcher stopped cleanly.\n";
}

void FileWatcher::watch_loop() {
    char buffer[4096] __attribute__ ((aligned(__alignof__(struct inotify_event))));

    while (is_running_) {
        struct pollfd fds[2];
        fds[0].fd = inotify_fd_;
        fds[0].events = POLLIN;
        fds[0].revents = 0;
        fds[1].fd = wake_fd_;
        fds[1].events = POLLIN;
        fds[1].revents = 0;

        if (poll(fds, 2, -1) < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[DEBUG_WATCHER_ERR] poll() failed: " << std::strerror(errno)
                      << "; halting watcher.\n";
            break;
        }

        if (fds[1].revents & POLLIN) break; // stop() rang the doorbell
        if (!(fds[0].revents & POLLIN)) continue;

        ssize_t len = read(inotify_fd_, buffer, sizeof(buffer));
        if (len <= 0) {
            if (len < 0 && (errno == EINTR || errno == EAGAIN)) continue;
            // A read failure here would otherwise spin this thread at full speed.
            if (is_running_) {
                std::cerr << "[DEBUG_WATCHER_ERR] inotify read failed: " << std::strerror(errno)
                          << "; halting watcher.\n";
            }
            break;
        }

        ssize_t i = 0;
        bool should_trigger = false;
        while (i < len) {
            struct inotify_event* event = reinterpret_cast<struct inotify_event*>(&buffer[i]);

            if (event->mask & IN_Q_OVERFLOW) {
                // The kernel queue overflowed and individual events were dropped,
                // so no filename survived. Re-index everything instead.
                std::cout << "[DEBUG_WATCHER] inotify queue overflowed; forcing a full re-index.\n";
                should_trigger = true;
            }
            else if (event->len > 0) {
                std::string name(event->name);
                // Only monitor markdown files; ignore settings and temp files
                if (name.find(".md") != std::string::npos && name.find(".tmp") == std::string::npos) {
                    std::cout << "[DEBUG_WATCHER] Linux event caught on file: " << name << "\n";
                    should_trigger = true;
                }
            }
            i += static_cast<ssize_t>(sizeof(struct inotify_event) + event->len);
        }

        if (should_trigger) {
            // Debounce: Wait for Obsidian to finish flushing data before building the snapshot
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            callback_();
        }
    }
}

#endif
