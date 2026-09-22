#pragma once

#include <string>
#include <functional> 
#include <thread>
#include <atomic>

class FileWatcher{
    public:
        //takes the target vault path and a callback to trigger when changes happen
        FileWatcher(const std::string& path, std::function<void()> change_callback);
        ~FileWatcher();

        void start();
        void stop();


    private:
        void watch_loop();

        std::string path_to_watch_;
        std::function<void()> callback_;
        std::thread watch_thread_;
        std::atomic<bool> is_running_{false};

        #ifdef _WIN32
            void* handle_ = nullptr; //Map to Win32 Handle
        #else
            int inotify_fd_ = -1;
            int watch_fd_ = -1;
            int wake_fd_ = -1; //eventfd stop() uses to break the reader out of poll()
        #endif 
};