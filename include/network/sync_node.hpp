#pragma once
#include <string>
#include <nlohmann/json.hpp>
#include <asio.hpp>
#include <thread>
#include <atomic>

class SyncNode{
 public:
    SyncNode(unsigned short local_port, const std::string& remote_ip, unsigned short remote_port);
    ~SyncNode();

    //starting background network threads
    void start();

    //stoping all operations
    void stop();

    //sending a message over an active connection
    void send_message(const nlohmann::json& message);

private:
    //background exection loops
    void listen_loop();
    void receive_loop(asio::ip::tcp::socket socket);

    unsigned short local_port_;
    std::string  remote_ip_;
    unsigned short remote_port_;

    asio::io_context io_context_;
    std::thread listener_thread_;
    std::thread sender_thread_;
    std::atomic<bool> is_running_{false};
    
    //active outbound connection storage
    std::mutex socket_mutex_;
    std::unique_ptr<asio::ip::tcp::socket> outbound_socket_;

};