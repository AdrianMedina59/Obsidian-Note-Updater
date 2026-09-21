#pragma once
#include <string>
#include <nlohmann/json.hpp>
#include <asio.hpp>
#include <thread>
#include <atomic>
#include <mutex>
#include <memory>
#include "storage/snapshot.hpp"

class SyncNode{
 public:
    SyncNode(unsigned short local_port, const std::string& remote_ip, unsigned short remote_port, SnapshotEngine& snapshot_engine);
    ~SyncNode();

    //starting background network threads
    void start();

    //stoping all operations
    void stop();

    //sending a message over an active connection
    void send_message(const nlohmann::json& message);

    //helper function to send the current vault index to the peer
    void send_vault_index();

private:
    //background exection loops
    void listen_loop();
    void receive_loop(std::shared_ptr<asio::ip::tcp::socket> socket);
    void reconcile_remote_index(const nlohmann::json& remote_payload);
    void handle_file_request(const std::string& relative_path);
    void handle_incoming_payload(const nlohmann::json& remote_payload);

    unsigned short local_port_;
    std::string  remote_ip_;
    unsigned short remote_port_;

    SnapshotEngine& snapshot_engine_; // Reference to local file state

    asio::io_context io_context_;
    std::thread listener_thread_;
    std::thread sender_thread_;
    std::atomic<bool> is_running_{false};
    
    //active outbound connection storage
    std::mutex socket_mutex_;
    std::shared_ptr<asio::ip::tcp::socket> active_socket_;

    //serializes asio::write calls: several threads send frames on the same socket
    std::mutex write_mutex_;

};