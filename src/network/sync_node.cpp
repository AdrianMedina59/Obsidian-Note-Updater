#include "network/sync_node.hpp"
#include <iostream>

SyncNode::SyncNode(unsigned short local_port, const std::string& remote_ip, unsigned short remote_port)
    : local_port_(local_port), remote_ip_(remote_ip), remote_port_(remote_port)  {}

SyncNode::~SyncNode(){
    stop();
}

void SyncNode::start(){
    is_running_ = true;

    //launching server thread using blocking network patterns
    listener_thread_ = std::thread(&SyncNode::listen_loop, this);


    //launch active connection outreach thread
    sender_thread_ = std::thread([this](){
        asio::ip::tcp::resolver resolver(io_context_);
        while(is_running_)
        {
            try{
                auto endpoints = resolver.resolve(remote_ip_, std::to_string(remote_port_));
                auto sock = std::make_unique<asio::ip::tcp::socket>(io_context_);

                std::cout << "[CLIENT] Attempting connection to " << remote_ip_ << ":" << remote_port_ << "...\n";
                asio::connect(*sock, endpoints);

                std::cout << "[CLIENT] Successfully connected to remote peer!\n";
                {
                    std::lock_guard<std::mutex> lock(socket_mutex_);
                    outbound_socket_ = std::move(sock);
                }
                break;
            }catch(const std::exception& e){
                //retry loop backoff to prevent CPU pegging
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
        }
    });
}

void SyncNode::stop(){
    if(!is_running_) return;

    is_running_ = false;

    io_context_.stop();

    if(listener_thread_.joinable()) listener_thread_.join();
    if(sender_thread_.joinable()) sender_thread_.join();

    std::cout << "[SYSTEM] Network transport infrastructure shutdown cleanly.\n";
}

void SyncNode::listen_loop(){
    try{
        asio::ip::tcp::acceptor acceptor(io_context_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), local_port_));
        std::cout << "[SERVER] Listening on port " << local_port_ << "...\n";

        while(is_running_){
            asio::ip::tcp::socket socket(io_context_);
            acceptor.accept(socket); //Blocks until a peer arrives
            std::cout << "[SERVER] Connection received from: " << socket.remote_endpoint() << "\n";

            //Delegate session tracking to a seperate detached worker thread
            std::thread(&SyncNode::receive_loop, this, std::move(socket)).detach();
        }

    }catch(const std::exception e ){
        if(is_running_){
            std::cerr << "[Server Error] Exception encountered: " << e.what() << "\n";
        }
    }
}

void SyncNode::receive_loop(asio::ip::tcp::socket socket){
    try{
        while(is_running_){
            //read 4-byte header specifying length payload constraint
            uint32_t payload_length = 0;
            asio::read(socket, asio::buffer(&payload_length, sizeof(payload_length)));

            std::vector<char> buffer(payload_length);
            asio::read(socket, asio::buffer(buffer.data(), payload_length));

            std::string raw_json(buffer.begin(), buffer.end());
            auto parsed_payload = nlohmann::json::parse(raw_json);

            std::cout << "[RECEIVED NODE EVENT] Header Content: " << parsed_payload.dump(2) << "\n";
        }
    }catch(const std::exception& e){
        std::cout << "[SESSION] peer disconnected or connection timed out.\n";
    }
}

void SyncNode::send_message(const nlohmann::json& message){
    std::lock_guard<std::mutex> lock(socket_mutex_);
    if(!outbound_socket_ || !outbound_socket_->is_open()){
        std::cerr << "[TRANSPORT ERROR] Target peer socket pipline unavailable.\n";
        return;
    }

    try{
        std::string serialized = message.dump();
        uint32_t length = static_cast<uint32_t>(serialized.size());

        //Send length prefix first, followed by raw JSON data
        std::vector<asio::const_buffer> buffers;
        buffers.push_back(asio::buffer(&length, sizeof(length)));
        buffers.push_back(asio::buffer(serialized.data(), length));

        asio::write(*outbound_socket_, buffers);

    }catch(const std::exception& e){
        std::cerr << "[TRANSMIT ERROR] Message serialization delivery failure: " << e.what() << "\n";
    }

}