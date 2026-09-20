#include "network/sync_node.hpp"
#include <iostream>

SyncNode::SyncNode(unsigned short local_port, const std::string& remote_ip, unsigned short remote_port, SnapshotEngine& snapshot_engine)
    : local_port_(local_port), remote_ip_(remote_ip), remote_port_(remote_port),  snapshot_engine_(snapshot_engine) {}

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


void SyncNode::send_vault_index(){
    //regen snapshot right before trasnmitting to ensure accuracy 
    snapshot_engine_.generate_snapshot();
    const auto& local_snapshot = snapshot_engine_.get_snapshot();

    nlohmann::json index_payload = {
        {"type", "MSG_VAULT_INDEX"},
        {"files", nlohmann::json::array()}
    };

    for(const auto& [path, meta] : local_snapshot){
        nlohmann::json file_meta = {
            {"path", meta.relative_path},
            {"size", meta.file_size},
            {"hash", meta.content_hash},
            {"mtime", meta.last_modified}
        };
        index_payload["files"].push_back(file_meta);
    }

    std::cout << "[TRANSPORT] Transmitting local vault catolog map to peer ....\n";
    send_message(index_payload);
}



void SyncNode::listen_loop() {
    try {
        asio::ip::tcp::acceptor acceptor(io_context_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), local_port_));
        std::cout << "[Server] Listening on port " << local_port_ << "...\n";

        while (is_running_) {
            asio::ip::tcp::socket socket(io_context_);
            acceptor.accept(socket); 
            std::cout << "[Server] Connection received from: " << socket.remote_endpoint() << "\n";
            
            // --- NEW: Promote the accepted socket to handle outbound traffic if none exists ---
            {
                std::lock_guard<std::mutex> lock(socket_mutex_);
                if (!outbound_socket_ || !outbound_socket_->is_open()) {
                    // Clone/move the socket context safely before handing off to receive loop
                    outbound_socket_ = std::make_unique<asio::ip::tcp::socket>(std::move(socket));
                    std::cout << "[Server] Promoted inbound socket to bi-directional transport pipe!\n";
                    
                    // Spawn receiver thread using our newly tracked shared pointer
                    std::thread([this]() {
                        try {
                            while (is_running_) {
                                uint32_t payload_length = 0;
                                // Read from the shared outbound socket pointer
                                {
                                    std::lock_guard<std::mutex> lock(socket_mutex_);
                                    if(!outbound_socket_) break;
                                    asio::read(*outbound_socket_, asio::buffer(&payload_length, sizeof(payload_length)));
                                }

                                std::vector<char> buffer(payload_length);
                                {
                                    std::lock_guard<std::mutex> lock(socket_mutex_);
                                    asio::read(*outbound_socket_, asio::buffer(buffer.data(), payload_length));
                                }

                                std::string raw_json(buffer.begin(), buffer.end());
                                auto parsed_payload = nlohmann::json::parse(raw_json);

                                if (parsed_payload.contains("type") && parsed_payload["type"] == "MSG_VAULT_INDEX") {
                                    reconcile_remote_idex(parsed_payload);
                                }
                            }
                        } catch (...) {
                            std::cout << "[Session] Peer disconnected from bi-directional channel.\n";
                        }
                    }).detach();
                    
                    continue; // Skip standard detachment logic since we handled it
                }
            }

            // Fallback for secondary connections
            std::thread(&SyncNode::receive_loop, this, std::move(socket)).detach();
        }
    }
    catch (const std::exception& e) {
        if (is_running_) std::cerr << "[Server Error] " << e.what() << "\n";
    }
}


void SyncNode::receive_loop(asio::ip::tcp::socket socket) {
    try {
        // Wrap local socket instance into a shared state
        auto shared_sock = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
        while (is_running_) {
            uint32_t payload_length = 0;
            asio::read(*shared_sock, asio::buffer(&payload_length, sizeof(payload_length)));

            std::vector<char> buffer(payload_length);
            asio::read(*shared_sock, asio::buffer(buffer.data(), payload_length));

            std::string raw_json(buffer.begin(), buffer.end());
            auto parsed_payload = nlohmann::json::parse(raw_json);

            if (parsed_payload.contains("type") && parsed_payload["type"] == "MSG_VAULT_INDEX") {
                reconcile_remote_idex(parsed_payload);
            }
        }
    }
    catch (...) {
        std::cout << "[Session] Peer disconnected.\n";
    }
}



void SyncNode::reconcile_remote_idex(const nlohmann::json& remote_payload)
{
    std::cout << "\n [RECONCILIATION STAGE] Analyzing remote Vault map...\n";

    //regen local index to ensure we compare fresh data
    snapshot_engine_.generate_snapshot();
    auto local_snapshot = snapshot_engine_.get_snapshot();

    std::unordered_map<std::string, nlohmann::json> remote_files;
    for(const auto& file : remote_payload["files"])
    {
        remote_files[file["path"]] = file;
    }

    std::cout << "Discovered Discrepancies....\n";

    //1. check for the files that are modified or completely missing locally
    for(const auto& [path, remote_meta] : remote_files){
        std::string r_hash = remote_meta["hash"];
        int64_t r_mtime = remote_meta["mtime"];

        if(local_snapshot.find(path) == local_snapshot.end()){
            std::cout << " [MISSING LOCALLY] File need to be downloaded: " << path << "\n";
        }
        else{
            const auto& local_meta = local_snapshot[path];
            if(local_meta.content_hash != r_hash){
                //content mismastch detected 
                if(r_mtime > local_meta.last_modified){
                    std::cout << " [OUTDATED LOCALLY] Remote copy is newer. Update " << path << "\n";
                }
                else{
                    std::cout << "[NEWER LOCALLY] local copy is newer. Peer needs update: " << path << "\n";
                }
            }
            //remove from local tracking map so remaining entries are marked as unique
            local_snapshot.erase(path);
        }
    }

    //2. anything remainining is local_snapshot doesn't exist on the remote peer 
    for(const auto& [path, local_meta] : local_snapshot){
        std::cout << "[UNIQUE TO LOCAL] Local files needs to be updated: " << path << "\n";
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