#include "network/sync_node.hpp"
#include <fstream>
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
                                    reconcile_remote_index(parsed_payload);
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

            std::string msg_type = parsed_payload.value("type", "");

            if(msg_type == "MSG_VAULT_INDEX"){
                reconcile_remote_index(parsed_payload);
            }
            else if(msg_type == "MSG_FILE_REQ"){
                //peer wahts a file from us. Read it from disk and trasnmit it
                std::string requested_path = parsed_payload["path"];
                handle_file_request(parsed_payload);
            }
            else if(msg_type == "MSG_FILE_PAYLOAD"){
                //incoming file data streaming in from peer. Write it safely
                handle_incoming_payload(parsed_payload);
            }
            
        }
    }
    catch (...) {
        std::cout << "[Session] Peer disconnected.\n";
    }
}



void SyncNode::reconcile_remote_index(const nlohmann::json& remote_payload) {
    snapshot_engine_.generate_snapshot();
    auto local_snapshot = snapshot_engine_.get_snapshot();

    std::cout << "[Sync] Reconciling indices and issuing pull requests...\n";

    for (const auto& file : remote_payload["files"]) {
        std::string path = file["path"];
        std::string r_hash = file["hash"];
        int64_t r_mtime = file["mtime"];

        bool request_needed = false;

        if (local_snapshot.find(path) == local_snapshot.end()) {
            std::cout << " -> Pulling Missing File: " << path << "\n";
            request_needed = true;
        } else {
            const auto& local_meta = local_snapshot[path];
            if (local_meta.content_hash != r_hash && r_mtime > local_meta.last_modified) {
                std::cout << " -> Pulling Outdated File (Remote is newer): " << path << "\n";
                request_needed = true;
            }
        }

        if (request_needed) {
            nlohmann::json req_payload = {
                {"type", "MSG_FILE_REQ"},
                {"path", path}
            };
            send_message(req_payload);
        }
    }
}

void SyncNode::handle_file_request(const std::string& relative_path)
{
    //reconstruct canonical absolute path using host filesystem anchor
    fs::path full_path = fs::path(snapshot_engine_.get_vault_path()) / relative_path;

    if(!fs::exists(full_path) || !fs::is_regular_file(full_path)){
        std::cerr << "[TRANSFER ERROR] Denied pull request for non-existent assest: " << relative_path << "\n";
        return;
    }

    std::ifstream file(full_path, std::ios::binary);
    if(!file)return;

    //read raw content into a string buffer
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    nlohmann::json payload = {
        {"type", "MSG_FILE_PAYLOAD"},
        {"path", relative_path},
        {"content", content}
    };

  std::cout << "[Transfer] Uploading: " << relative_path << " (" << content.size() << " bytes)\n";
  send_message(payload);
}

void SyncNode::handle_incoming_payload(const nlohmann::json& payload)
{
    std::string relative_path = payload["path"];
    std::string content = payload["content"];

    fs::path target_path = fs::path(snapshot_engine_.get_vault_path()) / relative_path;

    //ensure nested parent directories exist locally before attempting file creation
    fs::create_directory(target_path.parent_path());

    //1. implement atomic write - output contents to a temp staging workspace
    fs::path tmp_path = target_path;
    tmp_path.replace_extension(target_path.extension().string() + ".tmp");

    std::ofstream out_file(tmp_path, std::ios::binary);
    if(!out_file){
        std::cerr << "[ID ERROR] Failed tp open temp files for write: " << tmp_path << "\n";
        return; 
    }
    out_file.write(content.data(), content.size());
    out_file.close();

    //2. Atomic rename operation swaps staging file over target notes cleanly
    try{
        fs::rename(tmp_path, target_path);
        std::cout << "[SYNC SUCCESS] Successfully synced and updated file: " << relative_path << "\n";
    }catch(const std::exception& e){
        std::cerr << "[IO ERROR] Atomic replacement commit failed: " << e.what() << "\n";
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