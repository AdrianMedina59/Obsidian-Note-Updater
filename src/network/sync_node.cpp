#include "network/sync_node.hpp"
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <algorithm>


namespace {

// --- Debug instrumentation -------------------------------------------------
// Enabled by default. Silence with SYNC_DEBUG=0 in the environment.
std::mutex g_log_mutex;

bool debug_enabled() {
    static const bool enabled = [] {
        const char* v = std::getenv("SYNC_DEBUG");
        return !(v && std::string(v) == "0");
    }();
    return enabled;
}

int64_t uptime_ms() {
    static const auto start = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
}

std::string thread_tag() {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    return oss.str();
}

void dbg(const std::string& stage, const std::string& detail) {
    if (!debug_enabled()) return;
    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << "[DBG " << std::setw(7) << uptime_ms() << "ms thr:" << thread_tag()
              << "] " << stage << " | " << detail << "\n" << std::flush;
}

// Serialized plain output so concurrent threads cannot garble each other's lines.
void say(const std::string& line) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << line << "\n" << std::flush;
}

void complain(const std::string& line) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cerr << line << std::endl;
}

std::string hex_dump(const void* data, size_t len) {
    const auto* bytes = static_cast<const unsigned char*>(data);
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        oss << std::setw(2) << static_cast<int>(bytes[i]) << ' ';
    }
    oss << "| ascii: ";
    for (size_t i = 0; i < len; ++i) {
        bool printable = bytes[i] >= 32 && bytes[i] < 127;
        oss << (printable ? static_cast<char>(bytes[i]) : '.');
    }
    return oss.str();
}

} // namespace


SyncNode::SyncNode(unsigned short local_port, const std::string& remote_ip,
                     unsigned short remote_port, SnapshotEngine& snapshot_engine)
    : local_port_(local_port), remote_ip_(remote_ip), remote_port_(remote_port),
      snapshot_engine_(snapshot_engine) {}


SyncNode::~SyncNode(){
    stop();
}

void SyncNode::start() {
    is_running_ = true;
    dbg("START", "local_port=" + std::to_string(local_port_) +
                 " remote=" + remote_ip_ + ":" + std::to_string(remote_port_) +
                 " vault=" + snapshot_engine_.get_vault_path());

    listener_thread_ = std::thread(&SyncNode::listen_loop, this);

    sender_thread_ = std::thread([this]() {
        asio::ip::tcp::resolver resolver(io_context_);
        while (is_running_) {
            {
                std::lock_guard<std::mutex> lock(socket_mutex_);
                if (active_socket_ && active_socket_->is_open()) {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                    continue;
                }
            }
            try {
                auto endpoints = resolver.resolve(remote_ip_, std::to_string(remote_port_));
                auto sock = std::make_shared<asio::ip::tcp::socket>(io_context_);

                asio::connect(*sock, endpoints);

                {
                    std::lock_guard<std::mutex> lock(socket_mutex_);
                    if (!active_socket_ || !active_socket_->is_open()) {
                        active_socket_ = sock;
                        say("[Client] Established outbound channel to peer!");
                        dbg("DIAL", "outbound socket promoted, remote=" +
                                    sock->remote_endpoint().address().to_string() + ":" +
                                    std::to_string(sock->remote_endpoint().port()));
                        std::thread(&SyncNode::receive_loop, this, active_socket_).detach();
                    } else {
                        dbg("DIAL", "outbound connect succeeded but an inbound channel is already active; discarding");
                    }
                }
                break;
            }
            catch (const std::exception& e) {
                dbg("DIAL", std::string("connect attempt failed, retrying in 3s: ") + e.what());
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
            catch (...) {
                dbg("DIAL", "connect attempt failed (unknown), retrying in 3s");
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
        }
        dbg("DIAL", "dialer thread exiting");
    });
}

void SyncNode::stop() {
    if (!is_running_) return;
    dbg("STOP", "shutting down node");
    is_running_ = false;
    io_context_.stop();
    if (listener_thread_.joinable()) listener_thread_.join();
    if (sender_thread_.joinable()) sender_thread_.join();
    dbg("STOP", "node threads joined");
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

    say("[TRANSPORT] Transmitting local vault catolog map to peer ....");
    dbg("INDEX-OUT", "cataloged " + std::to_string(local_snapshot.size()) + " local files");
    send_message(index_payload);
}




void SyncNode::listen_loop() {
    try {
        asio::ip::tcp::acceptor acceptor(io_context_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), local_port_));
        dbg("LISTEN", "acceptor bound on port " + std::to_string(local_port_));
        while (is_running_) {
            auto socket = std::make_shared<asio::ip::tcp::socket>(io_context_);
            acceptor.accept(*socket);

            std::lock_guard<std::mutex> lock(socket_mutex_);
            if (!active_socket_ || !active_socket_->is_open()) {
                active_socket_ = socket;
                say("[Server] Accepted inbound connection and promoted to active channel!");
                dbg("LISTEN", "inbound peer " + socket->remote_endpoint().address().to_string() + ":" +
                              std::to_string(socket->remote_endpoint().port()));

                std::thread(&SyncNode::receive_loop, this, active_socket_).detach();
            } else {
                // Already have a working socket connection, close this duplicate gracefully
                say("[Server] Duplicate handshake ignored. Dropping connection link safely.");
                dbg("LISTEN", "duplicate inbound dropped; an active channel already exists");
                socket->close();
            }
        }
    } catch (const std::exception& e) {
        dbg("LISTEN", std::string("acceptor loop ended: ") + e.what());
    } catch (...) {
        dbg("LISTEN", "acceptor loop ended (unknown exception)");
    }
}


void SyncNode::receive_loop(std::shared_ptr<asio::ip::tcp::socket> socket) {
    // Hold a local reference to the socket within this thread's frame scope
    // to ensure it cannot be destroyed mid-execution by another thread resetting active_socket_
    std::shared_ptr<asio::ip::tcp::socket> keeper = socket;

    uint64_t frames_in = 0;
    uint64_t bytes_in  = 0;
    dbg("RECV", "reader thread attached to channel");

    try {
        while (is_running_ && keeper && keeper->is_open()) {
            uint32_t payload_length = 0;
            asio::error_code ec;

            // Read 4-byte header length prefix securely
            asio::read(*keeper, asio::buffer(&payload_length, sizeof(payload_length)), ec);
            if (ec) {
                say("[Session] Transport stream ended: " + ec.message());
                dbg("RECV", "header read failed after " + std::to_string(frames_in) +
                            " frames / " + std::to_string(bytes_in) + " bytes; ec=" +
                            std::to_string(ec.value()) + " (" + ec.category().name() + ")");
                break;
            }

            if (payload_length == 0 || payload_length > 64 * 1024 * 1024) {
                // A bogus length means the byte stream is no longer frame aligned.
                // Continuing would keep reading body bytes as headers forever, so
                // report what arrived and stop instead of spinning silently.
                complain("[Framing Error] Implausible frame length " + std::to_string(payload_length) +
                         " after " + std::to_string(frames_in) + " good frames. Stream is desynchronized.");
                dbg("RECV", "bad header bytes: " + hex_dump(&payload_length, sizeof(payload_length)));
                break;
            }

            dbg("RECV", "frame #" + std::to_string(frames_in + 1) + " header declares " +
                        std::to_string(payload_length) + " body bytes");

            std::vector<char> buffer(payload_length);
            asio::read(*keeper, asio::buffer(buffer.data(), payload_length), ec);
            if (ec) {
                say("[Session] Body read error: " + ec.message());
                dbg("RECV", "body read failed at frame #" + std::to_string(frames_in + 1) +
                            "; ec=" + std::to_string(ec.value()));
                break;
            }

            ++frames_in;
            bytes_in += sizeof(payload_length) + payload_length;

            try {
                std::string raw_json(buffer.begin(), buffer.end());
                auto parsed_payload = nlohmann::json::parse(raw_json);
                std::string msg_type = parsed_payload.value("type", "");

                dbg("RECV", "frame #" + std::to_string(frames_in) + " type=" +
                            (msg_type.empty() ? "<missing>" : msg_type) +
                            " path=" + parsed_payload.value("path", "-"));

                // A failure while handling one message must not tear down the whole
                // session, so each dispatch is contained here rather than at loop scope.
                try {
                    if (msg_type == "MSG_VAULT_INDEX") {
                        std::thread([this, raw_json_copy = std::move(raw_json)]() {
                            try {
                                auto thread_safe_json = nlohmann::json::parse(raw_json_copy);
                                reconcile_remote_index(thread_safe_json);
                            }
                            catch (const std::exception& e) {
                                complain(std::string("[Reconciliation Thread Error] ") + e.what());
                            }
                        }).detach();
                    }
                    else if (msg_type == "MSG_FILE_REQ") {
                        handle_file_request(parsed_payload["path"]);
                    }
                    else if (msg_type == "MSG_FILE_PAYLOAD") {
                        handle_incoming_payload(parsed_payload);
                    }
                    else {
                        dbg("RECV", "no handler for message type '" + msg_type + "' - ignored");
                    }
                }
                catch (const std::exception& e) {
                    complain("[Handler Error] type=" + msg_type + " threw: " + e.what() +
                             " -- message skipped, session kept alive.");
                }
            }
            catch(const nlohmann::json::parse_error& e){
                 complain(std::string("[Framing Error] JSON packet corruption detected: ") + e.what());
                 dbg("RECV", "first bytes of bad frame: " +
                             hex_dump(buffer.data(), std::min<size_t>(buffer.size(), 120)));
            }
        }
    }
    catch (const std::exception& e) {
        say(std::string("[Session] Standard exception encountered in reading channel: ") + e.what());
    }
    catch (...) {
        say("[Session] Runtime failure inside reading channel.");
    }

    dbg("RECV", "reader exiting after " + std::to_string(frames_in) + " frames / " +
                std::to_string(bytes_in) + " bytes; is_running=" + std::to_string(is_running_.load()) +
                " socket_open=" + std::to_string(keeper && keeper->is_open()));

    // Clean up cleanly only when everything has stopped using the connection context
    std::lock_guard<std::mutex> lock(socket_mutex_);
    if (active_socket_ == keeper) {
        asio::error_code close_ec;
        active_socket_->close(close_ec);
        active_socket_.reset();
        say("[Session] Channel cleared out cleanly.");
    } else {
        dbg("RECV", "channel already replaced by another socket; leaving active_socket_ untouched");
    }
}



void SyncNode::reconcile_remote_index(const nlohmann::json& remote_payload) {
    snapshot_engine_.generate_snapshot();
    auto local_snapshot = snapshot_engine_.get_snapshot();

    say("[Sync] Reconciling indices and issuing pull requests...");
    dbg("RECONCILE", "remote advertises " + std::to_string(remote_payload["files"].size()) +
                     " files, local holds " + std::to_string(local_snapshot.size()));

    size_t requested = 0;
    size_t up_to_date = 0;

    for (const auto& file : remote_payload["files"]) {
        std::string path = file["path"];
        std::string r_hash = file["hash"];
        int64_t r_mtime = file["mtime"];

        bool request_needed = false;

        if (local_snapshot.find(path) == local_snapshot.end()) {
            say(" -> Pulling Missing File: " + path);
            request_needed = true;
        } else {
            const auto& local_meta = local_snapshot[path];
            if (local_meta.content_hash != r_hash && r_mtime > local_meta.last_modified) {
                say(" -> Pulling Outdated File (Remote is newer): " + path);
                request_needed = true;
            } else {
                ++up_to_date;
                dbg("RECONCILE", "skip " + path + " (local_hash=" + local_meta.content_hash +
                                 " remote_hash=" + r_hash +
                                 " local_mtime=" + std::to_string(local_meta.last_modified) +
                                 " remote_mtime=" + std::to_string(r_mtime) + ")");
            }
        }

        if (request_needed) {
            nlohmann::json req_payload = {
                {"type", "MSG_FILE_REQ"},
                {"path", path}
            };
            send_message(req_payload);
            ++requested;

            //short throttling backoff to ensure packets are sent not too fast
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    dbg("RECONCILE", "done: " + std::to_string(requested) + " pull requests issued, " +
                     std::to_string(up_to_date) + " already current");
}

void SyncNode::handle_file_request(const std::string& relative_path)
{
    //reconstruct canonical absolute path using host filesystem anchor
    fs::path full_path = fs::path(snapshot_engine_.get_vault_path()) / relative_path;
    dbg("SERVE", "peer requested '" + relative_path + "' -> " + full_path.string());

    if(!fs::exists(full_path) || !fs::is_regular_file(full_path)){
        complain("[TRANSFER ERROR] Denied pull request for non-existent assest: " + relative_path);
        dbg("SERVE", "resolved path missing or not a regular file: " + full_path.string());
        return;
    }


     // Safeguard against empty files breaking the socket tunnel
    if (fs::file_size(full_path) == 0) {
        say("[Transfer] Creating empty file placeholder for: " + relative_path);
        nlohmann::json payload = {
            {"type", "MSG_FILE_PAYLOAD"},
            {"path", relative_path},
            {"content", ""} // Explicitly empty string block
        };
        send_message(payload);
        return;
    }


    std::ifstream file(full_path, std::ios::binary);
    if(!file){
        complain("[TRANSFER ERROR] Could not open for read: " + full_path.string());
        return;
    }

    //read raw content into a string buffer
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    nlohmann::json payload = {
        {"type", "MSG_FILE_PAYLOAD"},
        {"path", relative_path},
        {"content", content}
    };

    say("[Transfer] Uploading: " + relative_path + " (" + std::to_string(content.size()) + " bytes)");
    send_message(payload);
}

void SyncNode::handle_incoming_payload(const nlohmann::json& payload)
{
    std::string relative_path = payload["path"];
    std::string content = payload["content"];

    fs::path target_path = fs::path(snapshot_engine_.get_vault_path()) / relative_path;
    dbg("WRITE", "incoming '" + relative_path + "' (" + std::to_string(content.size()) +
                 " bytes) -> " + target_path.string());

    //ensure nested parent directories exist locally before attempting file creation.
    //create_directories (plural) is required here: vault paths are several levels deep
    //and single-level create_directory throws when an intermediate parent is missing.
    fs::path parent_dir = target_path.parent_path();
    if (!parent_dir.empty() && !fs::exists(parent_dir)) {
        std::error_code mk_ec;
        fs::create_directories(parent_dir, mk_ec);
        if (mk_ec) {
            complain("[IO ERROR] Could not create parent directory '" + parent_dir.string() +
                     "': " + mk_ec.message());
            return;
        }
        dbg("WRITE", "created missing directory tree " + parent_dir.string());
    }

    //1. implement atomic write - output contents to a temp staging workspace
    fs::path tmp_path = target_path;
    tmp_path.replace_extension(target_path.extension().string() + ".tmp");

    std::ofstream out_file(tmp_path, std::ios::binary);
    if(!out_file){
        complain("[IO ERROR] Failed to open temp file for write: " + tmp_path.string());
        return;
    }
    out_file.write(content.data(), content.size());
    out_file.close();
    if(!out_file){
        complain("[IO ERROR] Staging write failed for: " + tmp_path.string());
        return;
    }
    dbg("WRITE", "staged " + std::to_string(content.size()) + " bytes at " + tmp_path.string());

    //2. Atomic rename operation swaps staging file over target notes cleanly
    try{
        fs::rename(tmp_path, target_path);
        say("[SYNC SUCCESS] Successfully synced and updated file: " + relative_path);
    }catch(const std::exception& e){
        complain(std::string("[IO ERROR] Atomic replacement commit failed: ") + e.what());
    }
}


void SyncNode::send_message(const nlohmann::json& message) {
    // Copy the active pointer context locally within the lock window to prevent data race modifications
    std::shared_ptr<asio::ip::tcp::socket> current_sock;
    {
        std::lock_guard<std::mutex> lock(socket_mutex_);
        if (!active_socket_ || !active_socket_->is_open()) {
            dbg("SEND", "DROPPED type=" + message.value("type", "<none>") +
                        " path=" + message.value("path", "-") +
                        " reason=no active channel (socket is " +
                        std::string(active_socket_ ? "closed" : "null") + ")");
            return;
        }
        current_sock = active_socket_;
    }

    const std::string msg_type = message.value("type", "<none>");
    const std::string msg_path = message.value("path", "-");

    std::string serialized;
    try {
        serialized = message.dump();
    }
    catch (const std::exception& e) {
        // nlohmann throws type_error 316 here when a note's bytes are not valid UTF-8,
        // which is the usual reason one specific file refuses to leave the machine.
        complain("[Transmit Error] Could not serialize type=" + msg_type + " path=" + msg_path +
                 ": " + e.what());
        return;
    }

    try {
        uint32_t length = static_cast<uint32_t>(serialized.size());

        std::string packet;
        packet.resize(sizeof(length) + length);

        std::memcpy(packet.data(), &length, sizeof(length));
        std::memcpy(packet.data() + sizeof(length), serialized.data(), length);

        // asio::write is not safe to call concurrently on one socket, and three threads
        // reach this function: the reader serving MSG_FILE_REQ, the reconcile thread
        // issuing pulls, and the console thread sending the index. Without this lock two
        // frames interleave and the peer reads a body byte as a length prefix.
        std::lock_guard<std::mutex> write_lock(write_mutex_);
        dbg("SEND", "type=" + msg_type + " path=" + msg_path + " frame=" +
                    std::to_string(packet.size()) + " bytes (4 header + " +
                    std::to_string(length) + " json)");

        size_t written = asio::write(*current_sock, asio::buffer(packet));

        dbg("SEND", "flushed " + std::to_string(written) + "/" + std::to_string(packet.size()) +
                    " bytes for type=" + msg_type + " path=" + msg_path);
    }
    catch (const asio::system_error& e) {
        complain("[Transmit Error] Packet stream write failure on type=" + msg_type +
                 " path=" + msg_path + ": " + e.what() + " [ec=" +
                 std::to_string(e.code().value()) + " " + e.code().category().name() + "]");
    }
    catch (const std::exception& e) {
        complain("[Transmit Error] Packet stream write failure on type=" + msg_type +
                 " path=" + msg_path + ": " + e.what());
    }
    catch (...) {
        complain("[Transmit Error] Unknown packet stream write failure on type=" + msg_type +
                 " path=" + msg_path + ".");
    }
}
