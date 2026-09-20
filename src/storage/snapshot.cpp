#include "storage/snapshot.hpp"
#include <fstream>
#include <iostream>
#include <chrono>
#include <sstream>
#include <iomanip>

SnapshotEngine::SnapshotEngine(const std::string& vault_path)
    : vault_path_(fs::canonical(vault_path)) {}

const std::unordered_map<std::string, FileMetadata>& SnapshotEngine::get_snapshot() const{
    return current_snapshot_;
}

std::string SnapshotEngine::calculate_hash(const fs::path& file_path){
    std::ifstream file(file_path, std::ios::binary);
    if(!file) return "000000000000";

    //simple FNV-1a hash string implementation
    uint64_t hash = 14695981039346656037ULL;
    char buffer[4096];
    while(file.read(buffer, sizeof(buffer))){
        for(std::streamsize i = 0; i < file.gcount(); ++i)
        {
            hash ^= static_cast<uint8_t>(buffer[i]);
            hash *= 1099511628211ULL;
        }
    }

    //Handle remaining bytes if file size doesn't perfectly align to 4096 bytes
    for(std::streamsize i = 0; i < file.gcount(); ++i){
        hash ^= static_cast<uint8_t>(buffer[i]);
        hash *= 1099511628211ULL;
    }

    std::stringstream stream;
    stream << std::hex << std::setw(16) << std::setfill('0') << hash;
    return stream.str();
}

void SnapshotEngine::generate_snapshot(){
    current_snapshot_.clear();

    if(!fs::exists(vault_path_) || !fs::is_directory(vault_path_)){
        std::cerr << "[SNAPSHOT ENGINE ERROR] Target vault path does not exist.\n";
    }

    //recursive directroy transveral iterator
    for(const auto& entry: fs::recursive_directory_iterator(vault_path_))
    {
        const auto& path = entry.path();

        //filter out the hidden config directory
        std::string string_path = path.string();
        if(string_path.find(".obsidian") != std::string::npos){
            continue;
        }

        //only track standard files (skip structural folders themselves)
        if(fs::is_regular_file(path)){
            try{

                 //filter to only accept markdown files
                 if(path.extension() != ".md"){
                    continue;
                 }   

                //compute the path relative to the root of the vault
                std::filesystem::path relative = fs::relative(path, vault_path_);
                std::string rel_path_string = relative.generic_string(); //forces unified '/'

                auto last_write_time = fs::last_write_time(path);
                auto time_since_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                        last_write_time.time_since_epoch()
                ).count();

                FileMetadata meta{
                    rel_path_string,
                    fs::file_size(path),
                    time_since_epoch,
                    calculate_hash(path)
                };
                
                current_snapshot_[rel_path_string] = meta;
            }
            catch(const std::exception& e)
            {
                std::cerr << "[SNAPSHOT ENGINE EXCEPTION] Failed Processing " << path << ": " << e.what() << "\n"; 
            }
        }
    }
}

void SnapshotEngine::debug_print() const{

    std::cout << "\n=== Current Obsidian Vault Index State ===\n";
    std::cout << "Total Tracked Files: " << current_snapshot_.size() << "\n";
    for (const auto& [path, meta] : current_snapshot_) {
        std::cout << " -> File: " << path \
                  << " | Size: " << meta.file_size \
                  << " bytes | Hash: " << meta.content_hash \
                  << " | ModTime: " << meta.last_modified << "\n";
    }
    std::cout << "===========================================\n\n";
}