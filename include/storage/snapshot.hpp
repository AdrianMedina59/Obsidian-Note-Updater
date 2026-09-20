#pragma once

#include <string>
#include <unordered_map>
#include <filesystem>

namespace fs = std::filesystem;

struct FileMetadata{
    std::string relative_path;
    uint64_t file_size;
    int64_t last_modified;
    std::string content_hash;
};

class SnapshotEngine{
    public:
        explicit SnapshotEngine(const std::string& vault_path);
        
        //returns the current snapshot map: key is relative path, value is file metadata
        const std::unordered_map<std::string, FileMetadata>& get_snapshot() const;

        //Prints the snapshot to the console for testing
        void debug_print() const;

        //Recersively scans the vault path and populates the current snapshot
        void generate_snapshot();

    private:
        //Helper to calculate a file hash securely
        std::string calculate_hash(const fs::path& file_path);

        fs::path vault_path_;
        std::unordered_map<std::string, FileMetadata> current_snapshot_;
};