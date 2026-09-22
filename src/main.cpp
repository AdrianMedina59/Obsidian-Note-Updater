#include "network/sync_node.hpp"
#include "storage/snapshot.hpp"
#include  "watcher/file_watcher.hpp"
#include <iostream>


int main(int argc, char* argv[])
{
    if(argc< 5)
    {
        std::cout << "Usage: peer_sync <local port> <remote ip> <remote_port> <vault_directory_path>\n";
        return 1;
    }

    unsigned short  local_port = static_cast<unsigned short>(std::stoi(argv[1]));
    std::string remote_ip = argv[2];
    unsigned short remote_port = static_cast<unsigned short>(std::stoi(argv[3]));
    std::string vault_dir = argv[4];

    SnapshotEngine engine(vault_dir);
   
    //initalzing sync node with required members
    SyncNode node(local_port, remote_ip, remote_port, engine);
    node.start();

    // Instantiate and start the real-time file watcher
    FileWatcher watcher(vault_dir, [&node]() {
        std::cout << "[Watcher Notification] Local filesystem changes registered! Re-indexing vault...\n";
        node.send_vault_index();
    });
    watcher.start();

    std::cout << "\n[System] Real-time Sync Engine active. Make edits inside your vault to verify...\n";
    std::cout << "Type 'q' and press Enter to cleanly shut down the application.\n\n";
    
    std::string input;
    while (std::getline(std::cin, input)) {
        if (input == "q") break;
    }

    std::cout << "[System] Shuttling down file tracking workers...\n";
    watcher.stop();
    node.stop();
    return 0;

}