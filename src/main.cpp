#include "network/sync_node.hpp"
#include "storage/snapshot.hpp"
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

    std::cout <<"Press Enter to transmit a test sync transaction handshake or 'q' then Enter to quit... \n";
    std::string input {};
    while(std::getline(std::cin, input))
    {
        if(input == "q") break;

        node.send_vault_index();
    }

    node.stop();
    return 0;

}