#include "network/sync_node.hpp"
#include <iostream>


int main(int argc, char* argv[])
{
    if(argc< 4)
    {
        std::cout << "Usage: peer_sync <local port> <remote ip> <remote_port>\n";
    }

    unsigned short  local_port = static_cast<unsigned short>(std::stoi(argv[1]));
    std::string remote_ip = argv[2];
    unsigned short remote_port = static_cast<unsigned short>(std::stoi(argv[3]));

    SyncNode node(local_port, remote_ip, remote_port);
    node.start();

    std::cout <<"Press Enter to transmit a test sync transaction handshake or 'q' then Enter to quit... \n";
    std::string input {};
    while(std::getline(std::cin, input))
    {
        if(input == "q") break;

        nlohmann::json handshake_tx = {
            {"type", "SYNC_REQ"},
            {"version", "1.0.0"},
            {"valut_name", "MyObisidionVault"}
        };
        node.send_message(handshake_tx);
    }

    node.stop();
    return 0;

}