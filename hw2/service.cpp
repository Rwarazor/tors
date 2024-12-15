#include "raft.h"

#include <iostream>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Expected 1 argument: node-id\n";
        exit(1);
    }
    size_t id = atoi(argv[1]);
    if (id < 1 || id > hw2::raft::TOTAL_IDS) {
        std::cerr << "Expected node-id to be between 1 and " << hw2::raft::TOTAL_IDS << "\n";
        exit(1);
    }
    hw2::raft::Node node(id);

    std::string s;
    while (std::cin >> s) {
        if (s == "get") {
            std::string key;
            std::cin >> key;
            std::string val = node.Get(key).value_or("{_nullopt_}");
            std::cout << "From key " << s << " got value " << val << '\n';
        } else if (s == "set") {
            std::string key, val;
            std::cin >> key >> val;
            std::cout << "Setting key " << s << " to value " << val << '\n';
            node.Set(key, val, [&node, val, key](hw2::raft::Node::SetStatus status) {
                if (status == hw2::raft::Node::SetStatus::OK_DONE) {
                    std::cout << "key " << key << " successfully set to " << val << '\n';
                } else if (status == hw2::raft::Node::SetStatus::OK_NOT_LEADER) {
                    std::cout << "key couldn't be set because node is not a leader\n";
                    if (node.LeaderId().has_value()) {
                        std::cout << "Actual leader " << node.LeaderId().value() << "\n";
                    } else {
                        std::cout << "Currently there is no leader\n";
                    }
                } else {
                    std::cout << "Set failed\n";
                }
            });
        } else if (s == "delete") {
            std::string key;
            std::cin >> key;
            std::cout << "Deleting key " << s << '\n';
            node.Delete(key, [&node, key](hw2::raft::Node::SetStatus status) {
                if (status == hw2::raft::Node::SetStatus::OK_DONE) {
                    std::cout << "key " << key << " successfully deleted " << '\n';
                } else if (status == hw2::raft::Node::SetStatus::OK_NOT_LEADER) {
                    std::cout << "key couldn't be set because node is not a leader\n";
                    if (node.LeaderId().has_value()) {
                        std::cout << "Actual leader " << node.LeaderId().value();
                    } else {
                        std::cout << "Currently there is no leader\n";
                    }
                } else {
                    std::cout << "Delete failed";
                }
            });
        } else if (s == "stop") {
            break;
        }
    }
}
