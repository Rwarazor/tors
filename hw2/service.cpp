#include "raft.h"

#include <iostream>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Expected 1 argument: node-id\n";
        exit(1);
    }
    size_t id = atoi(argv[1]);
    if (id < 1 || id > 3) {
        std::cerr << "Expected node-id to be between 1 and 3\n";
        exit(1);
    }
    hw2::raft::Node node(id);

    sleep(1000);
}
