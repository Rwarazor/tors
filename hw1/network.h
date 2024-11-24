#pragma once

#include "serialize.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace hw1 {

namespace network {

template <typename Callback>
void ConsumeAllMesages(int socket, Callback callback) {
    constexpr size_t MAXBUFLEN = 1000;
    char buffer[MAXBUFLEN];
    while (true) {
        int bytesRecvd;

        if ((bytesRecvd = recv(socket, buffer, MAXBUFLEN - 1, MSG_DONTWAIT)) == -1) {
            if (errno == EAGAIN) {
                return;
            }
            perror("recv");
            throw std::runtime_error("errno");
        }
        if (bytesRecvd == 0) {
            return;
        }
        std::stringstream input(std::string(buffer, buffer + bytesRecvd));
        while (input.peek() != std::char_traits<char>::eof()) {
            proto::SomeMessage msg;
            serialize::FromBytes(input, msg);
            callback(msg);
        }
    }
}

inline bool SendStringNonBlocking(int socket, std::string str, bool failBlocking = true) {
    auto res = send(socket, str.c_str(), str.size(), MSG_DONTWAIT);
    if (res == -1) {
        if (!failBlocking && errno == EAGAIN) {
            return false;
        }
        perror("sendAssumeNonBlocking");
        throw std::runtime_error("errno");
    } else if (res != str.size()) {
        throw std::runtime_error("not full msg sent");
    }
    return true;
}

} // namespace network
} // namespace hw1
