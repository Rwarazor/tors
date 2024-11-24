#include "address.h"
#include "math.h"
#include "network.h"
#include "queue.h"

#include <arpa/inet.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

using namespace hw1::serialize;
using namespace hw1;

constexpr size_t MAXBUFLEN = 1000;

int createUdpSocket(unsigned short port) {
    int udpSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSocket == -1) {
        perror("udpSocket creation failed");
        throw std::runtime_error("errno");
    }
    int reuse = 1;
    if (setsockopt(udpSocket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1) {
        perror("setsockopt reuse failed");
        throw std::runtime_error("errno");
    }
    sockaddr_in servAddr;
    servAddr.sin_family = AF_INET;
    servAddr.sin_port = htons(port);
    servAddr.sin_addr.s_addr = INADDR_ANY;
    memset(servAddr.sin_zero, 0, sizeof(servAddr.sin_zero));

    if (bind(udpSocket, (sockaddr *)&servAddr, sizeof(servAddr)) == -1) {
        perror("bind");
        throw std::runtime_error("errno");
    }
    return udpSocket;
}

int createAcceptSocket(unsigned short port) {
    int acceptSocket = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (acceptSocket == -1) {
        perror("acceptSocket creation failed");
        throw std::runtime_error("errno");
    }
    sockaddr_in servAddr;
    servAddr.sin_family = AF_INET;
    servAddr.sin_port = htons(port);
    servAddr.sin_addr.s_addr = INADDR_ANY;
    memset(servAddr.sin_zero, 0, sizeof(servAddr.sin_zero));

    if (bind(acceptSocket, (sockaddr *)&servAddr, sizeof(servAddr)) == -1) {
        perror("bind");
        throw std::runtime_error("errno");
    }
    if (listen(acceptSocket, 5) == -1) {
        perror("listen");
        throw std::runtime_error("errno");
    }
    return acceptSocket;
}

int main(int argc, char *argv[]) {
    // int port = 50444;
    if (argc != 2) {
        throw std::runtime_error("errno");
    }
    int port = atoi(argv[1]);

    pollfd fds[3];
    std::cout << "creating udp socket\n";
    fds[0].fd = createUdpSocket(WORKER_UDP_PORT);
    std::cout << "creating accept socket\n";
    fds[1].fd = createAcceptSocket(port);
    fds[2].fd = -1;

    std::stringstream tcpStream(std::ios_base::app | std::ios_base::out | std::ios_base::in);

    bool hasMaster = false;

    proto::WorkDescription workDesc;

    Queue<size_t> segments;
    Queue<proto::tcp::WorkResponse> results;
    std::optional<proto::tcp::WorkResponse> currentWrite;
    std::atomic_size_t workHash = 0;

    std::thread workThread{[&]() {
        while (true) {
            auto segment = segments.pop();
            if (segment.has_value()) {
                double xl = workDesc.xFrom + (workDesc.xTo - workDesc.xFrom) * segment.value() /
                                                 workDesc.segmentsCnt;
                double xr = workDesc.xFrom + (workDesc.xTo - workDesc.xFrom) *
                                                 (segment.value() + 1) / workDesc.segmentsCnt;
                double sum = 0;
                for (size_t step = 0; step < workDesc.probesPerSegment; ++step) {
                    double x = xl + (xr - xl) * step / workDesc.probesPerSegment;
                    double y = math::FunctionAt(x);
                    sum += y;
                }
                results.push({
                    .workHash = workHash,
                    .segmentIndex = segment.value(),
                    .result = sum / workDesc.probesPerSegment / workDesc.segmentsCnt *
                              (workDesc.xTo - workDesc.xFrom),
                });
            } else {
                sleep(1);
            }
        }
    }};
    workThread.detach();
    auto lastMessageTime = std::chrono::system_clock::now();
    while (true) {
        fds[0].events = hasMaster ? 0 : POLLIN;
        fds[1].events = hasMaster ? 0 : POLLIN;
        fds[2].events = POLLIN | POLLOUT;
        if (poll(fds, 3, -1) == -1) {
            perror("poll");
            throw std::runtime_error("errno");
        }
        if (fds[0].revents & POLLIN) {
            int bytesRecvd;
            char buffer[MAXBUFLEN];
            sockaddr_in addrFrom;
            unsigned int addrSize = sizeof(addrFrom);
            if ((bytesRecvd = recvfrom(fds[0].fd, buffer, MAXBUFLEN - 1, MSG_DONTWAIT,
                                       (sockaddr *)&addrFrom, &addrSize)) == -1) {
                perror("recvfrom");
                throw std::runtime_error("errno");
            }
            std::stringstream input(std::string(buffer, buffer + bytesRecvd));
            std::cout << "Reading DiscoveryRequest " << bytesRecvd << '\n';
            proto::SomeMessage msg;
            FromBytes(input, msg);
            if (!std::holds_alternative<proto::udp::DiscoveryRequest>(msg)) {
                throw std::runtime_error("bad msg");
            }
            sockaddr_in broadcastAddr;
            std::string msgTo = ToBytes(proto::udp::DiscoveryResponse{
                .workerTCPPort = port,
            });
            std::cout << "Sending DiscoveryResponse " << '\n';
            std::cout << "to addr " << addrFrom.sin_addr.s_addr << '\n';
            std::cout << "to port " << addrFrom.sin_port << '\n';
            int res;
            if ((res = sendto(fds[0].fd, msgTo.c_str(), msgTo.size(), MSG_DONTWAIT,
                              (sockaddr *)&addrFrom, sizeof(addrFrom))) == -1) {
                perror("sendto");
                throw std::runtime_error("errno");
            }
            std::cout << "Sent " << res << "bytes\n";
        }
        if (fds[1].revents & POLLIN) {
            std::cout << "Accepting master\n";
            if ((fds[2].fd = accept(fds[1].fd, 0, 0)) == -1) {
                if (errno != EAGAIN) {
                    perror("accept master");
                    throw std::runtime_error("errno");
                }
            } else {
                hasMaster = true;
                lastMessageTime = std::chrono::system_clock::now();
            }
        }
        if (fds[2].revents & POLLIN) {
            std::cout << "recieving msg " << '\n';
            network::ConsumeAllMesages(fds[2].fd, [&](proto::SomeMessage msg) {
                if (std::holds_alternative<proto::tcp::StartRequest>(msg)) {
                    workDesc = std::get<proto::tcp::StartRequest>(msg).workDesc;
                    std::cout << "recieved StartRequest \n";
                } else if (std::holds_alternative<proto::tcp::WorkRequest>(msg)) {
                    if (workDesc.segmentsCnt == 0) {
                        throw std::runtime_error("messages out of order");
                    }
                    auto req = std::get<proto::tcp::WorkRequest>(msg);
                    workHash = math::hash(req.segmentIndixes);
                    std::cout << "recieved WorkRequest \n";
                    segments.mtx.lock();
                    while (segments.pop(false).has_value()) {
                    };
                    for (const auto &segment : req.segmentIndixes) {
                        segments.push(segment, false);
                    }
                    segments.mtx.unlock();
                } else {
                    throw std::runtime_error("bad msg");
                }
            });
        }
        if (fds[2].revents & POLLOUT) {
            while (currentWrite.has_value() || (currentWrite = results.pop()).has_value()) {
                std::cout << "sending result for segment " << currentWrite.value().segmentIndex
                          << "\n";
                if (network::SendStringNonBlocking(fds[2].fd, ToBytes(currentWrite.value()),
                                                   false)) {
                    currentWrite = std::nullopt;
                }

                lastMessageTime = std::chrono::system_clock::now();
            }
            if ((std::chrono::system_clock::now() - lastMessageTime).count() > 3e9) {
                std::cout << "sending heartbeat\n";
                network::SendStringNonBlocking(
                    fds[2].fd, ToBytes(proto::tcp::Heartbeat{.workHash = workHash}), false);
                lastMessageTime = std::chrono::system_clock::now();
            }
        }
    }
}
