#include "address.h"
#include "math.h"
#include "network.h"

#include <algorithm>
#include <arpa/inet.h>
#include <assert.h>
#include <chrono>
#include <iostream>
#include <map>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <queue>
#include <set>
#include <signal.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

using namespace hw1::serialize;
using namespace hw1::proto;
using namespace hw1;

constexpr size_t MAXBUFLEN = 1000;
constexpr size_t MAX_WORKERS = 30;
const auto HEARTBEAT_TIMEOUT = std::chrono::seconds(5);

int createUdpSocket() {
    int udpSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (udpSocket == -1) {
        perror("udpSocket creation failed");
        throw std::runtime_error("perror");
    }
    int broadcast = 1;
    if (setsockopt(udpSocket, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast)) == -1) {
        perror("setsockopt for broadcast failed");
        throw std::runtime_error("perror");
    }
    sockaddr_in servAddr;
    servAddr.sin_family = AF_INET;
    servAddr.sin_port = 0;
    servAddr.sin_addr.s_addr = INADDR_ANY;
    if (bind(udpSocket, (sockaddr *)&servAddr, sizeof(servAddr)) == -1) {
        perror("bind");
        throw std::runtime_error("errno");
    }
    return udpSocket;
}

void broadcastMessage(int sock, unsigned short port, std::string message) {
    struct sockaddr_in broadcastAddr;
    broadcastAddr.sin_family = AF_INET;
    broadcastAddr.sin_port = htons(port);
    broadcastAddr.sin_addr.s_addr = INADDR_BROADCAST;
    memset(broadcastAddr.sin_zero, 0, sizeof(broadcastAddr.sin_zero));

    if (sendto(sock, message.c_str(), message.size(), 0, (struct sockaddr *)&broadcastAddr,
               sizeof(broadcastAddr)) == -1) {
        perror("sendto");
        throw std::runtime_error("perror");
    }
}

struct ActiveWorker {
    std::vector<size_t> segment_indexes;
    size_t index;
    sockaddr_in addr;
    mutable decltype(std::chrono::system_clock::now()) time;
    mutable bool started = false;
    mutable bool changed = false;
    mutable std::uint64_t lastassignedhash = 0;
    mutable std::uint64_t lastheartbeathash = 0;

    bool operator<(const ActiveWorker &other) const {
        return segment_indexes.size() < other.segment_indexes.size();
    }
};

struct Workers {
    Workers(size_t segments, size_t max_workers) {
        for (size_t i = 1; i <= max_workers; ++i) {
            unused_indexes.insert(i);
        }
        for (size_t i = 1; i <= segments; ++i) {
            unassignedSegments.push_back(i);
        }
    }

    bool imbalanced() {
        if (workers.size() < 2) {
            return false;
        }
        return std::prev(workers.end())->segment_indexes.size() -
                   workers.begin()->segment_indexes.size() >
               1;
    }

    void shuffle() {
        ActiveWorker a = *workers.begin();
        workers.erase(workers.begin());
        ActiveWorker b = *--workers.end();
        workers.erase(--workers.end());
        segmentToWorker[b.segment_indexes.back()] = a.index;
        a.segment_indexes.push_back(b.segment_indexes.back());
        b.segment_indexes.pop_back();
        a.changed = true;
        b.changed = true;
        workers.insert(a);
        workers.insert(b);
    }

    void updateHashes() {
        for (const auto &it : workers) {
            if (it.changed) {
                it.lastassignedhash = math::hash(it.segment_indexes);
            }
        }
    }

    void balance() {
        while (!unassignedSegments.empty() && !workers.empty()) {
            auto worker = *workers.begin();
            workers.erase(workers.begin());

            segmentToWorker[unassignedSegments.back()] = worker.index;
            worker.segment_indexes.push_back(unassignedSegments.back());
            worker.changed = true;
            unassignedSegments.pop_back();
            workers.insert(worker);
        }
        while (imbalanced()) {
            shuffle();
        }
        updateHashes();
    }

    size_t AddWorker(sockaddr_in addr) {
        for (const auto &worker : workers) {
            if (std::memcmp((void *)&worker.addr, (void *)&addr, sizeof(addr))) {
                return worker.index;
            }
        }
        size_t index = *unused_indexes.begin();
        unused_indexes.erase(index);
        workers.insert(ActiveWorker{.segment_indexes = {},
                                    .index = index,
                                    .addr = addr,
                                    .time = std::chrono::system_clock::now(),
                                    .changed = false});
        balance();
        return index;
    }

    void RemoveWorker(size_t index) {
        for (auto it = workers.begin(); it != workers.end(); ++it) {
            if (it->index == index) {
                unassignedSegments = it->segment_indexes;
                for (auto segment : unassignedSegments) {
                    segmentToWorker.erase(segment);
                }
                workers.erase(it);
                balance();
                unused_indexes.insert(index);
                return;
            }
        }
        throw std::out_of_range("RemoveWorker out of range");
    }

    const ActiveWorker &At(size_t index) {
        for (auto it = workers.begin(); it != workers.end(); ++it) {
            if (it->index == index) {
                return *it;
            }
        }
        throw std::out_of_range("no such worker");
    }

    std::multiset<ActiveWorker>::iterator Find(size_t index) {
        for (auto it = workers.begin(); it != workers.end(); ++it) {
            if (it->index == index) {
                return it;
            }
        }
        return workers.end();
    }

    void SegmentSolved(size_t segment) {
        auto it = Find(segmentToWorker.at(segment));
        segmentToWorker.erase(segment);
        ActiveWorker worker = *it;
        workers.erase(it);
        worker.segment_indexes.erase(
            std::find(worker.segment_indexes.begin(), worker.segment_indexes.end(), segment));
        workers.insert(worker);
    }

    std::map<size_t, size_t> segmentToWorker;
    std::vector<size_t> unassignedSegments;
    std::multiset<ActiveWorker> workers;
    std::set<size_t> unused_indexes;
};

int main() {
    signal(SIGPIPE, SIG_IGN);

    int udpSocket = createUdpSocket();
    // no sense in polling here, because this almost never blocks
    std::thread discoveryThread{[&]() {
        while (true) {
            std::cout << "sending discoveryRequest\n";
            broadcastMessage(udpSocket, WORKER_UDP_PORT, ToBytes(udp::DiscoveryRequest{}));
            sleep(5);
        }
    }};
    discoveryThread.detach();

    WorkDescription desc{
        .xFrom = 0,
        .xTo = 1,
        .segmentsCnt = 16,
        .probesPerSegment = 1'00'000'000,
    };

    size_t segmentsUnsolved = desc.segmentsCnt;
    double result = 0;
    Workers workers(desc.segmentsCnt, MAX_WORKERS);

    pollfd fds[MAX_WORKERS + 1];
    fds[0].fd = udpSocket;
    fds[0].events = POLLIN | POLLHUP | POLLERR;
    for (size_t i = 1; i <= MAX_WORKERS; ++i) {
        fds[i].fd = -1;
    }

    while (segmentsUnsolved > 0) {
        for (size_t i = 1; i <= MAX_WORKERS; ++i) {
            fds[i].events = POLLIN | POLLHUP | POLLERR;
        }
        for (auto &worker : workers.workers) {
            if (!worker.started || (worker.lastassignedhash != worker.lastheartbeathash)) {
                fds[worker.index].events |= POLLOUT;
            }
        }
        if (poll(fds, MAX_WORKERS + 1, 1000) == -1) {
            perror("poll");
            throw std::runtime_error("perror");
        }
        if (fds[0].revents & (POLLERR | POLLHUP)) {
            // TODO
            throw std::runtime_error("POLLERR POLLHUP on fds[0]");
        }
        if ((fds[0].revents & POLLIN)) {
            std::cout << "recieving DiscoveryResponse";
            char buffer[MAXBUFLEN];
            sockaddr_in addrFrom;
            unsigned int addrSize = sizeof(addrFrom);
            int bytesRecvd;
            if ((bytesRecvd = recvfrom(fds[0].fd, buffer, MAXBUFLEN - 1, MSG_DONTWAIT,
                                       (sockaddr *)&addrFrom, &addrSize)) == -1) {
                if (errno != EAGAIN) {
                    perror("recvfrom");
                    throw std::runtime_error("perror");
                }
            }
            if (bytesRecvd == 0) {
                std::cerr << "WARN, EOF even tough poll\n";
            } else {
                std::stringstream input(std::string(buffer, buffer + bytesRecvd));
                SomeMessage msg;
                FromBytes(input, msg);
                if (!std::holds_alternative<udp::DiscoveryResponse>(msg)) {
                    throw std::runtime_error("bad msg");
                }
                std::cout << "recieved DiscoveryResponse";
                auto resp = std::get<udp::DiscoveryResponse>(msg);
                addrFrom.sin_port = htons(resp.workerTCPPort);

                auto index = workers.AddWorker(addrFrom);

                if (fds[index].fd == -1) {
                    fds[index].fd = socket(AF_INET, SOCK_STREAM, 0);
                    if (fds[index].fd == -1) {
                        perror("tcpSocket creation failed");
                        throw std::runtime_error("perror");
                    }
                    std::cout << "Connecting to worker " << index << '\n';
                    if (connect(fds[index].fd, (sockaddr *)&addrFrom, addrSize) == -1) {
                        // TODO could fail because of untimely network partition
                        perror("connect");
                        throw std::runtime_error("perror");
                    }
                }
            }
        }
        for (size_t i = 1; i <= MAX_WORKERS; ++i) {
            if (fds[i].revents & (POLLERR | POLLHUP)) {
                std::cout << "kicking worker " << i << '\n';
                if (close(fds[i].fd) == -1) {
                    perror("close");
                    throw std::runtime_error("perror");
                };
                fds[i].fd = -1;
                workers.RemoveWorker(i);
                continue;
            }
            if (fds[i].revents & POLLIN) {
                network::ConsumeAllMesages(fds[i].fd, [&](SomeMessage msg) {
                    workers.At(i).time = std::chrono::system_clock::now();
                    if (std::holds_alternative<tcp::Heartbeat>(msg)) {
                        workers.At(i).lastheartbeathash = std::get<tcp::Heartbeat>(msg).workHash;
                        // std::cout << "Recieved heartbeat from worker " << i << '\n';
                    } else if (std::holds_alternative<tcp::WorkResponse>(msg)) {
                        auto &resp = std::get<tcp::WorkResponse>(msg);
                        workers.At(i).lastheartbeathash = resp.workHash;
                        std::cout << "Recieved response from worker " << i << " for segment "
                                  << resp.segmentIndex << '\n';
                        auto it = workers.segmentToWorker.find(resp.segmentIndex);
                        if (it != workers.segmentToWorker.end() && it->second == i) {
                            segmentsUnsolved--;
                            result += resp.result;
                            workers.SegmentSolved(resp.segmentIndex);
                        }
                    } else {
                        throw std::runtime_error("bad msg");
                    }
                });
            }
            if (fds[i].revents & POLLOUT) {
                const auto &worker = workers.At(i);
                if (!worker.started) {
                    worker.started = true;
                    std::cout << "Sending StartRequest to worker " << i << '\n';
                    network::SendStringNonBlocking(
                        fds[i].fd, ToBytes(proto::tcp::StartRequest{.workDesc = desc}));
                } else if (worker.lastassignedhash != worker.lastheartbeathash) {
                    // I KNOW THATS NOT TRUE, BUT ITS DONE SO WE DONT SPAM StartRequest
                    worker.lastheartbeathash = worker.lastassignedhash;
                    std::cout << "Sending WorkRequest to worker " << i << '\n';
                    // for (auto ind : worker.segment_indexes) {
                    //     std::cout << ind << ' ';
                    // }
                    std::cout << '\n';
                    network::SendStringNonBlocking(
                        fds[i].fd,
                        ToBytes(proto::tcp::WorkRequest{.segmentIndixes = worker.segment_indexes}));
                }
            }
            if (fds[i].fd != -1 &&
                (std::chrono::system_clock::now() - workers.At(i).time) > HEARTBEAT_TIMEOUT) {
                std::cout << "kicking worker " << i << '\n';
                if (close(fds[i].fd) == -1) {
                    perror("close");
                    throw std::runtime_error("perror");
                };
                fds[i].fd = -1;
                workers.RemoveWorker(i);
            }
        }
    }

    std::cout << "result: " << result << '\n';

    return 0;
}
