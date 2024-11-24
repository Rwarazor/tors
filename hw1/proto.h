#pragma once

#include <string>
#include <variant>
#include <vector>

namespace hw1 {

namespace proto {

using SessionId = uint64_t;

struct WorkDescription {
    // function fixed
    double xFrom;
    double xTo;
    std::size_t segmentsCnt;
    std::size_t probesPerSegment;
};

/*
      INTERACTION OVERVIEW
master                    worker
   |===========UDP===========|
   |----DiscoveryRequest---->|
   |                         |
   |<---DiscoveryResponse----|
   |===========UDP===========|
   |===========TCP===========|
   |------StartRequest------>|
   |-------WorkRequest------>| // WorkRequest desribes everything the worker needs to do,
   |                         | // to make having an up-to-date view of required work
   |<-------HeartBeat--------|
   |<-------HeartBeat--------| // sends heartbeats for failure-detection
   |<-------HeartBeat--------|
   |<------WorkResponse------| // sends results granularly,
   |<-------HeartBeat--------| // for them to persist upon node or network failure
   |<------WorkResponse------|
   |           ...           |
   |<-------HeartBeat--------|
   |<------WorkResponse------| // last requested work
   |<-------HeartBeat--------|
   |===========TCP===========| // breaking connection identifies end of work
   X                         |
                             |
                             X
*/

struct Message {};

namespace udp {

struct DiscoveryRequest : Message {};
struct DiscoveryResponse : Message {
    int workerTCPPort;
};

} // namespace udp

namespace tcp {

struct StartRequest : Message {
    SessionId sessionId;

    WorkDescription workDesc;
};
struct WorkRequest : Message {
    std::vector<std::size_t> segmentIndixes;
};
struct Heartbeat : Message {
    std::size_t workHash;
};
struct WorkResponse : Message {
    std::size_t workHash;
    std::size_t segmentIndex;
    double result;
};

} // namespace tcp

using SomeMessage = std::variant<udp::DiscoveryRequest, udp::DiscoveryResponse, tcp::StartRequest,
                                 tcp::WorkRequest, tcp::Heartbeat, tcp::WorkResponse>;

} // namespace proto

} // namespace hw1
