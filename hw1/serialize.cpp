#include "serialize.h"

namespace hw1 {

namespace serialize {

template <typename T>
concept IsTriviallySerializable = std::is_arithmetic_v<T> || std::is_enum_v<T>;

template <IsTriviallySerializable T>
std::string ToBytes(const T &obj) {
    std::string res;
    res.resize(sizeof(obj));
    std::memcpy(&res[0], &obj, sizeof(obj));
    return res;
}

template <IsTriviallySerializable T>
void FromBytes(std::istream &in, T &res) {
    in.read(reinterpret_cast<char *>(&res), sizeof(T));
    if (in.fail()) {
        std::cerr << "reading " << sizeof(T) << " bytes, read " << in.gcount() << '\n';
        throw std::runtime_error("bad format arthm");
    }
}

template <>
std::string ToBytes(const std::string &obj) {
    return ToBytes(obj.size()) + obj;
};

template <>
void FromBytes(std::istream &in, std::string &res) {
    std::size_t size;
    FromBytes(in, size);
    res.resize(size);
    in.read(&res[0], size);
    if (in.fail()) {
        std::cerr << "reading " << size << " bytes, read " << in.gcount() << '\n';
        throw std::runtime_error("bad format str");
    }
}

template <typename T>
std::string ToBytes(const std::vector<T> &obj) {
    std::string msg = ToBytes(obj.size());
    for (const auto &elem : obj) {
        msg.append(ToBytes(elem));
    }
    return msg;
};

template <typename T>
void FromBytes(std::istream &in, std::vector<T> &res) {
    std::size_t size;
    FromBytes(in, size);
    res.resize(size);
    for (auto &elem : res) {
        FromBytes(in, elem);
    }
};

template <>
std::string ToBytes(const proto::WorkDescription &obj) {
    return ToBytes(obj.probesPerSegment) + ToBytes(obj.segmentsCnt) + ToBytes(obj.xFrom) +
           ToBytes(obj.xTo);
}

template <>
void FromBytes(std::istream &in, proto::WorkDescription &res) {
    FromBytes(in, res.probesPerSegment);
    FromBytes(in, res.segmentsCnt);
    FromBytes(in, res.xFrom);
    FromBytes(in, res.xTo);
}

namespace {

template <typename T>
std::string ToBytesPrivate(const T &obj);

template <typename T>
void FromBytesPrivate(std::istream &in, T &res);

enum MessageType : uint8_t {
    DiscoveryRequest = 101,
    DiscoveryResponse = 102,

    StartRequest = 201,
    WorkRequest = 202,
    Heartbeat = 203,
    WorkResponse = 204
};

template <typename T>
MessageType TypeToEnumVal();

#define TYPE_TO_ENUM(type, enumVal)                                                                \
    template <>                                                                                    \
    MessageType TypeToEnumVal<type>() {                                                            \
        return MessageType::enumVal;                                                               \
    }

TYPE_TO_ENUM(proto::udp::DiscoveryRequest, DiscoveryRequest)
TYPE_TO_ENUM(proto::udp::DiscoveryResponse, DiscoveryResponse)
TYPE_TO_ENUM(proto::tcp::StartRequest, StartRequest)
TYPE_TO_ENUM(proto::tcp::WorkRequest, WorkRequest)
TYPE_TO_ENUM(proto::tcp::Heartbeat, Heartbeat)
TYPE_TO_ENUM(proto::tcp::WorkResponse, WorkResponse)

#undef TYPE_TO_ENUM

template <>
std::string ToBytesPrivate(const proto::udp::DiscoveryRequest &obj) {
    return "";
}

template <>
void FromBytesPrivate(std::istream &in, proto::udp::DiscoveryRequest &res) {}

template <>
std::string ToBytesPrivate(const proto::udp::DiscoveryResponse &obj) {
    return ToBytes(obj.workerTCPPort);
}

template <>
void FromBytesPrivate(std::istream &in, proto::udp::DiscoveryResponse &res) {
    FromBytes(in, res.workerTCPPort);
}

template <>
std::string ToBytesPrivate(const proto::tcp::StartRequest &obj) {
    return ToBytes(obj.sessionId) + ToBytes(obj.workDesc);
}

template <>
void FromBytesPrivate(std::istream &in, proto::tcp::StartRequest &res) {
    FromBytes(in, res.sessionId);
    FromBytes(in, res.workDesc);
}

template <>
std::string ToBytesPrivate(const proto::tcp::WorkRequest &obj) {
    return ToBytes(obj.segmentIndixes);
}

template <>
void FromBytesPrivate(std::istream &in, proto::tcp::WorkRequest &res) {
    FromBytes(in, res.segmentIndixes);
}

template <>
std::string ToBytesPrivate(const proto::tcp::Heartbeat &obj) {
    return ToBytes(obj.workHash);
}

template <>
void FromBytesPrivate(std::istream &in, proto::tcp::Heartbeat &res) {
    FromBytes(in, res.workHash);
}

template <>
std::string ToBytesPrivate(const proto::tcp::WorkResponse &obj) {
    return ToBytes(obj.workHash) + ToBytes(obj.segmentIndex) + ToBytes(obj.result);
}

template <>
void FromBytesPrivate(std::istream &in, proto::tcp::WorkResponse &res) {
    FromBytes(in, res.workHash);
    FromBytes(in, res.segmentIndex);
    FromBytes(in, res.result);
}

} // namespace

#define MakeToBytes(type)                                                                          \
    template <>                                                                                    \
    std::string ToBytes(const type &obj) {                                                         \
        return ToBytes(TypeToEnumVal<type>()) + ToBytesPrivate(obj);                               \
    };

MakeToBytes(proto::udp::DiscoveryRequest);
MakeToBytes(proto::udp::DiscoveryResponse);
MakeToBytes(proto::tcp::StartRequest);
MakeToBytes(proto::tcp::WorkRequest);
MakeToBytes(proto::tcp::Heartbeat);
MakeToBytes(proto::tcp::WorkResponse);

#define CHECK_MESSAGE_TYPE(type, name)                                                             \
    case MessageType::name:                                                                        \
        res.emplace<type>();                                                                       \
        FromBytesPrivate(in, std::get<type>(res));                                                 \
        break;

template <>
void FromBytes(std::istream &in, proto::SomeMessage &res) {
    MessageType type;
    FromBytes(in, type);
    switch (type) {
        CHECK_MESSAGE_TYPE(proto::udp::DiscoveryRequest, DiscoveryRequest)
        CHECK_MESSAGE_TYPE(proto::udp::DiscoveryResponse, DiscoveryResponse)
        CHECK_MESSAGE_TYPE(proto::tcp::StartRequest, StartRequest)
        CHECK_MESSAGE_TYPE(proto::tcp::WorkRequest, WorkRequest)
        CHECK_MESSAGE_TYPE(proto::tcp::Heartbeat, Heartbeat)
        CHECK_MESSAGE_TYPE(proto::tcp::WorkResponse, WorkResponse)
    default:
        break;
    }
}

#undef CHECK_MESSAGE_TYPE

} // namespace serialize

} // namespace hw1
