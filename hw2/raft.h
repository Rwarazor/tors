#pragma once

#include <filesystem>
#include <functional>
#include <optional>
#include <string>

namespace hw2 {

namespace raft {

// State machine is a key-value map
using KeyT = std::string;
using ValT = std::string;

// Partial updates (only for JSON value)
struct UpdateData {
    std::string path;
    std::string partialVal;
};

class NodeImpl;

class Node {
  public:
    using Id = size_t;

    Node(Id id);

    Id ThisId();
    std::optional<Id> LeaderId();

    enum class GetStatus {
        OK_DONE,       // returned node's view of key
        ERR_STARTING,  // node is acquiring wal after startup
        ERR_NO_LEADER, // no active leader
    };
    using CallbackGetT = std::function<void(GetStatus, std::optional<ValT>)>;
    void Get(KeyT key, CallbackGetT callback);

    enum class WriteStatus {
        OK_DONE,           // write is persisted
        OK_NOT_LEADER,     // no work done, node is not the leader
        ERR_KEY_NOT_FOUND, // delete or update addressing nonexistent key
        ERR_NO_LEADER,     // no active leader
    };
    using CallbackWriteT = std::function<void(WriteStatus)>;
    void Set(KeyT key, ValT val, CallbackWriteT callback);
    void Delete(KeyT key, CallbackWriteT callback);
    void Update(KeyT key, UpdateData data, CallbackWriteT callback);

  private:
    std::shared_ptr<NodeImpl> impl_;
};

} // namespace raft

} // namespace hw2
