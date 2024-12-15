#pragma once

#include <functional>
#include <memory>
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

constexpr size_t TOTAL_IDS = 3;

class Node {
  public:
    using Id = size_t;

    Node(Id id);

    Id ThisId();
    std::optional<Id> LeaderId();

    std::optional<ValT> Get(KeyT key);

    enum class SetStatus {
        OK_DONE,              // write is persisted
        OK_NOT_LEADER,        // no work done, node is not the leader
        ERR_NO_LEADER,        // no active leader
        ERR_NO_LONGER_LEADER, // node was a leader on the start of the request, but converted to
                              // follower
    };
    using CallbackSetT = std::function<void(SetStatus)>;
    void Set(KeyT key, ValT val, CallbackSetT callback);
    void Delete(KeyT key, CallbackSetT callback);
    void Update(KeyT key, UpdateData data, CallbackSetT callback);

  private:
    std::shared_ptr<NodeImpl> impl_;
};

} // namespace raft

} // namespace hw2
