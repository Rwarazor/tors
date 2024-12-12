#include "raft.h"
#include "network.h"

#include "raft.pb.h"

#include <cassert>
#include <chrono>
#include <cstddef>
#include <grpcpp/support/status.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>

namespace hw2 {

namespace raft {

class NodeImpl {
    static constexpr auto ELECTION_TIMEOUT = std::chrono::seconds(5);
    static constexpr auto HEARTBEAT_INTERVAL = std::chrono::seconds(1);
    static constexpr Node::Id TOTAL_IDS = 3;

  public:
    NodeImpl(Node::Id id) : networkNode_(*this, id), id_(id) {
        Initialize();
        std::cout << "Raft node initialized!\n";
    };

    Node::Id ThisId() { return id_; }

    std::optional<Node::Id> LeaderId() {
        switch (status_) {
        case Status::LEADER:
        case Status::FOLLOWER:
            return votedFor;
        default:
            return std::nullopt;
        }
    }

    void Get(KeyT key, Node::CallbackGetT callback) {
        throw std::runtime_error("Not implemented NodeImpl::Get");
    }
    void Set(KeyT key, ValT val, Node::CallbackWriteT callback) {
        throw std::runtime_error("Not implemented NodeImpl::Set");
    }
    void Delete(KeyT key, Node::CallbackWriteT callback) {
        throw std::runtime_error("Not implemented NodeImpl::Delete");
    }
    void Update(KeyT key, UpdateData data, Node::CallbackWriteT callback) {
        throw std::runtime_error("Not implemented NodeImpl::Update");
    }

    ~NodeImpl() {
        doWork_ = false;
        workThread_.join();
    }

  private:
    void Apply(::raft::proto::WALEntry entry) {
        throw std::runtime_error("Not implemented");
        //
    }

    void Heartbeat() {
        throw std::runtime_error("Not implemented");
        //
    }

    void Initialize() {
        workThread_ = std::thread{[this]() {
            while (doWork_) {
                Work();
            }
        }};
        std::cout << "Work thread started!\n";
    }

    void Work() {
        if (status_ != Status::CANDIDATE &&
            std::chrono::system_clock::now() - lastLeaderTime > ELECTION_TIMEOUT) //
        {
            ConvertToCandidate();
        }
        while (commitIndex > lastAppliedIndex) {
            Apply(log[lastAppliedIndex++]);
        }
        if (status_ == Status::LEADER &&
            std::chrono::system_clock::now() - lastHeartbeatTime > HEARTBEAT_INTERVAL) {
            Heartbeat();
        }
    }

    void ConvertToFollower() {
        status_ = Status::FOLLOWER;
        votedFor = std::nullopt;
    }

    void ConvertToCandidate() {
        status_ = Status::CANDIDATE;
        votedFor = ThisId();
        votesReceived = 1;
        throw std::runtime_error("Not implemented");
    }

    class NetworkNodeImpl : public network::Node {
      public:
        NetworkNodeImpl(NodeImpl &raftNode, network::Node::Id id)
            : network::Node(id), raftNode_(raftNode) {
            std::cout << "Network service-client initialized!\n";
        }

        void HandleRequestVote(Id from, const ::raft::proto::RequestVoteRequest *request,
                               ::raft::proto::RequestVoteResponse *response) override {
            if (request->term() < raftNode_.currentTerm) {
                response->set_votegranted(false);
                response->set_term(raftNode_.currentTerm);
                return;
            }
            if (request->term() > raftNode_.currentTerm) {
                raftNode_.currentTerm = request->term();
                raftNode_.ConvertToFollower();
            }
            if (raftNode_.votedFor.value_or(from) == from) {
                if (raftNode_.log.empty() ||
                    std::make_pair(raftNode_.log.back().term(), raftNode_.log.size()) <=
                        std::make_pair(request->lastlogterm(), request->lastlogindex())) //
                {
                    raftNode_.votedFor = from;
                    response->set_votegranted(true);
                    response->set_term(raftNode_.currentTerm);
                    raftNode_.lastLeaderTime = std::chrono::system_clock::now();
                    return;
                }
            }
            response->set_votegranted(false);
            response->set_term(raftNode_.currentTerm);
            return;
        };

        void HandleAppendEntries(Id from, const ::raft::proto::AppendEntriesRequest *request,
                                 ::raft::proto::AppendEntriesResponse *response) override {
            if (request->term() < raftNode_.currentTerm) {
                response->set_success(false);
                response->set_term(raftNode_.currentTerm);
                return;
            }
            if (request->term() > raftNode_.currentTerm) {
                raftNode_.currentTerm = request->term();
                raftNode_.ConvertToFollower();
                raftNode_.votedFor = from;
                raftNode_.lastLeaderTime = std::chrono::system_clock::now();
            }
            if (raftNode_.log.size() < request->prevlogindex() ||
                (request->prevlogindex() != 0 &&
                 raftNode_.log.at(request->prevlogindex() - 1).term())) //
            {
                response->set_success(false);
                response->set_term(raftNode_.currentTerm);
                response->set_lastreplicatedindex(raftNode_.commitIndex);
                return;
            }
            size_t curIndex = request->prevlogindex();
            for (const auto &entry : request->entries()) {
                if (raftNode_.log.size() == curIndex) {
                    raftNode_.log.push_back(entry);
                } else {
                    assert(raftNode_.log.size() > curIndex);
                    if (raftNode_.log.at(curIndex).term() != entry.term()) {
                        raftNode_.log.at(curIndex) = entry;
                    }
                }
                curIndex++;
            }
            if (request->leadercommitindex() > raftNode_.commitIndex) {
                raftNode_.commitIndex = std::min(request->leadercommitindex(), curIndex);
            }
            raftNode_.lastLeaderTime = std::chrono::system_clock::now();
        }

      private:
        NodeImpl &raftNode_;
    } networkNode_;
    // friend class NetworkNodeImpl;

    Node::Id id_;

    std::map<KeyT, ValT> state_;

    enum class Status {
        FOLLOWER,
        CANDIDATE,
        LEADER,
    } status_ = Status::FOLLOWER;

    size_t currentTerm = 0;
    std::optional<Node::Id> votedFor;
    std::vector<::raft::proto::WALEntry> log;
    size_t commitIndex = 0;
    size_t lastAppliedIndex = 0;

    decltype(std::chrono::system_clock::now()) lastLeaderTime = std::chrono::system_clock::now();

    // leaderOnly---
    std::map<Node::Id, size_t> nextIndex;
    std::map<Node::Id, size_t> matchIndex;
    decltype(std::chrono::system_clock::now()) lastHeartbeatTime = std::chrono::system_clock::now();
    // -------------
    // candidateOnly
    std::size_t votesReceived = 0;
    // -------------

    std::atomic_bool doWork_ = true;
    std::thread workThread_;
};

Node::Node(Node::Id id) : impl_(std::make_shared<NodeImpl>(id)) {};

Node::Id Node::ThisId() {
    return impl_->ThisId();
}
std::optional<Node::Id> Node::LeaderId() {
    return impl_->LeaderId();
}

void Node::Get(KeyT key, Node::CallbackGetT callback) {
    impl_->Get(key, callback);
}
void Node::Set(KeyT key, ValT val, Node::CallbackWriteT callback) {
    impl_->Set(key, val, callback);
}
void Node::Delete(KeyT key, Node::CallbackWriteT callback) {
    impl_->Delete(key, callback);
}
void Node::Update(KeyT key, UpdateData data, Node::CallbackWriteT callback) {
    impl_->Update(key, data, callback);
}

} // namespace raft
} // namespace hw2
