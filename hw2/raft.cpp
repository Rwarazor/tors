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
            return votedFor_;
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
        switch (entry.type()) {
        case ::raft::proto::WALEntryType::HEARTBEAT:
            return;
        case ::raft::proto::WALEntryType::SET:
            state_[entry.key()] = entry.val();
            return;
        case ::raft::proto::WALEntryType::DELETE:
            state_.erase(entry.key());
            return;
        case ::raft::proto::WALEntryType::UPDATE:
            throw std::runtime_error("Not implemented");
        default:
            throw std::runtime_error(
                "Unexpected enum (::raft::proto::WALEntry) value at NodeImpl::Apply");
        };
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
        std::lock_guard lg(mutex_);
        if (status_ != Status::LEADER &&
            std::chrono::system_clock::now() - lastLeaderTime_ > ELECTION_TIMEOUT) //
        {
            ConvertToCandidate();
        }
        while (commitIndex_ > lastAppliedIndex_) {
            Apply(log_[lastAppliedIndex_++]);
        }
        if (status_ == Status::LEADER) {
            if (std::chrono::system_clock::now() - lastHeartbeatTime_ > HEARTBEAT_INTERVAL) {
                Heartbeat();
            }
            UpdateCommitIndex();
        }
    }

    void ConvertToFollower() {
        std::cout << "Converting to follower\n";
        status_ = Status::FOLLOWER;
        votedFor_ = std::nullopt;
    }

    void ConvertToCandidate() {
        std::cout << "Converting to candidate, new term: " << ++currentTerm_ << "\n";
        status_ = Status::CANDIDATE;
        lastLeaderTime_ = std::chrono::system_clock::now();
        votedFor_ = ThisId();
        votesReceived_ = 1;
        ::raft::proto::RequestVoteRequest request;
        request.set_term(currentTerm_);
        request.set_lastlogterm(log_.empty() ? 0 : log_.back().term());
        request.set_lastlogindex(log_.size());
        for (Node::Id id = 1; id <= TOTAL_IDS; ++id) {
            if (id != ThisId()) {
                networkNode_.SendRequestVote(
                    id, request,
                    [this](network::Status status,
                           const ::raft::proto::RequestVoteResponse &response) {
                        std::lock_guard lg(mutex_);
                        if (status.ok()) {
                            if (response.term() > currentTerm_) {
                                currentTerm_ = response.term();
                                ConvertToFollower();
                            }
                            if (response.votegranted() && status_ == Status::CANDIDATE &&
                                response.term() == currentTerm_) {
                                // we check status && term because we might have recieved a delayed
                                // vote from previous election
                                votesReceived_++;
                                if (votesReceived_ * 2 > TOTAL_IDS) {
                                    ConvertToLeader();
                                }
                            }
                        }
                    });
            }
        }
    }

    void ConvertToLeader() {
        std::cout << "Converting to leader on term " << currentTerm_ << "\n";
        status_ = Status::LEADER;
        nextIndex_.clear();
        matchIndex_.clear();
        Heartbeat(true);
    }

    void Heartbeat(bool initialHeartbeat=false) {
        std::cout << "Sending heartbeat\n";
        ::raft::proto::WALEntry entry;
        entry.set_type(::raft::proto::WALEntryType::HEARTBEAT);
        entry.set_term(currentTerm_);
        WriteToWAL(entry, !initialHeartbeat);
    }

    void WriteToWAL(const ::raft::proto::WALEntry &entry, bool sendEntries = true) {
        lastHeartbeatTime_ = std::chrono::system_clock::now();
        log_.push_back(entry);
        ::raft::proto::AppendEntriesRequest request;
        request.set_term(currentTerm_);
        request.set_leadercommitindex(commitIndex_);
        for (Node::Id id = 1; id <= TOTAL_IDS; ++id) {
            if (id != ThisId()) {
                request.set_prevlogindex(nextIndex_[id]);
                request.set_lastlogterm(nextIndex_[id] == 0 ? 0 : log_[nextIndex_[id] - 1].term());
                request.clear_entries();
                if (sendEntries) {
                    for (size_t i = nextIndex_[id]; i < log_.size(); ++i) {
                        request.mutable_entries()->Add(::raft::proto::WALEntry(entry));
                    }
                    nextIndex_[id] = log_.size();
                }
                std::cout << "Sending " << request.entries_size() << " entries to node " << id
                          << "\n";
                networkNode_.SendAppendEntries(
                    id, request,
                    [lastindex = nextIndex_[id], id,
                     this](network::Status status,
                           const ::raft::proto::AppendEntriesResponse &response) {
                        std::lock_guard lg(mutex_);
                        if (status.ok()) {
                            if (response.term() > currentTerm_) {
                                currentTerm_ = response.term();
                                ConvertToFollower();
                                return;
                            }
                            if (status_ == Status::LEADER && response.term() == currentTerm_) {
                                if (response.success()) {
                                    matchIndex_[id] = lastindex;
                                } else {
                                    nextIndex_[id] = response.lastreplicatedindex();
                                    matchIndex_[id] =
                                        std::max(matchIndex_[id], response.lastreplicatedindex());
                                }
                            }
                        }
                    });
            }
        }
    }

    void UpdateCommitIndex() {
        std::vector<size_t> indexes;
        for (Node::Id id = 1; id <= TOTAL_IDS; ++id) {
            if (id == ThisId()) {
                indexes.push_back(log_.size());
            } else {
                indexes.push_back(matchIndex_[id]);
            }
        }
        std::sort(indexes.begin(), indexes.end());
        size_t newCommitIndex = indexes[TOTAL_IDS / 2];
        if (newCommitIndex > commitIndex_) {
            std::cout << "New entries commited! Total commited:" << newCommitIndex
                      << ", previous: " << commitIndex_ << '\n';
            commitIndex_ = newCommitIndex;
        }
    };

    class NetworkNodeImpl : public network::Node {
      public:
        NetworkNodeImpl(NodeImpl &raftNode, network::Node::Id id)
            : network::Node(id), raftNode_(raftNode) {
            std::cout << "Network service-client initialized!\n";
        }

        void HandleRequestVote(Id from, const ::raft::proto::RequestVoteRequest *request,
                               ::raft::proto::RequestVoteResponse *response) override {
            std::lock_guard lg(raftNode_.mutex_);
            std::cout << "RequestVote RPC received\n";
            if (request->term() < raftNode_.currentTerm_) {
                response->set_votegranted(false);
                response->set_term(raftNode_.currentTerm_);
                std::cout << "RequestVote RPC declined too small term\n";
                return;
            }
            if (request->term() > raftNode_.currentTerm_) {
                raftNode_.currentTerm_ = request->term();
                raftNode_.ConvertToFollower();
            }
            if (raftNode_.votedFor_.value_or(from) == from) {
                if (raftNode_.log_.empty() ||
                    std::make_pair(raftNode_.log_.empty() ? 0 : raftNode_.log_.back().term(),
                                   raftNode_.log_.size()) <=
                        std::make_pair(request->lastlogterm(), request->lastlogindex())) //
                {
                    raftNode_.votedFor_ = from;
                    response->set_votegranted(true);
                    response->set_term(raftNode_.currentTerm_);
                    raftNode_.lastLeaderTime_ = std::chrono::system_clock::now();
                    std::cout << "RequestVote RPC accepted\n";
                    return;
                }
            }
            response->set_votegranted(false);
            response->set_term(raftNode_.currentTerm_);
            std::cout << raftNode_.log_.size() << "  " << (raftNode_.log_.empty() ? 0 : raftNode_.log_.back().term()) << "\n";
            std::cout << request->lastlogindex() << "  " << request->lastlogterm() << "\n";
            std::cout << "RequestVote RPC declined too old log\n";
            return;
        };

        void HandleAppendEntries(Id from, const ::raft::proto::AppendEntriesRequest *request,
                                 ::raft::proto::AppendEntriesResponse *response) override {
            std::lock_guard lg(raftNode_.mutex_);
            std::cout << "AppendEntries RPC received\n";
            if (request->term() < raftNode_.currentTerm_) {
                response->set_success(false);
                response->set_term(raftNode_.currentTerm_);
                std::cout << "AppendEntries RPC declined too small term\n";
                return;
            }
            if (request->term() > raftNode_.currentTerm_) {
                raftNode_.currentTerm_ = request->term();
                raftNode_.ConvertToFollower();
                raftNode_.votedFor_ = from;
            }
            raftNode_.lastLeaderTime_ = std::chrono::system_clock::now();
            if (raftNode_.log_.size() < request->prevlogindex() ||
                (request->prevlogindex() != 0 &&
                 raftNode_.log_.at(request->prevlogindex() - 1).term() != request->lastlogterm())) //
            {
                response->set_success(false);
                response->set_term(raftNode_.currentTerm_);
                response->set_lastreplicatedindex(raftNode_.commitIndex_);
                std::cout << "AppendEntries RPC declined log gap\n";
                return;
            }
            size_t curIndex = request->prevlogindex();
            for (const auto &entry : request->entries()) {
                if (raftNode_.log_.size() == curIndex) {
                    raftNode_.log_.push_back(entry);
                } else {
                    assert(raftNode_.log_.size() > curIndex);
                    if (raftNode_.log_.at(curIndex).term() != entry.term()) {
                        raftNode_.log_.at(curIndex) = entry;
                    }
                }
                curIndex++;
            }
            if (request->leadercommitindex() > raftNode_.commitIndex_) {
                raftNode_.commitIndex_ = std::min(request->leadercommitindex(), curIndex);
            }
            raftNode_.lastLeaderTime_ = std::chrono::system_clock::now();
            response->set_success(true);
            response->set_term(raftNode_.currentTerm_);
            response->set_lastreplicatedindex(raftNode_.commitIndex_);
            std::cout << "AppendEntries RPC accepted new log size: " << raftNode_.log_.size()
                      << "\n";
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

    size_t currentTerm_ = 0;
    std::optional<Node::Id> votedFor_;
    std::vector<::raft::proto::WALEntry> log_;
    size_t commitIndex_ = 0;
    size_t lastAppliedIndex_ = 0;

    decltype(std::chrono::system_clock::now()) lastLeaderTime_ = std::chrono::system_clock::now();

    // leaderOnly---
    std::map<Node::Id, size_t> nextIndex_;
    std::map<Node::Id, size_t> matchIndex_;
    decltype(std::chrono::system_clock::now()) lastHeartbeatTime_ =
        std::chrono::system_clock::now();
    // -------------
    // candidateOnly
    std::size_t votesReceived_ = 0;
    // -------------

    std::atomic_bool doWork_ = true;
    std::thread workThread_;
    std::mutex mutex_;
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
