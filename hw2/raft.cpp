#include "raft.h"
#include "network.h"

#include "raft.pb.h"

#include <grpcpp/support/status.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace hw2 {

namespace raft {

class NodeImpl {
    static const Node::Id totalIds = 3;

  public:
    NodeImpl(Node::Id id) : networkNode_(*this, id), id_(id) {
        Initialize();
        std::cout << "Raft node initialized!\n";
    };

    Node::Id ThisId() { return id_; }
    std::optional<Node::Id> LeaderId() { return leaderId_; }

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
    void Initialize() {
        workThread_ = std::thread{[this]() {
            while (doWork_) {
                Work();
            }
        }};
        std::cout << "Work thread started!\n";
    }

    void Work() {
        while (true) {
            std::cout << "Sending heartbeats!\n";
            for (Node::Id id = 1; id <= totalIds; ++id) {
                if (id != id_) {
                    std::mutex mu;
                    std::condition_variable cv;
                    bool done = false;
                    ::raft::proto::RequestVoteRequest request;
                    request.set_term(id_ * 10 + id);
                    networkNode_.SendRequestVote(
                        id, request,
                        [&](grpc::Status status, ::raft::proto::RequestVoteResponse response) {
                            if (status.ok()) {
                                std::cout << "Succesfully performed RequestVote rpc on node " << id
                                          << '\n';
                                std::cout << "Response" << response.term() << " from node " << id
                                          << '\n';
                            } else {
                                std::cout << "Couldn't perform RequestVote rpc on node " << id
                                          << '\n';
                            }
                            std::lock_guard lock(mu);
                            done = true;
                            cv.notify_one();
                        });
                    std::unique_lock lock(mu);
                    cv.wait(lock, [&done] { return done; });
                }
            }
            sleep(5);
        }
        throw std::runtime_error("Not implemented NodeImpl::Work");
    }

    class NetworkNodeImpl : public network::Node {
      public:
        NetworkNodeImpl(NodeImpl &raftNode, network::Node::Id id)
            : network::Node(id), raftNode_(raftNode) {
            std::cout << "Network service-client initialized!\n";
        }

        void HandleRequestVote(Id from, const ::raft::proto::RequestVoteRequest *request,
                               ::raft::proto::RequestVoteResponse *response) override {
            std::cout << "Received heartbeat from " << from << "!\n";
            response->set_term(request->term() * 100 + 10 * id_ + from);
            // throw std::runtime_error("Not implemented NetworkNodeImpl::HandleRequestVote");
        };

        void HandleAppendEntries(Id from, const ::raft::proto::AppendEntriesRequest *request,
                                 ::raft::proto::AppendEntriesResponse *response) override {
            throw std::runtime_error("Not implemented NetworkNodeImpl::HandleAppendEntries");
        }

      private:
        NodeImpl &raftNode_;
    } networkNode_;

    Node::Id id_;
    std::optional<Node::Id> leaderId_;

    std::map<KeyT, ValT> state_;
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
