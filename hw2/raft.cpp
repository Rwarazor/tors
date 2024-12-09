#include "raft.h"

#include "raft.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <memory>
#include <stdexcept>
#include <thread>

namespace hw2 {

namespace raft {

class NodeImpl {
  public:
    NodeImpl(Node::Id id) : id_(id) {};

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
        throw std::runtime_error("Not fully implemented NodeImpl::Initialize");
    }

    void Work() { throw std::runtime_error("Not implemented NodeImpl::Work"); }

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
