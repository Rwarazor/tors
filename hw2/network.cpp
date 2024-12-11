#include "network.h"

#include "raft.grpc.pb.h"

#include <grpcpp/client_context.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/server_callback.h>

#include <memory>
#include <string>
#include <utility>

namespace hw2 {

namespace network {

Node::Node(Id id) : id_(id) {
    Serve();
}

void Node::SendRequestVote(Id to, ::raft::proto::RequestVoteRequest &request,
                           Node::SendRequestVoteCallbackT callback) {
    class Reader {
      public:
        Reader(::raft::proto::Node::Stub *stub, ::raft::proto::RequestVoteRequest *request,
               Node::SendRequestVoteCallbackT callback)
            : callback_(std::move(callback)) {
            stub->async()->RequestVote(&context_, request, &response_, [this](grpc::Status status) {
                callback_(status, response_);
                delete this;
            });
        }

      private:
        Node::SendRequestVoteCallbackT callback_;

        ::raft::proto::RequestVoteResponse response_;
        grpc::ClientContext context_{};
    };
    TryEnsureConnection(to);
    request.set___id(id_);
    new Reader(stubs_.at(to).get(), &request, std::move(callback));
}

void Node::SendAppendEntries(Id to, ::raft::proto::AppendEntriesRequest &request,
                             Node::SendAppendEntriesCallbackT callback) {
    class Reader {
      public:
        Reader(::raft::proto::Node::Stub *stub, ::raft::proto::AppendEntriesRequest *request,
               Node::SendAppendEntriesCallbackT callback)
            : callback_(std::move(callback)) {
            stub->async()->AppendEntries(&context_, request, &response_,
                                         [this](grpc::Status status) {
                                             callback_(status, response_);
                                             delete this;
                                         });
        }

      private:
        Node::SendAppendEntriesCallbackT callback_;

        ::raft::proto::AppendEntriesResponse response_;
        grpc::ClientContext context_{};
    };
    TryEnsureConnection(to);
    request.set___id(id_);
    new Reader(stubs_.at(to).get(), &request, std::move(callback));
}

grpc::ServerUnaryReactor *Node::RequestVote(grpc::CallbackServerContext *context,
                                            const ::raft::proto::RequestVoteRequest *request,
                                            ::raft::proto::RequestVoteResponse *response) {
    class Reactor : public grpc::ServerUnaryReactor {
      public:
        Reactor(Node *node, const ::raft::proto::RequestVoteRequest *request,
                ::raft::proto::RequestVoteResponse *response) {
            node->HandleRequestVote(request->__id(), request, response);
            Finish(grpc::Status::OK);
        }

      private:
        void OnDone() override { delete this; }
    };
    return new Reactor(this, request, response);
}

grpc::ServerUnaryReactor *Node::AppendEntries(grpc::CallbackServerContext *context,
                                              const ::raft::proto::AppendEntriesRequest *request,
                                              ::raft::proto::AppendEntriesResponse *response) {
    class Reactor : public grpc::ServerUnaryReactor {
      public:
        Reactor(Node *node, const ::raft::proto::AppendEntriesRequest *request,
                ::raft::proto::AppendEntriesResponse *response) {
            node->HandleAppendEntries(request->__id(), request, response);
            Finish(grpc::Status::OK);
        }

      private:
        void OnDone() override { delete this; }
    };
    return new Reactor(this, request, response);
}

void Node::TryEnsureConnection(Id otherId) {
    if (stubs_.contains(otherId)) {
        return;
    }
    int port = 50000 + otherId;
    auto channel = grpc::CreateChannel("localhost:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    stubs_.emplace(otherId, ::raft::proto::Node::NewStub(channel));
}

void Node::Serve() {
    int port = 50000 + id_;
    std::string server_address("0.0.0.0:" + std::to_string(port));
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    server_ = builder.BuildAndStart();
    servingThread_ = std::thread([&]() {
        std::cout << "Serving thread started!\n";
        server_->Wait();
    });
}

} // namespace network
} // namespace hw2