#pragma once

#include "raft.grpc.pb.h"
#include "raft.pb.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>

#include <functional>
#include <map>
#include <memory>
#include <thread>

namespace hw2 {

namespace network {

class Node : ::raft::proto::Node::CallbackService {
  public:
    using Id = size_t;

    Node(Id id);

    using SendRequestVoteCallbackT =
        std::function<void(grpc::Status, ::raft::proto::RequestVoteResponse)>;
    void SendRequestVote(Id to, ::raft::proto::RequestVoteRequest &request,
                         SendRequestVoteCallbackT callback);

    using SendAppendEntriesCallbackT =
        std::function<void(grpc::Status, ::raft::proto::AppendEntriesResponse)>;
    void SendAppendEntries(Id to, ::raft::proto::AppendEntriesRequest &request,
                           SendAppendEntriesCallbackT callback);

    void virtual HandleRequestVote(Id from, const ::raft::proto::RequestVoteRequest *request,
                                   ::raft::proto::RequestVoteResponse *response) = 0;

    void virtual HandleAppendEntries(Id from, const ::raft::proto::AppendEntriesRequest *request,
                                     ::raft::proto::AppendEntriesResponse *response) = 0;
    virtual ~Node() = default;

  protected:
    Id id_;

  private:
    grpc::ServerUnaryReactor *RequestVote(grpc::CallbackServerContext *context,
                                          const ::raft::proto::RequestVoteRequest *request,
                                          ::raft::proto::RequestVoteResponse *response) override;

    grpc::ServerUnaryReactor *
    AppendEntries(grpc::CallbackServerContext *context,
                  const ::raft::proto::AppendEntriesRequest *request,
                  ::raft::proto::AppendEntriesResponse *response) override;

    void TryEnsureConnection(Id otherId);

    void Serve();

    std::unique_ptr<grpc::Server> server_;
    std::thread servingThread_;
    std::map<Id, std::unique_ptr<::raft::proto::Node::Stub>> stubs_;
};

} // namespace network

} // namespace hw2
