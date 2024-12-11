#pragma once

#include "grpcpp/impl/service_type.h"
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

namespace proto = ::raft::proto;

class Node {
  public:
    using Id = size_t;
    using Status = grpc::Status;

    Node(Id id);

    using SendRequestVoteCallbackT = std::function<void(Status, proto::RequestVoteResponse)>;
    void SendRequestVote(Id to, proto::RequestVoteRequest &request,
                         SendRequestVoteCallbackT callback);

    using SendAppendEntriesCallbackT = std::function<void(Status, proto::AppendEntriesResponse)>;
    void SendAppendEntries(Id to, proto::AppendEntriesRequest &request,
                           SendAppendEntriesCallbackT callback);

    void virtual HandleRequestVote(Id from, const proto::RequestVoteRequest *request,
                                   proto::RequestVoteResponse *response) = 0;

    void virtual HandleAppendEntries(Id from, const proto::AppendEntriesRequest *request,
                                     proto::AppendEntriesResponse *response) = 0;
    virtual ~Node() = default;

  protected:
    Id id_;

  private:
    void TryEnsureConnection(Id otherId);

    void Serve();

    std::shared_ptr<grpc::Service> service;
    std::unique_ptr<grpc::Server> server_;
    std::thread servingThread_;
    std::map<Id, std::unique_ptr<proto::Node::Stub>> stubs_;
};

} // namespace network

} // namespace hw2
