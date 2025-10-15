#pragma once

#include <grpcpp/grpcpp.h>
#include "raft.pb.h"
#include "raft.grpc.pb.h"
#include "rafty/raft.hpp"

using grpc::Status;
using grpc::ServerContext;
namespace rafty {
class Raft;
class RaftServiceImpl final : public raftpb::RaftService::Service {
public:
    explicit RaftServiceImpl(Raft* raft_instance) : raft_(raft_instance) {}

    Status AppendEntries(ServerContext* context, const raftpb::AppendEntriesRequest* request, raftpb::AppendEntriesResponse* response) override;
    Status RequestVote(ServerContext* context, const raftpb::RequestVoteRequest* request, raftpb::RequestVoteResponse* response) override;

    Raft* raft_;
};

}