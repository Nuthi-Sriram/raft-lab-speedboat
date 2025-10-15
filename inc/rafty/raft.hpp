#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"

#include "common/utils/net_intercepter.hpp"

#include "toolings/msg_queue.hpp"

#include "rafty/raft_service_impl.hpp"

#include <atomic>
#include <chrono>
#include <random>
#include <thread>

#include "raft.grpc.pb.h" // it will pick up correct header
                          // when you generate the grpc proto files

using namespace toolings;

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

enum RaftState {
  Follower,
  Candidate,
  Leader
};

class Raft {
  friend class RaftServiceImpl;
public:
  // WARN: do not modify the signature of constructor and destructor
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run();                                      // lab 1
  State get_state() const;                         // lab 1
  ProposalResult propose(const std::string &data); // lab 2

  // WARN: do not modify the signature of
  // `start_server`, `stop_server`, `connect_peers`,
  // `is_dead`, and `kill`.
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();
  grpc::Status RequestVote(const raftpb::RequestVoteRequest* request, raftpb::RequestVoteResponse* response);

private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;

  std::unique_ptr<RaftServiceImpl> service_;
  // invoke `apply` when the command/proposal is ready to apply.
  void apply(const ApplyResult &result);

protected:
  // WARN: do not modify `mtx`.
  mutable std::mutex mtx;

private:
  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;

  // logger is available for you to logging information.
  std::unique_ptr<rafty::utils::logger> logger;

  // New members for heartbeat and election timers

  // Helper functions
  void main_loop();
  void start_election();
  void become_follower(uint64_t term);
  void become_candidate(uint64_t term, uint64_t voted_for = -1);
  void become_leader();
  void send_heartbeats();
  static std::chrono::milliseconds get_random_election_timeout(
    std::chrono::milliseconds min, std::chrono::milliseconds max);
  static std::string RaftStateToString(RaftState state);
  
  void reset_state();
  void update_latest_heartbeat();
  bool is_up_to_date(int last_log_term, int last_log_index) const;

  // Raft state_
  RaftState state_;
  uint64_t current_term_;
  uint64_t votes_received_;
  uint64_t last_log_index_;
  uint64_t last_log_term_;
  uint64_t leader_commit_index_;
  int64_t voted_for_;
  // std::vector<raftpb::LogEntry> log_;

  // Leader state_
  std::unordered_map<uint64_t, uint64_t> next_index_;
  std::unordered_map<uint64_t, uint64_t> match_index_;

  // Timers
  static std::chrono::milliseconds heartbeat_interval_;
  static std::chrono::milliseconds min_election_timeout_;
  static std::chrono::milliseconds max_election_timeout_;
  std::chrono::steady_clock::time_point last_heartbeat_;;
  std::chrono::milliseconds election_timeout_;

  // Threads
  std::thread main_loop_thread_;
  std::vector<std::thread> heartbeat_threads_;
  std::vector<std::thread> election_threads_;
};
} // namespace rafty

#include "rafty/impl/raft.ipp"
