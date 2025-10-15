#include <iostream>
#include <memory>

#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"

namespace rafty
{
	std::chrono::milliseconds Raft::heartbeat_interval_ = std::chrono::milliseconds(50);
	std::chrono::milliseconds Raft::min_election_timeout_ = std::chrono::milliseconds(200);
	std::chrono::milliseconds Raft::max_election_timeout_ = std::chrono::milliseconds(600);

	Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
			: id(config.id),
				listening_addr(config.addr),
				peer_addrs(config.peer_addrs),
				dead(false),
				ready_queue(ready),
				logger(utils::logger::get_logger(id)),

				// TODO: add more field if desired
				election_timeout_(get_random_election_timeout(Raft::min_election_timeout_, Raft::max_election_timeout_)),
				current_term_(0),
				voted_for_(-1),
				votes_received_(0),
				last_log_index_(0),
				last_log_term_(0),
				leader_commit_index_(0),
				state_(RaftState::Follower),
				heartbeat_threads_(std::vector<std::thread>()),
				election_threads_(std::vector<std::thread>()),
				last_heartbeat_(std::chrono::steady_clock::now())
	{
		// Additional initialization if needed
	}

	Raft::~Raft() { this->stop_server(); }

	void Raft::run()
	{
		// TODO: kick off the raft instance
		// Note: this function should be non-blocking

		// lab 1
		this->main_loop_thread_ = std::thread([this]
																					{ this->main_loop(); });
	}

	// Main loop implementatation
	void Raft::main_loop()
	{
		std::chrono::milliseconds elapsed;
		while (!dead.load())
		{
			auto timer = std::chrono::steady_clock::now();
			{
				std::lock_guard<std::mutex> lock(mtx);
				std::chrono::milliseconds elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_heartbeat_);
				if (elapsed > election_timeout_ && state_ == RaftState::Follower)
				{
					// logger->info("Server {} election timeout for term {}", id, current_term_);
					become_candidate(++current_term_, -1);
					continue;
				}
			}

			{
				std::lock_guard<std::mutex> lock(mtx);
				std::chrono::milliseconds elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_heartbeat_);
				if (elapsed > election_timeout_ && state_ == RaftState::Candidate)
				{
					// logger->info("Server {} election timeout for term {}. Restarting Elections for next term", id, current_term_);
					become_candidate(++current_term_, -1);
					continue;
				}
			}

			{
				std::lock_guard<std::mutex> lock(mtx);
				std::chrono::milliseconds elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - last_heartbeat_);
				if (elapsed > heartbeat_interval_ && state_ == RaftState::Leader)
				{
					send_heartbeats();
					update_latest_heartbeat();
				}
			}

			// logger->info("Server {} is in state {} for term {}", id, RaftStateToString(state_), current_term_);

			auto timer_end = std::chrono::steady_clock::now();
			std::this_thread::sleep_for(std::chrono::milliseconds(20 - (std::chrono::duration_cast<std::chrono::milliseconds>(timer_end - timer)).count()));
		}
	}

	State Raft::get_state() const
	{
		return State{
				static_cast<unsigned long>(current_term_),
				state_ == RaftState::Leader};
	}

	ProposalResult Raft::propose(const std::string &data)
	{
		// TODO: finish it
		// lab 2
	}

	// TODO: add more functions if desired.
	std::chrono::milliseconds Raft::get_random_election_timeout(std::chrono::milliseconds min, std::chrono::milliseconds max)
	{
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<int> dis((int)min.count(), (int)max.count());
		return std::chrono::milliseconds(dis(gen));
	}

	std::string Raft::RaftStateToString(RaftState state)
	{
		switch (state)
		{
		case Follower:
			return "Follower";
		case Candidate:
			return "Candidate";
		case Leader:
			return "Leader";
		default:
			return "Unknown";
		}
	}

	void Raft::start_election()
	{
		// logger->info("Entered start election for Server {} Term {}", id, current_term_);
		logger->info("Starting election for term {}", current_term_);

		for (const auto &[peer_id, stub] : peers_)
		{
			if (dead.load())
				break;

			raftpb::RequestVoteRequest request;
			request.set_term(current_term_);
			request.set_candidateid(id);
			request.set_lastlogindex(last_log_index_);
			request.set_lastlogterm(last_log_term_);

			election_threads_.emplace_back(
					std::thread([this, peer_id, stub = stub.get(), request]() mutable
											{
					if (dead.load()) return;

					raftpb::RequestVoteResponse response;
					auto context = create_context(peer_id);
					grpc::Status status = stub->RequestVote(&*context, request, &response);

					if (dead.load()) return;
					if (status.ok()) {
						if (response.term() > current_term_) {
							become_follower(response.term());
							return;
						}

						if (!response.votegranted()) {
							return;
						}

						// logger->info("Vote granted by peer {} for term {}", peer_id, current_term_);						
						votes_received_++;
						if (state_ == RaftState::Candidate) {
							// If this is the first vote received, add itself as well.
							// Now cannot vote anyone else
							if (voted_for_ == -1) {
								// Vote itself
								votes_received_++;
								voted_for_ = id;
							}
							if (votes_received_ > ((peers_.size() + 1) / 2)) {
								logger->info("Server won election for term {} by receiving {} out of {} votes.", current_term_, votes_received_, (peers_.size() + 1));
								become_leader();
								return;
							}
						}
					} else {
						// logger->warn("RequestVote RPC request to Server {} failed: {}", peer_id, status.error_message());
					} }));
		}
	}

	void Raft::become_follower(uint64_t term)
	{
		// logger->info("Server {} becoming follower for term {}", id, current_term_);
		state_ = RaftState::Follower;
		reset_state();
		current_term_ = term;
	}

	void Raft::become_candidate(uint64_t term, uint64_t voted_for)
	{
		// logger->info("Server {} becoming candidate for term {}", id, current_term_);
		state_ = RaftState::Candidate;
		reset_state();
		current_term_ = term;
		voted_for_ = voted_for;
		if (voted_for == id)
			votes_received_++;
		start_election();
	}

	void Raft::become_leader()
	{
		// logger->info("Server {} becoming leader for term {}", id, current_term_);
		state_ = RaftState::Leader;
		reset_state();
	}

	void Raft::reset_state()
	{
		votes_received_ = 0;
		voted_for_ = -1;
		election_timeout_ = get_random_election_timeout(min_election_timeout_, max_election_timeout_);
		update_latest_heartbeat();
	}

	void Raft::send_heartbeats()
	{
		// logger->info("Sending heartbeats from Server {} for term {}", id, current_term_);
		for (const auto &peer : peers_)
		{
			if (dead.load())
				return;
			heartbeat_threads_.emplace_back(
					std::thread([this, &peer]() mutable
											{
			if (dead.load()) return;
			raftpb::AppendEntriesRequest request;
			request.set_term(current_term_);
			request.set_leaderid(id);
			request.set_prevlogindex(last_log_index_);
			request.set_prevlogterm(last_log_term_);
			// request.set_entries();
			request.set_leadercommit(leader_commit_index_);

			raftpb::AppendEntriesResponse response;
			auto context = create_context(peer.first);

			grpc::Status status = peer.second->AppendEntries(&*context, request, &response);

			if (status.ok()) {
				if (response.term() > current_term_) {
					// logger->info("AppendEntriesRPC request to Server {} failed for term {}, received term {}. Becoming follower", peer.first, current_term_, response.term());
					become_follower(response.term());
					return;
				}
				// TODO: Later prev Index logging mechanism to be implemented here
				// logger->info("AppendEntriesRPC request to Server {} succeeded", peer.first);
			} else {
				// logger->warn("AppendEntriesRPC request to Server {} failed: {}", peer.first, status.error_message());
			} }));
		}
	}

	void Raft::update_latest_heartbeat()
	{
		last_heartbeat_ = std::chrono::steady_clock::now();
		// logger->info("Updating latest heartbeat for Server {} for term {} at {}", id, current_term_, last_heartbeat_.time_since_epoch().count());
	}

	bool Raft::is_up_to_date(int last_log_term, int last_log_index) const
	{
		return true;
	}

	Status RaftServiceImpl::AppendEntries(ServerContext *context, const raftpb::AppendEntriesRequest *request, raftpb::AppendEntriesResponse *response)
	{
		std::lock_guard<std::mutex> lock(raft_->mtx);
		// raft_->// logger->info("Received AppendEntries RPC from Server {} for term {}", request->leaderid(), request->term(), raft_->id);
		response->set_term(raft_->current_term_);
		response->set_success(false);

		if (request->term() < raft_->current_term_)
		{
			// Old outdated server with lower index. Reject the request
			return grpc::Status::OK;
		}

		raft_->become_follower(request->term());
		response->set_term(raft_->current_term_);
		response->set_success(true);
		return grpc::Status::OK;
	}

	Status RaftServiceImpl::RequestVote(grpc::ServerContext *context, const raftpb::RequestVoteRequest *request, raftpb::RequestVoteResponse *response)
	{
		std::lock_guard<std::mutex> lock(raft_->mtx);
		// raft_->// logger->info("Received RequestVote RPC from Server {} for term {}", request->candidateid(), request->term());

		response->set_term(raft_->current_term_);
		response->set_votegranted(false);
		if (raft_->current_term_ > request->term() || raft_->voted_for_ != -1)
		{
			// raft_->// logger->info("Vote not granted to Server {} for term {}", request->candidateid(), request->term());
			return grpc::Status::OK;
		}

		if (raft_->state_ == RaftState::Leader && raft_->current_term_ == request->term())
		{
			// raft_->// logger->info("Vote not granted to Server {} for term {}", request->candidateid(), request->term());
			return grpc::Status::OK;
		}

		response->set_votegranted(true);
		// raft_->// logger->info("Received a Vote Request. Going into election for term {}", request->term());
		raft_->become_candidate(request->term(), request->candidateid());
		// raft_->// logger->info("Vote granted to Server {} for term {}", request->candidateid(), request->term());
		return grpc::Status::OK;
	}

} // namespace rafty
