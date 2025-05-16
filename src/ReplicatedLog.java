import java.util.List;

public class ReplicatedLog {
    private void startLeaderElection() {
        replicationState.setGeneration(replicationState.getGeneration() + 1);
        registerSelfVote();
        requestVoteFrom(followers);
    }

    private void becomeFollower(int leaderId, Long generation) {
        replicationState.reset();
        replcationState.setGeneration(generation);
        replicationState.setLeaderId(leaderId);
        transitionTo(ServerRole.FOLLOWING);
    }

    Long appendToLocalLog(byte[] data) {
        Long generation = replicationState.getGeneration();
        return appendToLocalLog(data, generation);
    }

    private Long appendAndReplicate(byte[] data) {
        Long lastLogEntryIndex = appendToLocalLog(data);
        replicateOnFollowers(lastLogEntryIndex);
        return lastLogEntryIndex;
    }

    private void replicateOnFollowers(Long entryAtIndex) {
        for (final FollowerHandler follower : followers) {
            replicateOn(follower, entryAtIndex); // 복제 요청을 팔로워에게 보낸다.
        }
    }

    private ReplicationResponse appendEntries(ReplicationRequest replicationRequest) {
        var entries = replicationRequest.getEntries();
        entries.stream().filter(e -> !wal.exists(e))
                .forEach(e -> wal.writeEntry(e));

        return new ReplicationResponse(SUCCEEDED, serverId(),
                replicationState.getGeneration(), wal.getLastLogIndex());
    }

    Long computeHighwaterMark(List<Long> serverLogIndexes, int noOfServers) {
        serverLogIndexes.sort(Long::compareTo);
        return serverLogIndexes.get(noOfServers / 2);
    }

    Long appendToLocalLog(byte[] data, Long generation) {
        var logEntryId = wal.getLastLogIndex() + 1;
        var logEntry = new WALEntry(logEntryId, data,
                EntryType.DATA, generation);
        return wal.writeEntry(logEntry);
    }

    VoteResponse handleVoteRequest(VoteRequest voteRequest) {
        // 요청에 포함된 세대가 더 높으면 요청 수신자는 팔로워가 된다.
        // 하지만 누가 리더인지는 아직 모른다.
        if (voteRequest.getGeneration() > replicationState.getGeneration()) {
            becomeFollower(LEADER_NOT_KNOWN, voteRequest.getGeneration());
        }

        VoteResponse voteTracker = replicationState.getVoteTracker();
        if (voteRequest.getGeneration() == replicationState.getGeneration() && !replicationState.hasLeader()) {
            if (isUptoDate(voteRequest) && !voteTracker.alreadyVoted()) {
                voteTracker.registerVote(voteRequest.getServerId());
                return grantVote();
            }
            if (voteTracker.alreadyVoted()) {
                return voteTracker.voteFor == voteRequest.getServerId() ? grantVote() : rejectVote();
            }
        }
        return rejectVote();
    }

    private boolean isUptoDate(VoteRequest voteRequest) {
        Long lastLogEntryGeneration = voteRequest.getLastLogEntryGeneration();
        Long lastLogEntryIndex = voteRequest.getLastLogEntryIndex();
        return lastLogEntryGeneration > wal.getLastLogEntryGeneration() ||
                (lastLogEntryGeneration == wal.getLastLogEntryGeneration() && lastLogEntryIndex >= wal.getLastLogIndex());
    }


    public WALEntry readEntry(long index) {
        if (index > replicationState.getHighWaterMark()) {
            throw new IllegalArgumentException("Log entry not available.");
        }
        return wal.readAt(index);
    }

    void maybeTruncate(ReplicationRequest replicationRequest) {
        replicationRequest.getEntries().stram()
                .filter(this::isConflicting)
                .forEach(this::truncate);
    }

    private boolean isConflicting(WALEntry requestEntry) {
        return wal.getLastLogIndex() >= requestEntry.getIndex() &&
                requestEntry.getGeneration() != wal.getGeneration(requestEntry.getEntryIndex());
    }

    private void truncate(WALEntry entry) {
        wal.truncate(entry.getEntryIndex());
    }

    private void updateMatchingLogIndex(int serverId, long replicatedIndex) {
        FollowerHandler follower = getFollowerHandler(serverId);
        follower.updateLastReplicatedIndex(replicatedIndex);
    }

    private boolean isPreviousEntryGenerationMismatched(ReplicationRequest request) {
        return generationAt(request.getPrevLogIndex())
                != request.getPrevLogGeneration();
    }

    private Long generationAt(long prevLogIndex) {
        WALEntry walEntry = wal.readAt(prevLogIndex);
        return walEntry.getGeneration();
    }

    private void applyLogEntries(Long previousCommitIndex,
                                 Long commitIndex) {
        for (long index = previousCommitIndex + 1; index <= commitIndex; index++) {
            WALEntry walEntry = wal.readAt(index);
            logger.info("Applying entry at" + index + " on server " + serverId());
            var response = stateMachine.applyEntry(walEntry);
            completeActiveProposal(index, response);
        }
    }

    private void updateHeightWaterMark(ReplicationRequest request) {
        if (request.getHighWaterMark() > replicationState.getHighWaterMark()) {
            var previousHighWaterMark = replicationState.getHighWaterMark();
            replicationState.setHighWaterMark(request.getHighWaterMark());
            applyLogEntries(previousHighWaterMark, replicationState.getHighWaterMark());
        }
    }

}

class FollowerHandler {
    private long matchIndex;

    public void updateLastReplicatedIndex(long lastReplicatedLogIndex) {
        this.matchIndex = lastReplicatedLogIndex;
    }
}