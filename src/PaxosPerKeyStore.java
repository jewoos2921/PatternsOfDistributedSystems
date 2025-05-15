import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PaxosPerKeyStore {
    int serverId;
    int maxKnownPaxosRoundId = 1;
    int maxAttempts = 4;

    public PaxosPerKeyStore(int serverId) {
        this.serverId = serverId;
    }

    Map<String, Acceptor> key2Acceptors = new HashMap<>();
    List<PaxosPerKeyStore> peers;

    public void put(String key, String defaultProposal) {
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            var requestId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            var setValueCommand = new SetValueCommand(key, defaultProposal);

            if (runPaxos(key, requestId, setValueCommand)) {
                return;
            }

            Uninterruptibles.sleepUninterruptibly(
                    ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            logger.warn("Experienced Paxos contention. " + "Attempting with higher generation");
        }
        throw new WriteTimeOutException(attempts);
    }

    private boolean runPaxos(String key, MonotonicId generation,
                             Command initialValue) {
        var allAcceptors = getAcceptorInstancesFor(key);
        var prepareResponses = sendPrepare(generation, allAcceptors);
        if (isQuorumPrepared(prepareResponses)) {
            Command proposedValue = getValue(prepareResponses, initialValue);
            if (sendAccept(generation, proposedValue, allAcceptors)) {
                sendCommit(generation, proposedValue, allAcceptors);
            }
            if (proposedValue == initialValue) {
                return true;
            }
        }
        return false;
    }

    public Command getValue(List<PrepareResponse> prepareResponses,
                            Command initialValue) {
        var mostRecentAcceptedValue = getMostRecentAcceptedValue(prepareResponses);
        var proposedValue = mostRecentAcceptedValue.acceptedValue.isEmpty()
                ? initialValue
                : mostRecentAcceptedValue.acceptedValue.get();
        return proposedValue;
    }

    private PrepareResponse getMostRecentAcceptedValue(
            List<PrepareResponse> prepareResponses) {
        return prepareResponses.stream()
                .max(Comparator.comparing(r -> r.acceptedGeneration
                        .oElse(MonotonicId.empty()))).get();
    }

    public String get(String key) {
        int attempts = 0;
        while (attempts <= maxAttempts) {
            attempts++;
            var requestId = new MonotonicId(maxKnownPaxosRoundId++, serverId);
            var getValueCommand = new NoOpCommand(key);
            if (runPaxos(key, requestId, getValueCommand)) {
                return kv.get(key);
            }

            Uninterruptibles.sleepUninterruptibly(
                    ThreadLocalRandom.current().nextInt(100), MILLISECONDS);
            logger.warn("Experienced Paxos contention. " + "Attempting with higher generation");
        }
        throw new ReadTimeOutException(attempts);
    }
}

