import java.io.IOException;

public class TransactionCoordinator {
    private int requestId;

    public MonotonicId begin() {
        return new MonotonicId(requestId++, config.getServerId());
    }

    public void loadTransactionsFromWAL() throws IOException {
        
    }
}

class MonotonicId {
    private int requestId;
    private int serverId;

    public MonotonicId(int requestId, int serverId) {
        this.requestId = requestId;
        this.serverId = serverId;
    }

    public int getRequestId() {
        return requestId;
    }

    public int getServerId() {
        return serverId;
    }
}

class TransactionClient {
    private void beginTransaction(String key) {

    }
}

class TransactionExecutor {

}