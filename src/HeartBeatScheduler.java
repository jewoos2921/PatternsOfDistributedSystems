import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HeartBeatScheduler {
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private Runnable action;
    private Long heartBeatInterval;

    public HeartBeatScheduler(Runnable action, Long heartBeatIntervalMs) {
        this.action = action;
        this.heartBeatInterval = heartBeatIntervalMs;
    }

    private ScheduledFuture<?> scheduledTask;

    public void start() {
        scheduledTask = executor.scheduleWithFixedDelay(
                new HeartBeatTask(action),
                heartBeatInterval, heartBeatInterval, TimeUnit.MILLISECONDS);
    }

}

class AbstractFailureDetector {
    private HeartBeatScheduler heartbeatScheduler = new HeartBeatScheduler(this::hearBeatCheck, 100L);

    abstract void hearBeatCheck();

    abstract void heartBeatReceived(T serverID);
}

class SendingServer {
    private void sendHeartBeat() throws IOException {
        socketChannel.blockingSend(newHeartBeatRequest(serverId));
    }
}

class ReceivingServer {
    private void handleRequest(Message<RequestOrResponse> request,
                               ClientConnection clientConnection) {
        RequestOrResponse clientRequest = request.getRequest();
        if (isHeartBeatRequest(clientRequest)) {
            HeartBeatRequest heartbeatRequest = deserialize(clientRequest);
            failureDetector.heartBeatReceived(heartbeatRequest.getServerId());
            sendResponse(clientConnection,
                    request.getRequest().getCorrelationId());
        } else {
            // 다른 요청을 처리한다.
        }
    }
}

class TimeoutBasedFailureDetector {
    @Override
    public void heartBeatReceived(T serverId) {
        Long currentTime = System.nanoTime();
        heartbeatReceivedTimes.put(serverId, currentTime);
        markUp(serverId);
    }

    @Override
    void hearBeatCheck() {
        Long now = System.nanoTime();
        Set<T> serverIds = heartbeatReceivedTimes.keySet();
        for (T serverId : serverIds) {
            Long lastHeartbeatReceivedTime = heartbeatReceivedTimes.get(serverId);
            Long timeSinceLastHeartbeat = now - lastHeartbeatReceivedTime;
            if (timeSinceLastHeartbeat >= timeoutNanos) {
                markDown(serverId);
            }
        }
    }
}