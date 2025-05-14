import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LogCleaner {
    public void startup() {
        scheduleLogCleaning();
    }

    private void scheduleLogCleaning() {
        singleThreadedExecutor.schedule(() -> {
            cleanLog();
        }, config.getCleanTaskIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public void cleanLog() {
        List<WALSegment> segmentsToBeDeleted = getSegmentsToBeDeleted();
        for (WALSegment walSegment : segmentsToBeDeleted) {
            wal.removeAndDeleteSegment(walSegment);
        }
        scheduleLogCleaning();
    }
}

class SnapshotBasedLogCleaner {
    @Override
    List<WALSegment> getSegmentsToBeDeleted() {
        return getSegmentsBefore(this.snapshotIndex);
    }

    List<WALSegment> getSegmentsBefore(Long snapshotIndex) {
        List<WALSegment> markedForDeletion = new ArrayList<>();
        List<WALSegment> sortedSavedSegments = wal.sortedSavedSegments();
        for (WALSegment sortedSavedSegment : sortedSavedSegments) {
            if (sortedSavedSegment.getLastLongEntryIndex() < snapshotIndex) {
                markedForDeletion.add(sortedSavedSegment);
            }
        }
        return markedForDeletion;
    }
}

class TimeBasedLogCleaner {
    private List<WALSegment> getSegmentsPast(Long logMaxDurationMs) {
        long now = System.currentTimeMillis();
        List<WALSegment> markedForDeletion = new ArrayList<>();
        List<WALSegment> sortedSavedSegments = wal.sortedSavedSegments();
        for (WALSegment sortedSavedSegment : sortedSavedSegments) {
            Long lastTimestamp = sortedSavedSegment.getLastLogEntryTimestamp();
            if (timeElapsedSince(now, lastTimestamp) > logMaxDurationMs) {
                markedForDeletion.add(sortedSavedSegment);
            }
        }
        return markedForDeletion;
    }

    private long timeElapsedSince(long now, long lastLogEntryTimestamp) {
        return now - lastLogEntryTimestamp;
    }
}