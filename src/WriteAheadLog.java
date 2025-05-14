import java.util.ArrayList;
import java.util.List;

public class WriteAheadLog {
    public WriteAheadLog() {
        this.logCleaner = new LogCleaner(config);
        this.logCleaner.startup();
    }

    public Long writeEntry(WALEntry entry) {
        maybeRoll();
        return openSegment.writeEntry(entry);
    }

    private void maybeRoll() {
        if (openSegment.size() >= config.getMaxLogSize()) {
            openSegment.flush();
            sortedSavedSegments.add(openSegment);
            long lastId = openSegment.getLastLogEntryIndex();
            openSegment = WALSegment.open(lastId, config.getWalDir());
        }
    }

    public List<WALEntry> readFrom(Long startIndex) {
        List<WALSegment> segments = getAllSegmentsContainingLogGreaterThan(startIndex);
        return readWalEntriesFrom(startIndex, segments);
    }

    private List<WALSegment> getAllSegmentsContainingLogGreaterThan(Long startIndex) {

        List<WALSegment> segments = new ArrayList<>();
        // 마지막 분할에서 시작 오프셋이
        // startIndex보다 작은 첫 번째 분할까지 역순으로 탐색
        // 이렇게 하면 시작 인덱스보다 큰 로그 항목을 포함하는 모든 분할을 찾을 수 있다.
        for (int i = sortedSavedSegments.size() - 1; i >= 0; i--) {
            WALSegment walSegment = sortedSavedSegments.get(i);
            segments.add(walSegment);

            if (walSegment.getBaseOffset() <= startIndex) {
                // 시작 인덱스보다 작은 baseoffset을 가진 첫 번째 분할에서 중단
                break;
            }
        }

        if (openSegment.getBaseOffset() <= startIndex) {
            segments.add(openSegment);
        }

        return segments;
    }
}

class WALSegment {
    public static String createFileName(Long startIndex) {
        return logPrefix + "_" + startIndex + logSuffix;
    }

    public static Long getBaseOffsetFromFileName(String fileName) {
        String[] nameAndSuffix = fileName.split(logSuffix);
        String[] prefixAndOffset = nameAndSuffix[0].split("_");
        if (prefixAndOffset[0].equals(logPrefix)) {
            return Long.parseLong(prefixAndOffset[1]);
        }

        return -1L;
    }
}