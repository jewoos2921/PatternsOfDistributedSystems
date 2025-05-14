import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class WALEntry {
    private final Long entryIndex;
    private final byte[] data;
    private final EntryType entryType;
    private final long timeStamp;

    public WALEntry(Long entryIndex, byte[] data, EntryType entryType, long timeStamp) {
        this.entryIndex = entryIndex;
        this.data = data;
        this.entryType = entryType;
        this.timeStamp = timeStamp;
    }
}

class KVStore {

    private Map<String, String> kv = new HashMap<>();

    public KVStore(Config config) {
        this.config = config;
        this.wal = WriteAheadLog.openWAL(config);
        this.applyLog();
    }

    public String get(String key) {
        return kv.get(key);
    }

    public void put(String key, String value) {
        appendLog(key, value);
        kv.put(key, value);
    }

    private Long appendLog(String key, String value) {
        return wal.writeEntry(new SetValueCommand(key, value).serialize());
    }

}

class SetValueCommand {
    private String key;
    private String value;

    public SetValueCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(Command.SetValueType);
        os.writeUTF(key);
        os.writeUTF(value);
    }

    public static SetValueCommand deserialize(InputStream is) {
        try {
            var dataInputStream = new DataInputStream(is);
            return new SetValueCommand(dataInputStream.readUTF(), dataInputStream.readUTF());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
