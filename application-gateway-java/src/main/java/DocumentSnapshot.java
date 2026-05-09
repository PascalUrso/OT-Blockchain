import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DocumentSnapshot {
    public String snapshotId;
    public String docId;
    public long version;
    public long timestamp;
    public long lastBlockNumber;
    public String committedView;
    public Map<String, List<Operation>> clientBuffers;
    public Set<String> knownClients;
}
