import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class TestRunConfig {
    public String channelName = "mychannel";
    public String chaincodeName = "otcollab";
    public String clientId = "userA";
    public String docId = "doc_1";
    public List<Integer> clocks = new ArrayList<>();
    public Integer totalOps = 0;
    public List<String> types = new ArrayList<>();
    public List<Integer> positions = new ArrayList<>();
    public List<String> values = new ArrayList<>();
    public long pollMillis = 300;
    public long testTimeoutSeconds = 600;

    public String mspId = "Org1MSP";
    public String certDirPath;
    public String keyDirPath;
    public String tlsCertPath;
    public String peerEndpoint = "localhost:7051";
    public String overrideAuth = "peer0.org1.example.com";

    public String mode = "interactive";

    public boolean isTestMode() {
        return "test".equalsIgnoreCase(mode);
    }

    public Connections.GatewayProfile toGatewayProfile() {
        return new Connections.GatewayProfile(
                mspId,
                Path.of(certDirPath),
                Path.of(keyDirPath),
                Path.of(tlsCertPath),
                peerEndpoint,
                overrideAuth);
    }
}
