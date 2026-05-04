import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public final class OpsLogGenerator {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    static final class Request {
        public int peerCount = 3;
        public int totalOps = 100;
        public String peerIdPrefix = "user";

        // uniform | normal | exponential
        public String peerOpsDistribution = "uniform";
        public double normalMean = 1.0;
        public double normalStd = 0.3;
        public double exponentialLambda = 1.0;

        // Percentage weights (not required to sum to 1.0, they are normalized)
        public double insertPct = 0.5;
        public double deletePct = 0.25;
        public double updatePct = 0.25;

        // Probability that current op shares the same clock slot with previous op
        public double concurrencyDegree = 0.2;

        public Long randomSeed;
        public String outputPath = "test-configs/generated/ops-schedule.json";
    }

    static final class Event {
        public int index;
        public String peerId;
        public long clock;
        public String type;
        public int position;
        public String value;

        Event(final int index,
              final String peerId,
              final long clock,
              final String type,
              final int position,
              final String value) {
            this.index = index;
            this.peerId = peerId;
            this.clock = clock;
            this.type = type;
            this.position = position;
            this.value = value;
        }
    }

    static final class PeerSchedule {
        public String peerId;
        public List<Integer> clocks = new ArrayList<>();
        public List<String> types = new ArrayList<>();
        public List<Integer> positions = new ArrayList<>();
        public List<String> values = new ArrayList<>();

        PeerSchedule(final String peerId) {
            this.peerId = peerId;
        }
    }

    static final class Output {
        public Meta meta;
        public List<PeerSchedule> peers;
        public List<Event> events;
    }

    static final class Meta {
        public String generatedAt;
        public int peerCount;
        public int totalOps;
        public String peerOpsDistribution;
        public double concurrencyDegree;
        public Map<String, Integer> realizedOpsPerPeer;
        public Map<String, Integer> realizedTypeCounts;
        public Long randomSeed;
    }

    private OpsLogGenerator() {
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 2 || !"--config".equals(args[0])) {
            System.out.println("Usage: ./gradlew generateOpsLog -PgeneratorArgs='--config <config.json> [--out <output.json>]'");
            return;
        }

        Path configPath = Path.of(args[1]).toAbsolutePath().normalize();
        Request request = GSON.fromJson(Files.readString(configPath, StandardCharsets.UTF_8), Request.class);
        if (request == null) {
            throw new IllegalArgumentException("Invalid config: " + configPath);
        }

        String overrideOut = parseOutArg(args);
        Path outputPath = overrideOut == null
                ? resolvePath(configPath.getParent(), request.outputPath)
                : resolvePath(configPath.getParent(), overrideOut);

        validateRequest(request);

        Random random = request.randomSeed == null ? new Random() : new Random(request.randomSeed);
        Output output = generate(request, random);

        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }
        Files.writeString(outputPath, GSON.toJson(output), StandardCharsets.UTF_8);

        System.out.println("Generated schedule: " + outputPath);
        System.out.println("Peers=" + output.meta.peerCount + ", totalOps=" + output.meta.totalOps
                + ", realizedTypeCounts=" + output.meta.realizedTypeCounts);
    }

    private static Output generate(final Request request, final Random random) {
        List<String> peerIds = new ArrayList<>();
        for (int i = 1; i <= request.peerCount; i++) {
            peerIds.add(request.peerIdPrefix + i);
        }

        int[] perPeerCounts = allocateOpsPerPeer(request, random);
        List<String> peerOrder = buildPeerOrder(peerIds, perPeerCounts, random);

        Map<String, PeerSchedule> schedules = new LinkedHashMap<>();
        for (String peerId : peerIds) {
            schedules.put(peerId, new PeerSchedule(peerId));
        }

        Map<String, Integer> lastClockByPeer = new HashMap<>();
        for (String peerId : peerIds) {
            lastClockByPeer.put(peerId, 0);
        }

        Set<Integer> usedClocks = new HashSet<>();
        int maxUsedClock = -1;

        int docLength = 0;
        Map<String, Integer> typeCounts = new LinkedHashMap<>();
        typeCounts.put("insert", 0);
        typeCounts.put("delete", 0);
        typeCounts.put("update", 0);

        List<Event> events = new ArrayList<>();
        for (int i = 0; i < peerOrder.size(); i++) {
            String peerId = peerOrder.get(i);

            int generatedCountBefore = i;
            int lower = lastClockByPeer.getOrDefault(peerId, 0);
            int upper = generatedCountBefore;
            int clock = chooseClock(request, random, usedClocks, maxUsedClock, lower, upper);
            lastClockByPeer.put(peerId, clock);
            usedClocks.add(clock);
            if (clock > maxUsedClock) {
                maxUsedClock = clock;
            }

            String type = pickValidType(request, random, docLength);

            int position;
            String value;
            if ("insert".equals(type)) {
                position = random.nextInt(docLength + 1);
                value = randomLowercaseValue(random);
                docLength++;
            } else if ("delete".equals(type)) {
                position = random.nextInt(docLength);
                value = "";
                docLength--;
            } else {
                position = random.nextInt(docLength);
                value = randomLowercaseValue(random);
            }
            typeCounts.put(type, typeCounts.get(type) + 1);

            Event event = new Event(i + 1, peerId, clock, type, position, value);
            events.add(event);

            PeerSchedule schedule = schedules.get(peerId);
            schedule.clocks.add((int) clock);
            schedule.types.add(type);
            schedule.positions.add(position);
            schedule.values.add(value);
        }

        Map<String, Integer> realizedPerPeer = new LinkedHashMap<>();
        for (int i = 0; i < peerIds.size(); i++) {
            realizedPerPeer.put(peerIds.get(i), perPeerCounts[i]);
        }

        Meta meta = new Meta();
        meta.generatedAt = Instant.now().toString();
        meta.peerCount = request.peerCount;
        meta.totalOps = request.totalOps;
        meta.peerOpsDistribution = request.peerOpsDistribution;
        meta.concurrencyDegree = request.concurrencyDegree;
        meta.realizedOpsPerPeer = realizedPerPeer;
        meta.realizedTypeCounts = typeCounts;
        meta.randomSeed = request.randomSeed;

        Output output = new Output();
        output.meta = meta;
        output.peers = new ArrayList<>(schedules.values());
        output.events = events;
        return output;
    }

    private static int chooseClock(final Request request,
                                   final Random random,
                                   final Set<Integer> usedClocks,
                                   final int maxUsedClock,
                                   final int lower,
                                   final int upper) {
        if (upper <= lower) {
            return lower;
        }

        boolean wantConcurrent = !usedClocks.isEmpty() && random.nextDouble() < request.concurrencyDegree;
        if (wantConcurrent) {
            List<Integer> reusable = new ArrayList<>();
            for (Integer c : usedClocks) {
                if (c != null && c >= lower && c <= upper) {
                    reusable.add(c);
                }
            }
            if (!reusable.isEmpty()) {
                return reusable.get(random.nextInt(reusable.size()));
            }
        }

        int start = Math.max(lower, maxUsedClock + 1);
        List<Integer> fresh = new ArrayList<>();
        for (int c = start; c <= upper; c++) {
            if (!usedClocks.contains(c)) {
                fresh.add(c);
            }
        }
        if (!fresh.isEmpty()) {
            return fresh.get(random.nextInt(fresh.size()));
        }

        return upper;
    }

    private static int[] allocateOpsPerPeer(final Request request, final Random random) {
        double[] weights = new double[request.peerCount];
        double sum = 0.0;
        for (int i = 0; i < request.peerCount; i++) {
            double w = sampleWeight(request, random);
            weights[i] = w;
            sum += w;
        }

        int[] counts = new int[request.peerCount];
        double[] remainders = new double[request.peerCount];
        int allocated = 0;
        for (int i = 0; i < request.peerCount; i++) {
            double scaled = request.totalOps * (weights[i] / sum);
            counts[i] = (int) Math.floor(scaled);
            remainders[i] = scaled - counts[i];
            allocated += counts[i];
        }

        int left = request.totalOps - allocated;
        while (left > 0) {
            int bestIdx = 0;
            for (int i = 1; i < remainders.length; i++) {
                if (remainders[i] > remainders[bestIdx]) {
                    bestIdx = i;
                }
            }
            counts[bestIdx]++;
            remainders[bestIdx] = 0;
            left--;
        }

        return counts;
    }

    private static double sampleWeight(final Request request, final Random random) {
        String dist = request.peerOpsDistribution == null ? "uniform" : request.peerOpsDistribution.toLowerCase(Locale.ROOT);
        switch (dist) {
            case "normal":
            case "norm":
                return Math.max(1e-6, request.normalMean + random.nextGaussian() * request.normalStd);
            case "exponential":
            case "exp":
                double u = Math.max(1e-12, random.nextDouble());
                return Math.max(1e-6, -Math.log(1.0 - u) / request.exponentialLambda);
            case "uniform":
            default:
                return 1.0;
        }
    }

    private static String pickValidType(final Request request, final Random random, final int docLength) {
        double ins = Math.max(0.0, request.insertPct);
        double del = Math.max(0.0, request.deletePct);
        double upd = Math.max(0.0, request.updatePct);

        double total = ins + del + upd;
        if (total <= 0.0) {
            ins = 1.0;
            del = 0.0;
            upd = 0.0;
            total = 1.0;
        }

        if (docLength <= 0) {
            return "insert";
        }

        double r = random.nextDouble() * total;
        if (r < ins) {
            return "insert";
        }
        if (r < ins + del) {
            return "delete";
        }
        return "update";
    }

    private static String randomLowercaseValue(final Random random) {
        return String.valueOf((char) ('a' + random.nextInt(26)));
    }

    private static List<String> buildPeerOrder(final List<String> peerIds, final int[] counts, final Random random) {
        List<String> order = new ArrayList<>();
        for (int i = 0; i < peerIds.size(); i++) {
            for (int j = 0; j < counts[i]; j++) {
                order.add(peerIds.get(i));
            }
        }

        for (int i = order.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            String tmp = order.get(i);
            order.set(i, order.get(j));
            order.set(j, tmp);
        }

        return order;
    }

    private static String parseOutArg(final String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--out".equals(args[i])) {
                return args[i + 1];
            }
        }
        return null;
    }

    private static void validateRequest(final Request request) {
        if (request.peerCount <= 0) {
            throw new IllegalArgumentException("peerCount must be > 0");
        }
        if (request.totalOps <= 0) {
            throw new IllegalArgumentException("totalOps must be > 0");
        }
        if (request.exponentialLambda <= 0.0) {
            throw new IllegalArgumentException("exponentialLambda must be > 0");
        }
        if (request.concurrencyDegree < 0.0 || request.concurrencyDegree > 1.0) {
            throw new IllegalArgumentException("concurrencyDegree must be within [0, 1]");
        }
        if (request.peerIdPrefix == null || request.peerIdPrefix.isBlank()) {
            request.peerIdPrefix = "user";
        }
        if (request.outputPath == null || request.outputPath.isBlank()) {
            request.outputPath = "test-configs/generated/ops-schedule.json";
        }
    }

    private static Path resolvePath(final Path baseDir, final String configuredPath) {
        Path path = Path.of(configuredPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }

        if (baseDir == null) {
            return path.normalize();
        }
        return baseDir.resolve(path).normalize();
    }
}
