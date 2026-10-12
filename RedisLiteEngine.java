import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A small, thread-safe in-memory key-value store with TTL and LRU eviction. */
public final class RedisLiteEngine {
    private static final int MAX_CAPACITY = 5;
    private static final long TTL_SCAN_INTERVAL_SECONDS = 1L;

    private static final class Entry {
        private final String value;
        private final long expiresAtNanos;

        private Entry(String value, long expiresAtNanos) {
            this.value = value;
            this.expiresAtNanos = expiresAtNanos;
        }

        private boolean isExpired(long nowNanos) {
            return expiresAtNanos != 0L && nowNanos - expiresAtNanos >= 0L;
        }
    }

    /** Access-order LinkedHashMap keeps the least-recently-used entry first. */
    private static final class CacheStore {
        private final LinkedHashMap<String, Entry> entries =
                new LinkedHashMap<String, Entry>(16, 0.75f, true);

        synchronized void set(String key, String value, long ttlSeconds) {
            long expiresAt = ttlSeconds == 0L
                    ? 0L
                    : System.nanoTime() + TimeUnit.SECONDS.toNanos(ttlSeconds);

            // Replacing a key refreshes its position and its expiration time.
            entries.put(key, new Entry(value, expiresAt));
            if (entries.size() > MAX_CAPACITY) {
                Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
                Map.Entry<String, Entry> eldest = iterator.next();
                String evictedKey = eldest.getKey();
                iterator.remove();
                System.out.println("[LRU] Evicted least recently used key: " + evictedKey);
            }
        }

        synchronized String get(String key) {
            Entry entry = entries.get(key); // Also marks the key as most recently used.
            if (entry == null) {
                return null;
            }
            if (entry.isExpired(System.nanoTime())) {
                entries.remove(key);
                System.out.println("[TTL] Evicted expired key: " + key);
                return null;
            }
            return entry.value;
        }

        synchronized void evictExpired() {
            long now = System.nanoTime();
            Iterator<Map.Entry<String, Entry>> iterator = entries.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Entry> item = iterator.next();
                if (item.getValue().isExpired(now)) {
                    String key = item.getKey();
                    iterator.remove();
                    System.out.println("[TTL] Evicted expired key: " + key);
                }
            }
        }

        synchronized void printStatus() {
            evictExpired();
            long approximateBytes = 0L;
            StringBuilder order = new StringBuilder();
            for (Map.Entry<String, Entry> item : entries.entrySet()) {
                if (order.length() > 0) {
                    order.append(" -> ");
                }
                order.append(item.getKey());
                approximateBytes += item.getKey().getBytes(StandardCharsets.UTF_8).length;
                approximateBytes += item.getValue().value.getBytes(StandardCharsets.UTF_8).length;
            }

            System.out.println("Active keys (" + entries.size() + "/" + MAX_CAPACITY + "): "
                    + (entries.isEmpty() ? "(none)" : entries.keySet()));
            System.out.println("Approximate payload memory: " + approximateBytes + " bytes"
                    + " (UTF-8 key/value bytes; excludes JVM object overhead)");
            System.out.println("LRU order (least to most recently used): "
                    + (order.length() == 0 ? "(empty)" : order));
        }
    }

    private RedisLiteEngine() { }

    public static void main(String[] args) {
        final CacheStore store = new CacheStore();
        ScheduledExecutorService ttlWorker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "RedisLite-TTL-Worker");
            thread.setDaemon(true);
            return thread;
        });
        ttlWorker.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    store.evictExpired();
                } catch (RuntimeException exception) {
                    System.err.println("[TTL] Worker error: " + exception.getMessage());
                }
            }
        }, TTL_SCAN_INTERVAL_SECONDS, TTL_SCAN_INTERVAL_SECONDS, TimeUnit.SECONDS);

        System.out.println("RedisLite Engine (capacity " + MAX_CAPACITY + ")");
        System.out.println("Commands: SET [key] [value] | SETEX [key] [value] [ttl_in_seconds] | GET [key] | STATUS | EXIT");
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("redis> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\\s+");
                String command = parts[0].toUpperCase();
                if ("EXIT".equals(command) || "QUIT".equals(command)) {
                    break;
                }
                switch (command) {
                    case "SET":
                        if (parts.length != 3) {
                            System.out.println("Usage: SET [key] [value]");
                        } else {
                            store.set(parts[1], parts[2], 0L);
                            System.out.println("OK");
                        }
                        break;
                    case "SETEX":
                        if (parts.length != 4) {
                            System.out.println("Usage: SETEX [key] [value] [ttl_in_seconds]");
                            break;
                        }
                        try {
                            long ttlSeconds = Long.parseLong(parts[3]);
                            if (ttlSeconds <= 0L) {
                                System.out.println("TTL must be a positive number of seconds.");
                            } else {
                                store.set(parts[1], parts[2], ttlSeconds);
                                System.out.println("OK (expires in " + ttlSeconds + " seconds)");
                            }
                        } catch (NumberFormatException exception) {
                            System.out.println("TTL must be a positive whole number of seconds.");
                        }
                        break;
                    case "GET":
                        if (parts.length != 2) {
                            System.out.println("Usage: GET [key]");
                        } else {
                            String value = store.get(parts[1]);
                            System.out.println(value == null ? "(nil)" : value);
                        }
                        break;
                    case "STATUS":
                        if (parts.length != 1) {
                            System.out.println("Usage: STATUS");
                        } else {
                            store.printStatus();
                        }
                        break;
                    default:
                        System.out.println("Unknown command. Available: SET, SETEX, GET, STATUS, EXIT");
                }
            }
        } finally {
            ttlWorker.shutdownNow();
            System.out.println("RedisLite Engine stopped.");
        }
    }
}
