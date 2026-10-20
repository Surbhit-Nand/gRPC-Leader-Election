package com.cs324.bootstrap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * Small durable store for the bootstrap's monotonically increasing ID counters.
 *
 * <p>Only the counters are persisted. Active worker membership remains in memory because a
 * restarted bootstrap cannot know which old workers are still alive; those workers must
 * re-register (and are then assigned a fresh identity). Persisting the counters prevents a
 * restarted bootstrap from handing a previously used worker or job ID to a different entity.
 */
public final class BootstrapStateStore {

    private static final String NEXT_WORKER_ID = "nextWorkerId";
    private static final String NEXT_JOB_ID = "nextJobId";

    private final Path file;

    public BootstrapStateStore(Path file) {
        if (file == null) {
            throw new IllegalArgumentException("A state file is required");
        }
        this.file = file.toAbsolutePath().normalize();
    }

    public Path file() {
        return file;
    }

    public State load() {
        if (!Files.exists(file)) {
            return State.initial();
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(file)) {
            properties.load(input);
            return new State(positiveLong(properties.getProperty(NEXT_WORKER_ID)),
                    positiveLong(properties.getProperty(NEXT_JOB_ID)));
        } catch (IOException | RuntimeException e) {
            // A corrupt or unreadable state file must not stop the registry from starting. The
            // counters restart at one, which is safe only while no old worker is still running;
            // the README documents the required cluster-wide restart in that situation.
            System.err.println("Cannot read bootstrap state " + file + ": " + e.getMessage()
                    + "; starting counters at 1");
            return State.initial();
        }
    }

    public synchronized void save(long nextWorkerId, long nextJobId) {
        Properties properties = new Properties();
        properties.setProperty(NEXT_WORKER_ID, Long.toString(Math.max(1, nextWorkerId)));
        properties.setProperty(NEXT_JOB_ID, Long.toString(Math.max(1, nextJobId)));
        Path parent = file.getParent();
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (OutputStream output = Files.newOutputStream(temp)) {
                properties.store(output, "CS324 bootstrap ID counters");
            }
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            // Losing a counter write is preferable to failing an otherwise healthy cluster. The
            // in-memory counters remain monotonic; the next successful write repairs the file.
            System.err.println("Cannot persist bootstrap state " + file + ": " + e.getMessage());
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // Best effort cleanup only.
            }
        }
    }

    private static long positiveLong(String value) {
        if (value == null) {
            return 1;
        }
        long parsed = Long.parseLong(value.trim());
        return Math.max(1, parsed);
    }

    public record State(long nextWorkerId, long nextJobId) {
        public static State initial() {
            return new State(1, 1);
        }
    }
}
