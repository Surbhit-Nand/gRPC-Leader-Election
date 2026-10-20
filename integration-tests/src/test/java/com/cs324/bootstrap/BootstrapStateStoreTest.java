package com.cs324.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BootstrapStateStoreTest {

    @TempDir
    Path temp;

    @Test
    void countersSurviveAStoreRestart() {
        Path file = temp.resolve("state.properties");
        BootstrapStateStore first = new BootstrapStateStore(file);
        BootstrapRegistryImpl registry = new BootstrapRegistryImpl(first);

        assertEquals(1, registry.allocateJobId());
        assertEquals(2, registry.allocateJobId());
        assertEquals(1, registry.registerWorkerAuto("localhost", 0).workerId());

        BootstrapStateStore second = new BootstrapStateStore(file);
        BootstrapRegistryImpl restarted = new BootstrapRegistryImpl(second);
        assertEquals(3, restarted.allocateJobId());
        assertEquals(2, restarted.registerWorkerAuto("localhost", 0).workerId());
    }

    @Test
    void aCorruptFileFallsBackToFreshCounters() throws Exception {
        Path file = temp.resolve("state.properties");
        Files.writeString(file, "not a properties file\nnextJobId=nope\n");

        BootstrapStateStore store = new BootstrapStateStore(file);
        assertEquals(1, store.load().nextJobId());
        assertEquals(1, store.load().nextWorkerId());
    }

    @Test
    void saveWritesAReusableFile() {
        Path file = temp.resolve("nested/state.properties");
        BootstrapStateStore store = new BootstrapStateStore(file);
        store.save(8, 42);
        assertTrue(Files.exists(file));
        assertEquals(8, store.load().nextWorkerId());
        assertEquals(42, store.load().nextJobId());
    }
}
